/*-
 * Copyright (c) 2002-2005, 2009, 2013 Jeffrey Roberson <jeff@FreeBSD.org>
 * Copyright (c) 2004, 2005 Bosko Milekic <bmilekic@FreeBSD.org>
 * Copyright (c) 2004-2006 Robert N. M. Watson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice unmodified, this list of conditions, and the following
 *    disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * uma_core.c  Implementation of the Universal Memory allocator
 *
 * This allocator is intended to replace the multitude of similar object caches
 * in the standard FreeBSD kernel.  The intent is to be flexible as well as
 * efficient.  A primary design goal is to return unused memory to the rest of
 * the system.  This will make the system as a whole more flexible due to the
 * ability to move memory to subsystems which most need it instead of leaving
 * pools of reserved memory unused.
 *
 * The basic ideas stem from similar slab/zone based allocators whose algorithms
 * are well known.
 *
 */

#include "uma/uma.h"
#include "uma/uma_int.h"
#include "uma/rwlock.h"
#include "uma/sx.h"
#include "uma/cpufunc.h"
#include "uma/smp.h"
#include "uma/bitset.h"
#include "uma/malloc.h"
#include "uma/systm.h"
#include "uma/smp.h"
#include "uma/task.h"

#include <strings.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

#include <sys/mman.h>

typedef int bool;
#define true 1
#define false 0

/*
 * TODO:
 *	- Improve memory usage for large allocations
 *	- Investigate cache size adjustments
 */

/* I should really use ktr.. */
/*
#define UMA_DEBUG 1
#define UMA_DEBUG_ALLOC 1
#define UMA_DEBUG_ALLOC_1 1
*/

static pthread_once_t init_once = PTHREAD_ONCE_INIT;
static pthread_t uma_timeout_td;

/*
 * This is the zone and keg from which all zones are spawned.  The idea is that
 * even the zone & keg heads are allocated from the allocator, so we use the
 * bss section to bootstrap us.
 */
static struct uma_keg masterkeg;
static struct uma_zone masterzone_k;
static struct uma_zone masterzone_z;
static uma_zone_t kegs = &masterzone_k;
static uma_zone_t zones = &masterzone_z;

/* This is the zone from which all of uma_slab_t's are allocated. */
static uma_zone_t slabzone;

/*
 * The initial hash tables come out of this zone so they can be allocated
 * prior to malloc coming up.
 */
static uma_zone_t hashzone;

/* The boot-time adjusted value for cache line alignment. */
int uma_align_cache = 64 - 1;

/*
 * Are we allowed to allocate buckets?
 */
static int bucketdisable = 1;

/* Linked list of all kegs in the system */
static LIST_HEAD(,uma_keg) uma_kegs = LIST_HEAD_INITIALIZER(uma_kegs);

/* Linked list of all cache-only zones in the system */
static LIST_HEAD(,uma_zone) uma_cachezones =
    LIST_HEAD_INITIALIZER(uma_cachezones);

/* This RW lock protects the keg list */
static struct rwlock __cache_aligned uma_rwlock;

static struct sx uma_drain_lock;
static pthread_mutex_t uma_drain_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t uma_drain_cond = PTHREAD_COND_INITIALIZER;

/* Is the VM done starting up? */
static int booted = 0;
#define	UMA_STARTUP	1
#define	UMA_STARTUP2	2
#define	UMA_SHUTDOWN	3

/*
 * This is the handle used to schedule events that need to happen
 * outside of the allocation fast path.
 */
#define	UMA_TIMEOUT	20		/* Seconds for callout interval. */

/*
 * This structure is passed as the zone ctor arg so that I don't have to create
 * a special allocation function just for zones.
 */
struct uma_zctor_args {
	const char *name;
	size_t size;
	uma_ctor ctor;
	uma_dtor dtor;
	uma_init uminit;
	uma_fini fini;
	uma_import import;
	uma_release release;
	void *arg;
	uma_keg_t keg;
	int align;
	uint32_t flags;
};

struct uma_kctor_args {
	uma_zone_t zone;
	size_t size;
	uma_init uminit;
	uma_fini fini;
	int align;
	uint32_t flags;
};

struct uma_bucket_zone {
	uma_zone_t	ubz_zone;
	char		*ubz_name;
	int		ubz_entries;	/* Number of items it can hold. */
	int		ubz_maxsize;	/* Maximum allocation size per-item. */
};

/*
 * Compute the actual number of bucket entries to pack them in power
 * of two sizes for more efficient space utilization.
 */
#define	BUCKET_SIZE(n)						\
    (((sizeof(void *) * (n)) - sizeof(struct uma_bucket)) / sizeof(void *))

#define	BUCKET_MAX	BUCKET_SIZE(256)

struct uma_bucket_zone bucket_zones[] = {
	{ NULL, "4 Bucket", BUCKET_SIZE(4), 4096 },
	{ NULL, "6 Bucket", BUCKET_SIZE(6), 3072 },
	{ NULL, "8 Bucket", BUCKET_SIZE(8), 2048 },
	{ NULL, "12 Bucket", BUCKET_SIZE(12), 1536 },
	{ NULL, "16 Bucket", BUCKET_SIZE(16), 1024 },
	{ NULL, "32 Bucket", BUCKET_SIZE(32), 512 },
	{ NULL, "64 Bucket", BUCKET_SIZE(64), 256 },
	{ NULL, "128 Bucket", BUCKET_SIZE(128), 128 },
	{ NULL, "256 Bucket", BUCKET_SIZE(256), 64 },
	{ NULL, NULL, 0, 0}
};

/*
 * Flags and enumerations to be passed to internal functions.
 */
enum zfreeskip { SKIP_NONE = 0, SKIP_DTOR, SKIP_FINI };

/* Prototypes.. */

static void *page_alloc(uma_zone_t, vm_size_t, uint8_t *, int);
static void *startup_alloc(uma_zone_t, vm_size_t, uint8_t *, int);
static void page_free(void *, vm_size_t, uint8_t);
static uma_slab_t keg_alloc_slab(uma_keg_t, uma_zone_t, int);
static void cache_drain(uma_zone_t);
static void bucket_drain(uma_zone_t, uma_bucket_t);
static void bucket_cache_drain(uma_zone_t zone);
static int keg_ctor(void *, int, void *, int);
static void keg_dtor(void *, int, void *);
static int zone_ctor(void *, int, void *, int);
static void zone_dtor(void *, int, void *);
static int zero_init(void *, int, int);
static void zone_drain(uma_zone_t);
static void keg_small_init(uma_keg_t keg);
static void keg_large_init(uma_keg_t keg);
static void zone_foreach(void (*zfunc)(uma_zone_t));
static void zone_timeout(uma_zone_t zone);
static int hash_alloc(struct uma_hash *, u_int);
static int hash_expand(struct uma_hash *, struct uma_hash *);
static void hash_free(struct uma_hash *hash);
static void *uma_timeout(void *);
static void uma_startup3(void);
static void *zone_alloc_item(uma_zone_t, void *, int);
static void zone_free_item(uma_zone_t, void *, void *, enum zfreeskip);
static void bucket_enable(void);
static void bucket_init(void);
static uma_bucket_t bucket_alloc(uma_zone_t zone, void *, int);
static void bucket_free(uma_zone_t zone, uma_bucket_t, void *);
static void bucket_zone_drain(void);
static uma_bucket_t zone_alloc_bucket(uma_zone_t zone, void *, int flags);
static uma_slab_t zone_fetch_slab(uma_zone_t zone, uma_keg_t last, int flags);
static void *slab_alloc_item(uma_keg_t keg, uma_slab_t slab);
static void slab_free_item(uma_keg_t keg, uma_slab_t slab, void *item);
static uma_keg_t uma_kcreate(uma_zone_t zone, size_t size, uma_init uminit,
    uma_fini fini, int align, uint32_t flags);
static int zone_import(uma_zone_t zone, void **bucket, int max, int flags);
static void zone_release(uma_zone_t zone, void **bucket, int cnt);
static void uma_zero_item(void *item, uma_zone_t zone);
static void uma_startup2(void);
static uma_zone_t uma_zcreate_impl(const char *name, size_t size,
    uma_ctor ctor, uma_dtor dtor, uma_init uminit, uma_fini fini, int align,
    uint32_t flags);
static uma_zone_t uma_zcache_create_impl(char *name, int size,
    uma_ctor ctor, uma_dtor dtor, uma_init zinit, uma_fini zfini,
    uma_import zimport, uma_release zrelease, void *arg, int flags);
static void uma_startup_impl(void);

void uma_print_zone(uma_zone_t);
void uma_print_stats(void);

static inline void *uma_mmap(vm_size_t bytes)
{
    return valloc(bytes);
}

static int zone_warnings = 1;

/*
 * This routine checks to see whether or not it's safe to enable buckets.
 */
static void
bucket_enable(void)
{
	bucketdisable = 0; 
}

/*
 * Initialize bucket_zones, the array of zones of buckets of various sizes.
 *
 * For each zone, calculate the memory required for each bucket, consisting
 * of the header and an array of pointers.
 */
static void
bucket_init(void)
{
	struct uma_bucket_zone *ubz;
	int size;

	for (ubz = &bucket_zones[0]; ubz->ubz_entries != 0; ubz++) {
		size = roundup(sizeof(struct uma_bucket), sizeof(void *));
		size += sizeof(void *) * ubz->ubz_entries;
		ubz->ubz_zone = uma_zcreate_impl(ubz->ubz_name, size,
		    NULL, NULL, NULL, NULL, UMA_ALIGN_PTR,
		    UMA_ZONE_MTXCLASS | UMA_ZFLAG_BUCKET);
	}
}

/*
 * Given a desired number of entries for a bucket, return the zone from which
 * to allocate the bucket.
 */
static struct uma_bucket_zone *
bucket_zone_lookup(int entries)
{
	struct uma_bucket_zone *ubz;

	for (ubz = &bucket_zones[0]; ubz->ubz_entries != 0; ubz++)
		if (ubz->ubz_entries >= entries)
			return (ubz);
	ubz--;
	return (ubz);
}

static int
bucket_select(int size)
{
	struct uma_bucket_zone *ubz;

	ubz = &bucket_zones[0];
	if (size > ubz->ubz_maxsize)
		return MAX((ubz->ubz_maxsize * ubz->ubz_entries) / size, 1);

	for (; ubz->ubz_entries != 0; ubz++)
		if (ubz->ubz_maxsize < size)
			break;
	ubz--;
	return (ubz->ubz_entries);
}

static uma_bucket_t
bucket_alloc(uma_zone_t zone, void *udata, int flags)
{
	struct uma_bucket_zone *ubz;
	uma_bucket_t bucket;

	/*
	 * This is to stop us from allocating per cpu buckets while we're
	 * running out of vm.boot_pages.  Otherwise, we would exhaust the
	 * boot pages.  This also prevents us from allocating buckets in
	 * low memory situations.
	 */
	if (bucketdisable)
		return (NULL);
	/*
	 * To limit bucket recursion we store the original zone flags
	 * in a cookie passed via zalloc_arg/zfree_arg.  This allows the
	 * NOVM flag to persist even through deep recursions.  We also
	 * store ZFLAG_BUCKET once we have recursed attempting to allocate
	 * a bucket for a bucket zone so we do not allow infinite bucket
	 * recursion.  This cookie will even persist to frees of unused
	 * buckets via the allocation path or bucket allocations in the
	 * free path.
	 */
	if ((zone->uz_flags & UMA_ZFLAG_BUCKET) == 0)
		udata = (void *)(uintptr_t)zone->uz_flags;
	else {
		if ((uintptr_t)udata & UMA_ZFLAG_BUCKET)
			return (NULL);
		udata = (void *)((uintptr_t)udata | UMA_ZFLAG_BUCKET);
	}
	if ((uintptr_t)udata & UMA_ZFLAG_CACHEONLY)
		flags |= M_NOVM;
	ubz = bucket_zone_lookup(zone->uz_count);
	if (ubz->ubz_zone == zone && (ubz + 1)->ubz_entries != 0)
		ubz++;
	bucket = uma_zalloc_arg(ubz->ubz_zone, udata, flags);
	if (bucket) {
#ifdef INVARIANTS
		bzero(bucket->ub_bucket, sizeof(void *) * ubz->ubz_entries);
#endif
		bucket->ub_cnt = 0;
		bucket->ub_entries = ubz->ubz_entries;
	}

	return (bucket);
}

static void
bucket_free(uma_zone_t zone, uma_bucket_t bucket, void *udata)
{
	struct uma_bucket_zone *ubz;

	KASSERT(bucket->ub_cnt == 0,
	    ("bucket_free: Freeing a non free bucket."));
	if ((zone->uz_flags & UMA_ZFLAG_BUCKET) == 0)
		udata = (void *)(uintptr_t)zone->uz_flags;
	ubz = bucket_zone_lookup(bucket->ub_entries);
	uma_zfree_arg(ubz->ubz_zone, bucket, udata);
}

static void
bucket_zone_drain(void)
{
	struct uma_bucket_zone *ubz;

	for (ubz = &bucket_zones[0]; ubz->ubz_entries != 0; ubz++)
		zone_drain(ubz->ubz_zone);
}

static void
zone_log_warning(uma_zone_t zone)
{
	static const struct timeval warninterval = { 300, 0 };

	if (!zone_warnings || zone->uz_warning == NULL)
		return;

	if (uma_ratecheck(&zone->uz_ratecheck, &warninterval))
		printf("[zone: %s] %s\n", zone->uz_name, zone->uz_warning);
}

static inline void
zone_maxaction(uma_zone_t zone)
{

	if (zone->uz_maxaction.ta_func != NULL)
		uma_taskqueue_enqueue(&zone->uz_maxaction);
}

static void
zone_foreach_keg(uma_zone_t zone, void (*kegfn)(uma_keg_t))
{
	uma_klink_t klink;

	LIST_FOREACH(klink, &zone->uz_kegs, kl_link)
		kegfn(klink->kl_keg);
}

/*
 * Routine called by timeout which is used to fire off some time interval
 * based calculations.  (stats, hash size, etc.)
 *
 * Arguments:
 *	arg   Unused
 *
 * Returns:
 *	Nothing
 */
static void *
uma_timeout(void *unused)
{
	bucket_enable();
	for (;;) {
		sleep(UMA_TIMEOUT);
		zone_foreach(zone_timeout);
	}
	return 0;
}

/*
 * Routine to perform timeout driven calculations.  This expands the
 * hashes and does per cpu statistics aggregation.
 *
 *  Returns nothing.
 */
static void
keg_timeout(uma_keg_t keg)
{
	u_int slabs;

	KEG_LOCK(keg);
	/*
	 * Expand the keg hash table.
	 *
	 * This is done if the number of slabs is larger than the hash size.
	 * What I'm trying to do here is completely reduce collisions.  This
	 * may be a little aggressive.  Should I allow for two collisions max?
	 */
	if (keg->uk_flags & UMA_ZONE_HASH &&
	    (slabs = keg->uk_pages / keg->uk_ppera) >
	     keg->uk_hash.uh_hashsize) {
		struct uma_hash newhash;
		struct uma_hash oldhash;
		int ret;

		/*
		 * This is so involved because allocating and freeing
		 * while the keg lock is held will lead to deadlock.
		 * I have to do everything in stages and check for
		 * races.
		 */
		KEG_UNLOCK(keg);
		ret = hash_alloc(&newhash, 1 << fls(slabs));
		KEG_LOCK(keg);
		if (ret) {
			if (hash_expand(&keg->uk_hash, &newhash)) {
				oldhash = keg->uk_hash;
				keg->uk_hash = newhash;
			} else
				oldhash = newhash;

			KEG_UNLOCK(keg);
			hash_free(&oldhash);
			return;
		}
	}
	KEG_UNLOCK(keg);
}

static void
zone_timeout(uma_zone_t zone)
{

	zone_foreach_keg(zone, &keg_timeout);
}

/*
 * Allocate and zero fill the next sized hash table from the appropriate
 * backing store.
 *
 * Arguments:
 *	hash  A new hash structure with the old hash size in uh_hashsize
 *
 * Returns:
 *	1 on success and 0 on failure.
 */
static int
hash_alloc(struct uma_hash *hash, u_int size)
{
	size_t alloc;

	KASSERT(powerof2(size), ("hash size must be power of 2"));
	if (size > UMA_HASH_SIZE_INIT)  {
		hash->uh_hashsize = size;
		alloc = sizeof(hash->uh_slab_hash[0]) * hash->uh_hashsize;
		hash->uh_slab_hash = (struct slabhead *)malloc(alloc);
	} else {
		alloc = sizeof(hash->uh_slab_hash[0]) * UMA_HASH_SIZE_INIT;
		hash->uh_slab_hash = zone_alloc_item(hashzone, NULL,
		    M_WAITOK);
		hash->uh_hashsize = UMA_HASH_SIZE_INIT;
	}
	if (hash->uh_slab_hash) {
		bzero(hash->uh_slab_hash, alloc);
		hash->uh_hashmask = hash->uh_hashsize - 1;
		return (1);
	}

	return (0);
}

/*
 * Expands the hash table for HASH zones.  This is done from zone_timeout
 * to reduce collisions.  This must not be done in the regular allocation
 * path, otherwise, we can recurse on the vm while allocating pages.
 *
 * Arguments:
 *	oldhash  The hash you want to expand
 *	newhash  The hash structure for the new table
 *
 * Returns:
 *	Nothing
 *
 * Discussion:
 */
static int
hash_expand(struct uma_hash *oldhash, struct uma_hash *newhash)
{
	uma_slab_t slab;
	u_int hval;
	u_int idx;

	if (!newhash->uh_slab_hash)
		return (0);

	if (oldhash->uh_hashsize >= newhash->uh_hashsize)
		return (0);

	/*
	 * I need to investigate hash algorithms for resizing without a
	 * full rehash.
	 */

	for (idx = 0; idx < oldhash->uh_hashsize; idx++)
		while (!SLIST_EMPTY(&oldhash->uh_slab_hash[idx])) {
			slab = SLIST_FIRST(&oldhash->uh_slab_hash[idx]);
			SLIST_REMOVE_HEAD(&oldhash->uh_slab_hash[idx], us_hlink);
			hval = UMA_HASH(newhash, slab->us_data);
			SLIST_INSERT_HEAD(&newhash->uh_slab_hash[hval],
			    slab, us_hlink);
		}

	return (1);
}

/*
 * Free the hash bucket to the appropriate backing store.
 *
 * Arguments:
 *	slab_hash  The hash bucket we're freeing
 *	hashsize   The number of entries in that hash bucket
 *
 * Returns:
 *	Nothing
 */
static void
hash_free(struct uma_hash *hash)
{
	if (hash->uh_slab_hash == NULL)
		return;
	if (hash->uh_hashsize == UMA_HASH_SIZE_INIT)
		zone_free_item(hashzone, hash->uh_slab_hash, NULL, SKIP_NONE);
	else
		free(hash->uh_slab_hash);
}

/*
 * Frees all outstanding items in a bucket
 *
 * Arguments:
 *	zone   The zone to free to, must be unlocked.
 *	bucket The free/alloc bucket with items, cpu queue must be locked.
 *
 * Returns:
 *	Nothing
 */

static void
bucket_drain(uma_zone_t zone, uma_bucket_t bucket)
{
	int i;

	if (bucket == NULL)
		return;

	if (zone->uz_fini)
		for (i = 0; i < bucket->ub_cnt; i++) 
			zone->uz_fini(bucket->ub_bucket[i], zone->uz_size);
	zone->uz_release(zone->uz_arg, bucket->ub_bucket, bucket->ub_cnt);
	bucket->ub_cnt = 0;
}

/*
 * Drains the per cpu caches for a zone.
 *
 * NOTE: This may only be called while the zone is being turn down, and not
 * during normal operation.  This is necessary in order that we do not have
 * to migrate CPUs to drain the per-CPU caches.
 *
 * Arguments:
 *	zone     The zone to drain, must be unlocked.
 *
 * Returns:
 *	Nothing
 */
static void
cache_drain(uma_zone_t zone)
{
	uma_cache_t cache;
	int cpu;

	/*
	 * XXX: It is safe to not lock the per-CPU caches, because we're
	 * tearing down the zone anyway.  I.e., there will be no further use
	 * of the caches at this point.
	 *
	 * XXX: It would good to be able to assert that the zone is being
	 * torn down to prevent improper use of cache_drain().
	 *
	 * XXX: We lock the zone before passing into bucket_cache_drain() as
	 * it is used elsewhere.  Should the tear-down path be made special
	 * there in some form?
	 */
	CPU_FOREACH(cpu) {
		cache = &zone->uz_cpu[cpu];
		bucket_drain(zone, cache->uc_allocbucket);
		bucket_drain(zone, cache->uc_freebucket);
		if (cache->uc_allocbucket != NULL)
			bucket_free(zone, cache->uc_allocbucket, NULL);
		if (cache->uc_freebucket != NULL)
			bucket_free(zone, cache->uc_freebucket, NULL);
		cache->uc_allocbucket = cache->uc_freebucket = NULL;
	}
	ZONE_LOCK(zone);
	bucket_cache_drain(zone);
	ZONE_UNLOCK(zone);
}

static void
cache_shrink(uma_zone_t zone)
{

	if (zone->uz_flags & UMA_ZFLAG_INTERNAL)
		return;

	ZONE_LOCK(zone);
	zone->uz_count = (zone->uz_count_min + zone->uz_count) / 2;
	ZONE_UNLOCK(zone);
}

static void
cache_drain_safe_cpu(uma_zone_t zone)
{
	uma_cache_t cache;
	uma_bucket_t b1, b2;

	if (zone->uz_flags & UMA_ZFLAG_INTERNAL)
		return;

	b1 = b2 = NULL;
	ZONE_LOCK(zone);
	cache = &zone->uz_cpu[curcpu];
	CACHE_LOCK(cache);
	if (cache->uc_allocbucket) {
		if (cache->uc_allocbucket->ub_cnt != 0)
			LIST_INSERT_HEAD(&zone->uz_buckets,
			    cache->uc_allocbucket, ub_link);
		else
			b1 = cache->uc_allocbucket;
		cache->uc_allocbucket = NULL;
	}
	if (cache->uc_freebucket) {
		if (cache->uc_freebucket->ub_cnt != 0)
			LIST_INSERT_HEAD(&zone->uz_buckets,
			    cache->uc_freebucket, ub_link);
		else
			b2 = cache->uc_freebucket;
		cache->uc_freebucket = NULL;
	}
	CACHE_UNLOCK(cache);
	ZONE_UNLOCK(zone);
	if (b1)
		bucket_free(zone, b1, NULL);
	if (b2)
		bucket_free(zone, b2, NULL);
}

/*
 * Safely drain per-CPU caches of a zone(s) to alloc bucket.
 * This is an expensive call because it needs to bind to all CPUs
 * one by one and enter a critical section on each of them in order
 * to safely access their cache buckets.
 * Zone lock must not be held on call this function.
 */
static void
cache_drain_safe(uma_zone_t zone)
{
	int cpu;

	/*
	 * Polite bucket sizes shrinking was not enouth, shrink aggressively.
	 */
	if (zone)
		cache_shrink(zone);
	else
		zone_foreach(cache_shrink);

	CPU_FOREACH(cpu) {
		sched_bind(cpu);

		if (zone)
			cache_drain_safe_cpu(zone);
		else
			zone_foreach(cache_drain_safe_cpu);
	}
	sched_unbind();
}

/*
 * Drain the cached buckets from a zone.  Expects a locked zone on entry.
 */
static void
bucket_cache_drain(uma_zone_t zone)
{
	uma_bucket_t bucket;

	/*
	 * Drain the bucket queues and free the buckets, we just keep two per
	 * cpu (alloc/free).
	 */
	while ((bucket = LIST_FIRST(&zone->uz_buckets)) != NULL) {
		LIST_REMOVE(bucket, ub_link);
		ZONE_UNLOCK(zone);
		bucket_drain(zone, bucket);
		bucket_free(zone, bucket, NULL);
		ZONE_LOCK(zone);
	}

	/*
	 * Shrink further bucket sizes.  Price of single zone lock collision
	 * is probably lower then price of global cache drain.
	 */
	if (zone->uz_count > zone->uz_count_min)
		zone->uz_count--;
}

static void
keg_free_slab(uma_keg_t keg, uma_slab_t slab, int start)
{
	uint8_t *mem;
	int i;
	uint8_t flags;

	mem = slab->us_data;
	flags = slab->us_flags;
	i = start;
	if (keg->uk_fini != NULL) {
		for (i--; i > -1; i--)
			keg->uk_fini(slab->us_data + (keg->uk_rsize * i),
			    keg->uk_size);
	}
	if (keg->uk_flags & UMA_ZONE_OFFPAGE)
		zone_free_item(keg->uk_slabzone, slab, NULL, SKIP_NONE);
#ifdef UMA_DEBUG
	printf("%s: Returning %d bytes.\n", keg->uk_name,
	    PAGE_SIZE * keg->uk_ppera);
#endif
	keg->uk_freef(mem, PAGE_SIZE * keg->uk_ppera, flags);
}

/*
 * Frees pages from a keg back to the system.  This is done on demand from
 * the pageout daemon.
 *
 * Returns nothing.
 */
static void
keg_drain(uma_keg_t keg)
{
	struct slabhead freeslabs = { 0 };
	uma_slab_t slab, tmp;

	/*
	 * We don't want to take pages from statically allocated kegs at this
	 * time
	 */
	if (keg->uk_flags & UMA_ZONE_NOFREE || keg->uk_freef == NULL)
		return;

#ifdef UMA_DEBUG
	printf("%s free items: %u\n", keg->uk_name, keg->uk_free);
#endif
	KEG_LOCK(keg);
	if (keg->uk_free == 0)
		goto finished;

	LIST_FOREACH_SAFE(slab, &keg->uk_free_slab, us_link, tmp) {
		/* We have nowhere to free these to. */
		if (slab->us_flags & UMA_SLAB_BOOT)
			continue;

		LIST_REMOVE(slab, us_link);
		keg->uk_pages -= keg->uk_ppera;
		keg->uk_free -= keg->uk_ipers;

		if (keg->uk_flags & UMA_ZONE_HASH)
			UMA_HASH_REMOVE(&keg->uk_hash, slab, slab->us_data);

		SLIST_INSERT_HEAD(&freeslabs, slab, us_hlink);
	}
finished:
	KEG_UNLOCK(keg);

	while ((slab = SLIST_FIRST(&freeslabs)) != NULL) {
		SLIST_REMOVE(&freeslabs, slab, uma_slab, us_hlink);
		keg_free_slab(keg, slab, keg->uk_ipers);
	}
}

static void
zone_drain_wait(uma_zone_t zone, int waitok)
{

	/*
	 * Set draining to interlock with zone_dtor() so we can release our
	 * locks as we go.  Only dtor() should do a WAITOK call since it
	 * is the only call that knows the structure will still be available
	 * when it wakes up.
	 */
	ZONE_LOCK(zone);
	while (zone->uz_flags & UMA_ZFLAG_DRAINING) {
		if (waitok == M_NOWAIT)
			goto out;
		uma_mtx_sleep(&zone->uz_cond, zone->uz_lockptr);
	}
	zone->uz_flags |= UMA_ZFLAG_DRAINING;
	bucket_cache_drain(zone);
	ZONE_UNLOCK(zone);
	/*
	 * The DRAINING flag protects us from being freed while
	 * we're running.  Normally the uma_rwlock would protect us but we
	 * must be able to release and acquire the right lock for each keg.
	 */
	zone_foreach_keg(zone, &keg_drain);
	ZONE_LOCK(zone);
	zone->uz_flags &= ~UMA_ZFLAG_DRAINING;
	pthread_cond_broadcast(&zone->uz_cond);
out:
	ZONE_UNLOCK(zone);
}

void
zone_drain(uma_zone_t zone)
{

	zone_drain_wait(zone, M_NOWAIT);
}

/*
 * Allocate a new slab for a keg.  This does not insert the slab onto a list.
 *
 * Arguments:
 *	wait  Shall we wait?
 *
 * Returns:
 *	The slab that was allocated or NULL if there is no memory and the
 *	caller specified M_NOWAIT.
 */
static uma_slab_t
keg_alloc_slab(uma_keg_t keg, uma_zone_t zone, int wait)
{
	uma_alloc allocf;
	uma_slab_t slab;
	uint8_t *mem;
	uint8_t flags;
	int i;

	uma_mtx_assert(&keg->uk_lock, MA_OWNED);
	slab = NULL;
	mem = NULL;

#ifdef UMA_DEBUG
	printf("alloc_slab:  Allocating a new slab for %s\n", keg->uk_name);
#endif
	allocf = keg->uk_allocf;
	KEG_UNLOCK(keg);

	if (keg->uk_flags & UMA_ZONE_OFFPAGE) {
		slab = zone_alloc_item(keg->uk_slabzone, NULL, wait);
		if (slab == NULL)
			goto out;
	}

	/*
	 * This reproduces the old vm_zone behavior of zero filling pages the
	 * first time they are added to a zone.
	 *
	 * Malloced items are zeroed in uma_zalloc.
	 */

	if ((keg->uk_flags & UMA_ZONE_MALLOC) == 0)
		wait |= M_ZERO;
	else
		wait &= ~M_ZERO;

	if (keg->uk_flags & UMA_ZONE_NODUMP)
		wait |= M_NODUMP;

	/* zone is passed for legacy reasons. */
	mem = allocf(zone, keg->uk_ppera * PAGE_SIZE, &flags, wait);
	if (mem == NULL) {
		if (keg->uk_flags & UMA_ZONE_OFFPAGE)
			zone_free_item(keg->uk_slabzone, slab, NULL, SKIP_NONE);
		slab = NULL;
		goto out;
	}

	/* Point the slab into the allocated memory */
	if (!(keg->uk_flags & UMA_ZONE_OFFPAGE))
		slab = (uma_slab_t )(mem + keg->uk_pgoff);

	slab->us_keg = keg;
	slab->us_data = mem;
	slab->us_freecount = keg->uk_ipers;
	slab->us_flags = flags;
	BIT_FILL(SLAB_SETSIZE, &slab->us_free);
#ifdef INVARIANTS
	BIT_ZERO(SLAB_SETSIZE, &slab->us_debugfree);
#endif

	if (keg->uk_init != NULL) {
		for (i = 0; i < keg->uk_ipers; i++)
			if (keg->uk_init(slab->us_data + (keg->uk_rsize * i),
			    keg->uk_size, wait) != 0)
				break;
		if (i != keg->uk_ipers) {
			keg_free_slab(keg, slab, i);
			slab = NULL;
			goto out;
		}
	}
out:
	KEG_LOCK(keg);

	if (slab != NULL) {
		if (keg->uk_flags & UMA_ZONE_HASH)
			UMA_HASH_INSERT(&keg->uk_hash, slab, mem);

		keg->uk_pages += keg->uk_ppera;
		keg->uk_free += keg->uk_ipers;
	}

	return (slab);
}

/*
 * This function is intended to be used early on in place of page_alloc() so
 * that we may use the boot time page cache to satisfy allocations before
 * the VM is ready.
 */
static void *
startup_alloc(uma_zone_t zone, vm_size_t bytes, uint8_t *pflag, int wait)
{
	uma_keg_t keg;

	keg = zone_first_keg(zone);
	keg->uk_allocf = page_alloc;
	return keg->uk_allocf(zone, bytes, pflag, wait);
}

/*
 * Allocates a number of pages from the system
 *
 * Arguments:
 *	bytes  The number of bytes requested
 *	wait  Shall we wait?
 *
 * Returns:
 *	A pointer to the alloced memory or possibly
 *	NULL if M_NOWAIT is set.
 */
static void *
page_alloc(uma_zone_t zone, vm_size_t bytes, uint8_t *pflag, int wait)
{
	void *p = 0;

	*pflag = UMA_SLAB_KMEM;
	p = uma_mmap(bytes);
	if (p == MAP_FAILED)
		return NULL;
	return p;
}

/*
 * Frees a number of pages to the system
 *
 * Arguments:
 *	mem   A pointer to the memory to be freed
 *	size  The size of the memory being freed
 *	flags The original p->us_flags field
 *
 * Returns:
 *	Nothing
 */
static void
page_free(void *mem, vm_size_t size, uint8_t flags)
{
    free(mem);
}

/*
 * Zero fill initializer
 *
 * Arguments/Returns follow uma_init specifications
 */
static int
zero_init(void *mem, int size, int flags)
{
	bzero(mem, size);
	return (0);
}

/*
 * Finish creating a small uma keg.  This calculates ipers, and the keg size.
 *
 * Arguments
 *	keg  The zone we should initialize
 *
 * Returns
 *	Nothing
 */
static void
keg_small_init(uma_keg_t keg)
{
	u_int rsize;
	u_int memused;
	u_int wastedspace;
	u_int shsize;
	u_int slabsize;

#if 0
	if (keg->uk_flags & UMA_ZONE_PCPU) {
		u_int ncpus = (mp_maxid + 1) ? (mp_maxid + 1) : MAXCPU;

		slabsize = sizeof(struct pcpu);
		keg->uk_ppera = howmany(ncpus * sizeof(struct pcpu),
		    PAGE_SIZE);
	} else
#endif
 	{
		slabsize = UMA_SLAB_SIZE;
		keg->uk_ppera = 1;
	}

	/*
	 * Calculate the size of each allocation (rsize) according to
	 * alignment.  If the requested size is smaller than we have
	 * allocation bits for we round it up.
	 */
	rsize = keg->uk_size;
	if (rsize < slabsize / SLAB_SETSIZE)
		rsize = slabsize / SLAB_SETSIZE;
	if (rsize & keg->uk_align)
		rsize = (rsize & ~keg->uk_align) + (keg->uk_align + 1);
	keg->uk_rsize = rsize;

	if (keg->uk_flags & UMA_ZONE_OFFPAGE)
		shsize = 0;
	else 
		shsize = sizeof(struct uma_slab);

	if (rsize <= slabsize - shsize)
		keg->uk_ipers = (slabsize - shsize) / rsize;
	else {
		/* Handle special case when we have 1 item per slab, so
		 * alignment requirement can be relaxed. */
		KASSERT(keg->uk_size <= slabsize - shsize,
		    ("%s: size %u greater than slab", __func__, keg->uk_size));
		keg->uk_ipers = 1;
	}
	KASSERT(keg->uk_ipers > 0 && keg->uk_ipers <= SLAB_SETSIZE,
	    ("%s: keg->uk_ipers %u", __func__, keg->uk_ipers));

	memused = keg->uk_ipers * rsize + shsize;
	wastedspace = slabsize - memused;

	/*
	 * We can't do OFFPAGE if we're internal or if we've been
	 * asked to not go to the VM for buckets.  If we do this we
	 * may end up going to the VM  for slabs which we do not
	 * want to do if we're UMA_ZFLAG_CACHEONLY as a result
	 * of UMA_ZONE_VM, which clearly forbids it.
	 */
	if ((keg->uk_flags & UMA_ZFLAG_INTERNAL) ||
	    (keg->uk_flags & UMA_ZFLAG_CACHEONLY))
		return;

	/*
	 * See if using an OFFPAGE slab will limit our waste.  Only do
	 * this if it permits more items per-slab.
	 *
	 * XXX We could try growing slabsize to limit max waste as well.
	 * Historically this was not done because the VM could not
	 * efficiently handle contiguous allocations.
	 */
	if ((wastedspace >= slabsize / UMA_MAX_WASTE) &&
	    (keg->uk_ipers < (slabsize / keg->uk_rsize))) {
		keg->uk_ipers = slabsize / keg->uk_rsize;
		KASSERT(keg->uk_ipers > 0 && keg->uk_ipers <= SLAB_SETSIZE,
		    ("%s: keg->uk_ipers %u", __func__, keg->uk_ipers));
#ifdef UMA_DEBUG
		printf("UMA decided we need offpage slab headers for "
		    "keg: %s, calculated wastedspace = %d, "
		    "maximum wasted space allowed = %d, "
		    "calculated ipers = %d, "
		    "new wasted space = %d\n", keg->uk_name, wastedspace,
		    slabsize / UMA_MAX_WASTE, keg->uk_ipers,
		    slabsize - keg->uk_ipers * keg->uk_rsize);
#endif
		keg->uk_flags |= UMA_ZONE_OFFPAGE;
	}

	if ((keg->uk_flags & UMA_ZONE_OFFPAGE))
		keg->uk_flags |= UMA_ZONE_HASH;
}

/*
 * Finish creating a large (> UMA_SLAB_SIZE) uma kegs.  Just give in and do
 * OFFPAGE for now.  When I can allow for more dynamic slab sizes this will be
 * more complicated.
 *
 * Arguments
 *	keg  The keg we should initialize
 *
 * Returns
 *	Nothing
 */
static void
keg_large_init(uma_keg_t keg)
{
	u_int shsize;

	KASSERT(keg != NULL, ("Keg is null in keg_large_init"));
	KASSERT((keg->uk_flags & UMA_ZFLAG_CACHEONLY) == 0,
	    ("keg_large_init: Cannot large-init a UMA_ZFLAG_CACHEONLY keg"));
	KASSERT((keg->uk_flags & UMA_ZONE_PCPU) == 0,
	    ("%s: Cannot large-init a UMA_ZONE_PCPU keg", __func__));

	keg->uk_ppera = howmany(keg->uk_size, PAGE_SIZE);
	keg->uk_ipers = 1;
	keg->uk_rsize = keg->uk_size;

	/* Check whether we have enough space to not do OFFPAGE. */
	if ((keg->uk_flags & UMA_ZONE_OFFPAGE) == 0) {
		shsize = sizeof(struct uma_slab);
		if (shsize & UMA_ALIGN_PTR)
			shsize = (shsize & ~UMA_ALIGN_PTR) +
			    (UMA_ALIGN_PTR + 1);

		if (PAGE_SIZE * keg->uk_ppera - keg->uk_rsize < shsize) {
			/*
			 * We can't do OFFPAGE if we're internal, in which case
			 * we need an extra page per allocation to contain the
			 * slab header.
			 */
			if ((keg->uk_flags & UMA_ZFLAG_INTERNAL) == 0)
				keg->uk_flags |= UMA_ZONE_OFFPAGE;
			else
				keg->uk_ppera++;
		}
	}

	if ((keg->uk_flags & UMA_ZONE_OFFPAGE))
		keg->uk_flags |= UMA_ZONE_HASH;
}

static void
keg_cachespread_init(uma_keg_t keg)
{
	int alignsize;
	int trailer;
	int pages;
	int rsize;

	KASSERT((keg->uk_flags & UMA_ZONE_PCPU) == 0,
	    ("%s: Cannot cachespread-init a UMA_ZONE_PCPU keg", __func__));

	alignsize = keg->uk_align + 1;
	rsize = keg->uk_size;
	/*
	 * We want one item to start on every align boundary in a page.  To
	 * do this we will span pages.  We will also extend the item by the
	 * size of align if it is an even multiple of align.  Otherwise, it
	 * would fall on the same boundary every time.
	 */
	if (rsize & keg->uk_align)
		rsize = (rsize & ~keg->uk_align) + alignsize;
	if ((rsize & alignsize) == 0)
		rsize += alignsize;
	trailer = rsize - keg->uk_size;
	pages = (rsize * (PAGE_SIZE / alignsize)) / PAGE_SIZE;
	pages = MIN(pages, (128 * 1024) / PAGE_SIZE);
	keg->uk_rsize = rsize;
	keg->uk_ppera = pages;
	keg->uk_ipers = ((pages * PAGE_SIZE) + trailer) / rsize;
	keg->uk_flags |= UMA_ZONE_OFFPAGE;
	KASSERT(keg->uk_ipers <= SLAB_SETSIZE,
	    ("%s: keg->uk_ipers too high(%d) increase max_ipers", __func__,
	    keg->uk_ipers));
}

/*
 * Keg header ctor.  This initializes all fields, locks, etc.  And inserts
 * the keg onto the global keg list.
 *
 * Arguments/Returns follow uma_ctor specifications
 *	udata  Actually uma_kctor_args
 */
static int
keg_ctor(void *mem, int size, void *udata, int flags)
{
	struct uma_kctor_args *arg = udata;
	uma_keg_t keg = mem;
	uma_zone_t zone;

	bzero(keg, size);
	keg->uk_size = arg->size;
	keg->uk_init = arg->uminit;
	keg->uk_fini = arg->fini;
	keg->uk_align = arg->align;
	keg->uk_free = 0;
	keg->uk_reserve = 0;
	keg->uk_pages = 0;
	keg->uk_flags = arg->flags;
	keg->uk_allocf = page_alloc;
	keg->uk_freef = page_free;
	keg->uk_slabzone = NULL;

	/*
	 * The master zone is passed to us at keg-creation time.
	 */
	zone = arg->zone;
	keg->uk_name = zone->uz_name;

	if (arg->flags & UMA_ZONE_ZINIT)
		keg->uk_init = zero_init;

	if (arg->flags & UMA_ZONE_PCPU)
		keg->uk_flags |= UMA_ZONE_OFFPAGE;

	if (keg->uk_flags & UMA_ZONE_CACHESPREAD) {
		keg_cachespread_init(keg);
	} else {
		if (keg->uk_size > (UMA_SLAB_SIZE - sizeof(struct uma_slab)))
			keg_large_init(keg);
		else
			keg_small_init(keg);
	}

	if (keg->uk_flags & UMA_ZONE_OFFPAGE)
		keg->uk_slabzone = slabzone;

	/*
	 * If we haven't booted yet we need allocations to go through the
	 * startup cache until the vm is ready.
	 */
	if (keg->uk_ppera == 1) {
		if (booted < UMA_STARTUP2)
			keg->uk_allocf = startup_alloc;
	} else if (booted < UMA_STARTUP2 &&
	    (keg->uk_flags & UMA_ZFLAG_INTERNAL))
		keg->uk_allocf = startup_alloc;

	/*
	 * Initialize keg's lock
	 */
	KEG_LOCK_INIT(keg, (arg->flags & UMA_ZONE_MTXCLASS));
	pthread_cond_init(&keg->uk_cond, NULL);
	/*
	 * If we're putting the slab header in the actual page we need to
	 * figure out where in each page it goes.  This calculates a right
	 * justified offset into the memory on an ALIGN_PTR boundary.
	 */
	if (!(keg->uk_flags & UMA_ZONE_OFFPAGE)) {
		u_int totsize;

		/* Size of the slab struct and free list */
		totsize = sizeof(struct uma_slab);

		if (totsize & UMA_ALIGN_PTR)
			totsize = (totsize & ~UMA_ALIGN_PTR) +
			    (UMA_ALIGN_PTR + 1);
		keg->uk_pgoff = (PAGE_SIZE * keg->uk_ppera) - totsize;

		/*
		 * The only way the following is possible is if with our
		 * UMA_ALIGN_PTR adjustments we are now bigger than
		 * UMA_SLAB_SIZE.  I haven't checked whether this is
		 * mathematically possible for all cases, so we make
		 * sure here anyway.
		 */
		totsize = keg->uk_pgoff + sizeof(struct uma_slab);
		if (totsize > PAGE_SIZE * keg->uk_ppera) {
			printf("zone %s ipers %d rsize %d size %d\n",
			    zone->uz_name, keg->uk_ipers, keg->uk_rsize,
			    keg->uk_size);
			panic("UMA slab won't fit.");
		}
	}

	if (keg->uk_flags & UMA_ZONE_HASH)
		hash_alloc(&keg->uk_hash, 0);

#ifdef UMA_DEBUG
	printf("UMA: %s(%p) size %d(%d) flags %#x ipers %d ppera %d out %d free %d\n",
	    zone->uz_name, zone, keg->uk_size, keg->uk_rsize, keg->uk_flags,
	    keg->uk_ipers, keg->uk_ppera,
	    (keg->uk_pages / keg->uk_ppera) * keg->uk_ipers - keg->uk_free,
	    keg->uk_free);
#endif

	LIST_INSERT_HEAD(&keg->uk_zones, zone, uz_link);

	uma_rw_wlock(&uma_rwlock);
	LIST_INSERT_HEAD(&uma_kegs, keg, uk_link);
	uma_rw_wunlock(&uma_rwlock);
	return (0);
}

/*
 * Zone header ctor.  This initializes all fields, locks, etc.
 *
 * Arguments/Returns follow uma_ctor specifications
 *	udata  Actually uma_zctor_args
 */
static int
zone_ctor(void *mem, int size, void *udata, int flags)
{
	struct uma_zctor_args *arg = udata;
	uma_zone_t zone = mem;
	uma_keg_t keg;
	int cpu, sz;

	bzero(zone, size);
	zone->uz_name = arg->name;
	zone->uz_ctor = arg->ctor;
	zone->uz_dtor = arg->dtor;
	zone->uz_slab = zone_fetch_slab;
	zone->uz_init = NULL;
	zone->uz_fini = NULL;
	zone->uz_allocs = 0;
	zone->uz_frees = 0;
	zone->uz_fails = 0;
	zone->uz_sleeps = 0;
	zone->uz_count = 0;
	zone->uz_count_min = 0;
	zone->uz_flags = 0;
	zone->uz_warning = NULL;
	timerclear(&zone->uz_ratecheck);
	keg = arg->keg;

	ZONE_LOCK_INIT(zone, (arg->flags & UMA_ZONE_MTXCLASS));
	for (sz = offsetof(struct uma_zone, uz_cpu), cpu = 0;
	     sz < size; ++cpu, sz += sizeof(struct uma_cache))
		uma_mtx_init(&zone->uz_cpu[cpu].uc_mtx, "uma cache mtx", "", 0);

	/*
	 * This is a pure cache zone, no kegs.
	 */
	if (arg->import) {
		zone->uz_flags = arg->flags;
		zone->uz_size = arg->size;
		zone->uz_import = arg->import;
		zone->uz_release = arg->release;
		zone->uz_arg = arg->arg;
		zone->uz_lockptr = &zone->uz_lock;
		uma_rw_wlock(&uma_rwlock);
		LIST_INSERT_HEAD(&uma_cachezones, zone, uz_link);
		uma_rw_wunlock(&uma_rwlock);
		goto out;
	}

	/*
	 * Use the regular zone/keg/slab allocator.
	 */
	zone->uz_import = (uma_import)zone_import;
	zone->uz_release = (uma_release)zone_release;
	zone->uz_arg = zone; 

	if (keg == NULL) {
		if ((keg = uma_kcreate(zone, arg->size, arg->uminit, arg->fini,
		    arg->align, arg->flags)) == NULL)
			return (ENOMEM);
	} else {
		struct uma_kctor_args karg;
		int error;

		/* We should only be here from uma_startup() */
		karg.size = arg->size;
		karg.uminit = arg->uminit;
		karg.fini = arg->fini;
		karg.align = arg->align;
		karg.flags = arg->flags;
		karg.zone = zone;
		error = keg_ctor(arg->keg, sizeof(struct uma_keg), &karg,
		    flags);
		if (error)
			return (error);
	}

	/*
	 * Link in the first keg.
	 */
	zone->uz_klink.kl_keg = keg;
	LIST_INSERT_HEAD(&zone->uz_kegs, &zone->uz_klink, kl_link);
	zone->uz_lockptr = &keg->uk_lock;
	zone->uz_size = keg->uk_size;
	zone->uz_flags |= (keg->uk_flags &
	    (UMA_ZONE_INHERIT | UMA_ZFLAG_INHERIT));

	/*
	 * Some internal zones don't have room allocated for the per cpu
	 * caches.  If we're internal, bail out here.
	 */
	if (keg->uk_flags & UMA_ZFLAG_INTERNAL) {
		return (0);
	}

out:
	KASSERT((arg->flags & (UMA_ZONE_MAXBUCKET | UMA_ZONE_NOBUCKET)) !=
	    (UMA_ZONE_MAXBUCKET | UMA_ZONE_NOBUCKET),
	    ("Invalid zone flag combination"));
	if ((arg->flags & UMA_ZONE_MAXBUCKET) != 0)
		zone->uz_count = BUCKET_MAX;
	else if ((arg->flags & UMA_ZONE_NOBUCKET) != 0)
		zone->uz_count = 0;
	else
		zone->uz_count = bucket_select(zone->uz_size);
	zone->uz_count_min = zone->uz_count;

	return (0);
}

/*
 * Keg header dtor.  This frees all data, destroys locks, frees the hash
 * table and removes the keg from the global list.
 *
 * Arguments/Returns follow uma_dtor specifications
 *	udata  unused
 */
static void
keg_dtor(void *arg, int size, void *udata)
{
	uma_keg_t keg;

	keg = (uma_keg_t)arg;
	KEG_LOCK(keg);
	if (keg->uk_free != 0) {
		printf("Freed UMA keg (%s) was not empty (%d items). "
		    " Lost %d pages of memory.\n",
		    keg->uk_name ? keg->uk_name : "",
		    keg->uk_free, keg->uk_pages);
	}
	KEG_UNLOCK(keg);

	hash_free(&keg->uk_hash);

	KEG_LOCK_FINI(keg);
	pthread_cond_destroy(&keg->uk_cond);
}

/*
 * Zone header dtor.
 *
 * Arguments/Returns follow uma_dtor specifications
 *	udata  unused
 */
static void
zone_dtor(void *arg, int size, void *udata)
{
	uma_klink_t klink;
	uma_zone_t zone;
	uma_keg_t keg;
	int cpu, sz;

	zone = (uma_zone_t)arg;
	keg = zone_first_keg(zone);

	if (!(zone->uz_flags & UMA_ZFLAG_INTERNAL))
		cache_drain(zone);

	uma_rw_wlock(&uma_rwlock);
	LIST_REMOVE(zone, uz_link);
	uma_rw_wunlock(&uma_rwlock);
	/*
	 * XXX there are some races here where
	 * the zone can be drained but zone lock
	 * released and then refilled before we
	 * remove it... we dont care for now
	 */
	zone_drain_wait(zone, M_WAITOK);
	/*
	 * Unlink all of our kegs.
	 */
	while ((klink = LIST_FIRST(&zone->uz_kegs)) != NULL) {
		klink->kl_keg = NULL;
		LIST_REMOVE(klink, kl_link);
		if (klink == &zone->uz_klink)
			continue;
		free(klink);
	}
	/*
	 * We only destroy kegs from non secondary zones.
	 */
	if (keg != NULL)  {
		uma_rw_wlock(&uma_rwlock);
		LIST_REMOVE(keg, uk_link);
		uma_rw_wunlock(&uma_rwlock);
		zone_free_item(kegs, keg, NULL, SKIP_NONE);
	}
	ZONE_LOCK_FINI(zone);

	for (sz = offsetof(struct uma_zone, uz_cpu), cpu = 0;
	     sz < size; ++cpu, sz += sizeof(struct uma_cache)) {
		uma_mtx_destroy(&zone->uz_cpu[cpu].uc_mtx);
	}
}

/*
 * Traverses every zone in the system and calls a callback
 *
 * Arguments:
 *	zfunc  A pointer to a function which accepts a zone
 *		as an argument.
 *
 * Returns:
 *	Nothing
 */
static void
zone_foreach(void (*zfunc)(uma_zone_t))
{
	uma_keg_t keg;
	uma_zone_t zone;

	uma_rw_rlock(&uma_rwlock);
	LIST_FOREACH(keg, &uma_kegs, uk_link) {
		LIST_FOREACH(zone, &keg->uk_zones, uz_link)
			zfunc(zone);
	}
	uma_rw_runlock(&uma_rwlock);
}

/* Public functions */
/* See uma.h */
void
uma_startup(void)
{
	pthread_once(&init_once, uma_startup_impl);
}

static void
uma_startup_impl(void)
{
	struct uma_zctor_args args;

#ifdef UMA_DEBUG
	printf("Creating uma keg headers zone and keg.\n");
#endif
	uma_rw_init(&uma_rwlock, "UMA lock");

	/* "manually" create the initial zone */
	memset(&args, 0, sizeof(args));
	args.name = "UMA Kegs";
	args.size = sizeof(struct uma_keg);
	args.ctor = keg_ctor;
	args.dtor = keg_dtor;
	args.uminit = zero_init;
	args.fini = NULL;
	args.keg = &masterkeg;
	args.align = 32 - 1;
	args.flags = UMA_ZFLAG_INTERNAL;
	/* The initial zone has no Per cpu queues so it's smaller */
	zone_ctor(kegs, sizeof(struct uma_zone), &args, M_WAITOK);

#ifdef UMA_DEBUG
	printf("Creating uma zone headers zone and keg.\n");
#endif
	args.name = "UMA Zones";
	args.size = sizeof(struct uma_zone) +
	    (sizeof(struct uma_cache) * (mp_maxid + 1));
	args.ctor = zone_ctor;
	args.dtor = zone_dtor;
	args.uminit = zero_init;
	args.fini = NULL;
	args.keg = NULL;
	args.align = 32 - 1;
	args.flags = UMA_ZFLAG_INTERNAL;
	/* The initial zone has no Per cpu queues so it's smaller */
	zone_ctor(zones, sizeof(struct uma_zone), &args, M_WAITOK);

#ifdef UMA_DEBUG
	printf("Creating slab and hash zones.\n");
#endif

	/* Now make a zone for slab headers */
	slabzone = uma_zcreate_impl("UMA Slabs",
				sizeof(struct uma_slab),
				NULL, NULL, NULL, NULL,
				UMA_ALIGN_PTR, UMA_ZFLAG_INTERNAL);

	hashzone = uma_zcreate_impl("UMA Hash",
	    sizeof(struct slabhead *) * UMA_HASH_SIZE_INIT,
	    NULL, NULL, NULL, NULL,
	    UMA_ALIGN_PTR, UMA_ZFLAG_INTERNAL);

	bucket_init();

	booted = UMA_STARTUP;

#ifdef UMA_DEBUG
	printf("UMA startup complete.\n");
#endif

	uma_startup2();
	uma_startup3();
}

/* see uma.h */
static void
uma_startup2(void)
{
	booted = UMA_STARTUP2;
	bucket_enable();
	uma_sx_init(&uma_drain_lock, "umadrain");
#ifdef UMA_DEBUG
	printf("UMA startup2 complete.\n");
#endif
}


static void
uma_startup3(void)
{
#ifdef UMA_DEBUG
	printf("Starting callout.\n");
#endif
	pthread_create(&uma_timeout_td, NULL, uma_timeout, NULL);

#ifdef UMA_DEBUG
	printf("UMA startup3 complete.\n");
#endif
}

void
uma_shutdown(void)
{

	booted = UMA_SHUTDOWN;
}

static uma_keg_t
uma_kcreate(uma_zone_t zone, size_t size, uma_init uminit, uma_fini fini,
		int align, uint32_t flags)
{
	struct uma_kctor_args args;

	args.size = size;
	args.uminit = uminit;
	args.fini = fini;
	args.align = (align == UMA_ALIGN_CACHE) ? uma_align_cache : align;
	args.flags = flags;
	args.zone = zone;
	return (zone_alloc_item(kegs, &args, M_WAITOK));
}

/* See uma.h */
void
uma_set_align(int align)
{

	if (align != UMA_ALIGN_CACHE)
		uma_align_cache = align;
}

/* See uma.h */
uma_zone_t
uma_zcreate(const char *name, size_t size, uma_ctor ctor, uma_dtor dtor,
		uma_init uminit, uma_fini fini, int align, uint32_t flags)
{
	uma_startup();
	return uma_zcreate_impl(name, size, ctor, dtor, uminit, fini,
			align, flags);
}

static uma_zone_t
uma_zcreate_impl(const char *name, size_t size, uma_ctor ctor, uma_dtor dtor,
		uma_init uminit, uma_fini fini, int align, uint32_t flags)

{
	struct uma_zctor_args args;
	uma_zone_t res;
	bool locked;

	KASSERT(powerof2(align + 1), ("invalid zone alignment %d for \"%s\"",
	    align, name));

	/* This stuff is essential for the zone ctor */
	memset(&args, 0, sizeof(args));
	args.name = name;
	args.size = size;
	args.ctor = ctor;
	args.dtor = dtor;
	args.uminit = uminit;
	args.fini = fini;
	args.align = align;
	args.flags = flags;
	args.keg = NULL;

	if (booted < UMA_STARTUP2) {
		locked = false;
	} else {
		uma_sx_slock(&uma_drain_lock);
		locked = true;
	}
	res = zone_alloc_item(zones, &args, M_WAITOK);
	if (locked)
		uma_sx_sunlock(&uma_drain_lock);
	return (res);
}

/* See uma.h */
uma_zone_t
uma_zcache_create(char *name, int size, uma_ctor ctor, uma_dtor dtor,
		    uma_init zinit, uma_fini zfini, uma_import zimport,
		    uma_release zrelease, void *arg, int flags)
{
	uma_startup();
	return uma_zcache_create_impl(name, size, ctor, dtor,
		    zinit, zfini, zimport, zrelease, arg, flags);
}

static uma_zone_t
uma_zcache_create_impl(char *name, int size, uma_ctor ctor, uma_dtor dtor,
		    uma_init zinit, uma_fini zfini, uma_import zimport,
		    uma_release zrelease, void *arg, int flags)
{
	struct uma_zctor_args args;

	memset(&args, 0, sizeof(args));
	args.name = name;
	args.size = size;
	args.ctor = ctor;
	args.dtor = dtor;
	args.uminit = zinit;
	args.fini = zfini;
	args.import = zimport;
	args.release = zrelease;
	args.arg = arg;
	args.align = 0;
	args.flags = flags;

	return (zone_alloc_item(zones, &args, M_WAITOK));
}

/* See uma.h */
void
uma_zdestroy(uma_zone_t zone)
{

	/*
	 * Large slabs are expensive to reclaim, so don't bother doing
	 * unnecessary work if we're shutting down.
	 */
	if (booted == UMA_SHUTDOWN &&
	    zone->uz_fini == NULL &&
	    zone->uz_release == (uma_release)zone_release)
		return;
	uma_sx_slock(&uma_drain_lock);
	zone_free_item(zones, zone, NULL, SKIP_NONE);
	uma_sx_sunlock(&uma_drain_lock);
}

void
uma_zwait(uma_zone_t zone)
{
	void *item;

	item = uma_zalloc_arg(zone, NULL, M_WAITOK);
	uma_zfree(zone, item);
}

/* See uma.h */
void *
uma_zalloc_arg(uma_zone_t zone, void *udata, int flags)
{
	void *item;
	uma_cache_t cache;
	uma_bucket_t bucket;
	int lockfail;
	int cpu;

	/* This is the fast path allocation */
#ifdef UMA_DEBUG_ALLOC_1
	printf("Allocating one item from %s(%p)\n", zone->uz_name, zone);
#endif

#ifdef DEBUG_MEMGUARD
	if (memguard_cmp_zone(zone)) {
		item = memguard_alloc(zone->uz_size, flags);
		if (item != NULL) {
			if (zone->uz_init != NULL &&
			    zone->uz_init(item, zone->uz_size, flags) != 0)
				return (NULL);
			if (zone->uz_ctor != NULL &&
			    zone->uz_ctor(item, zone->uz_size, udata,
			    flags) != 0) {
			    	zone->uz_fini(item, zone->uz_size);
				return (NULL);
			}
			return (item);
		}
		/* This is unfortunate but should not be fatal. */
	}
#endif
	/*
	 * If possible, allocate from the per-CPU cache.  There are two
	 * requirements for safe access to the per-CPU cache: (1) the thread
	 * accessing the cache must not be preempted or yield during access,
	 * and (2) the thread must not migrate CPUs without switching which
	 * cache it accesses.  We rely on a critical section to prevent
	 * preemption and migration.  We release the critical section in
	 * order to acquire the zone mutex if we are unable to allocate from
	 * the current cache; when we re-acquire the critical section, we
	 * must detect and handle migration if it has occurred.
	 */
	cpu = curcpu;
	cache = &zone->uz_cpu[cpu];
	CACHE_LOCK(cache);
zalloc_start:
	bucket = cache->uc_allocbucket;
	if (bucket != NULL && bucket->ub_cnt > 0) {
		bucket->ub_cnt--;
		item = bucket->ub_bucket[bucket->ub_cnt];
#ifdef INVARIANTS
		bucket->ub_bucket[bucket->ub_cnt] = NULL;
#endif
		KASSERT(item != NULL, ("uma_zalloc: Bucket pointer mangled."));
		cache->uc_allocs++;
		CACHE_UNLOCK(cache);
		if (zone->uz_ctor != NULL &&
		    zone->uz_ctor(item, zone->uz_size, udata, flags) != 0) {
			atomic_add_long(&zone->uz_fails, 1);
			zone_free_item(zone, item, udata, SKIP_DTOR);
			return (NULL);
		}
		if (flags & M_ZERO)
			uma_zero_item(item, zone);
		return (item);
	}

	/*
	 * We have run out of items in our alloc bucket.
	 * See if we can switch with our free bucket.
	 */
	bucket = cache->uc_freebucket;
	if (bucket != NULL && bucket->ub_cnt > 0) {
#ifdef UMA_DEBUG_ALLOC
		printf("uma_zalloc: Swapping empty with alloc.\n");
#endif
		cache->uc_freebucket = cache->uc_allocbucket;
		cache->uc_allocbucket = bucket;
		goto zalloc_start;
	}

	/*
	 * Discard any empty allocation bucket while we hold no locks.
	 */
	bucket = cache->uc_allocbucket;
	cache->uc_allocbucket = NULL;
	CACHE_UNLOCK(cache);
	if (bucket != NULL)
		bucket_free(zone, bucket, udata);

	/* Short-circuit for zones without buckets and low memory. */
	if (zone->uz_count == 0 || bucketdisable)
		goto zalloc_item;

	/*
	 * Attempt to retrieve the item from the per-CPU cache has failed, so
	 * we must go back to the zone.  This requires the zone lock, so we
	 * must drop the critical section, then re-acquire it when we go back
	 * to the cache.  Since the critical section is released, we may be
	 * preempted or migrate.  As such, make sure not to maintain any
	 * thread-local state specific to the cache from prior to releasing
	 * the critical section.
	 */
	lockfail = 0;
	if (ZONE_TRYLOCK(zone) == 0) {
		/* Record contention to size the buckets. */
		ZONE_LOCK(zone);
		lockfail = 1;
	}
	cpu = curcpu;
	cache = &zone->uz_cpu[cpu];
	CACHE_LOCK(cache);

	/*
	 * Since we have locked the zone we may as well send back our stats.
	 */
	atomic_add_long(&zone->uz_allocs, cache->uc_allocs);
	atomic_add_long(&zone->uz_frees, cache->uc_frees);
	cache->uc_allocs = 0;
	cache->uc_frees = 0;

	/* See if we lost the race to fill the cache. */
	if (cache->uc_allocbucket != NULL) {
		ZONE_UNLOCK(zone);
		goto zalloc_start;
	}

	/*
	 * Check the zone's cache of buckets.
	 */
	if ((bucket = LIST_FIRST(&zone->uz_buckets)) != NULL) {
		KASSERT(bucket->ub_cnt != 0,
		    ("uma_zalloc_arg: Returning an empty bucket."));

		LIST_REMOVE(bucket, ub_link);
		cache->uc_allocbucket = bucket;
		ZONE_UNLOCK(zone);
		goto zalloc_start;
	}
	/* We are no longer associated with this CPU. */
	CACHE_UNLOCK(cache);

	/*
	 * We bump the uz count when the cache size is insufficient to
	 * handle the working set.
	 */
	if (lockfail && zone->uz_count < BUCKET_MAX)
		zone->uz_count++;
	ZONE_UNLOCK(zone);

	/*
	 * Now lets just fill a bucket and put it on the free list.  If that
	 * works we'll restart the allocation from the beginning and it
	 * will use the just filled bucket.
	 */
	bucket = zone_alloc_bucket(zone, udata, flags);
	if (bucket != NULL) {
		ZONE_LOCK(zone);
		cpu = curcpu;
		cache = &zone->uz_cpu[cpu];
		CACHE_LOCK(cache);
		/*
		 * See if we lost the race or were migrated.  Cache the
		 * initialized bucket to make this less likely or claim
		 * the memory directly.
		 */
		if (cache->uc_allocbucket == NULL)
			cache->uc_allocbucket = bucket;
		else
			LIST_INSERT_HEAD(&zone->uz_buckets, bucket, ub_link);
		ZONE_UNLOCK(zone);
		goto zalloc_start;
	}

	/*
	 * We may not be able to get a bucket so return an actual item.
	 */
#ifdef UMA_DEBUG
	printf("uma_zalloc_arg: Bucketzone returned NULL\n");
#endif

zalloc_item:
	item = zone_alloc_item(zone, udata, flags);

	return (item);
}

static uma_slab_t
keg_fetch_slab(uma_keg_t keg, uma_zone_t zone, int flags)
{
	uma_slab_t slab;
	u_int reserve;

	uma_mtx_assert(&keg->uk_lock, MA_OWNED);
	slab = NULL;
	reserve = 0;
	if ((flags & M_USE_RESERVE) == 0)
		reserve = keg->uk_reserve;

	for (;;) {
		/*
		 * Find a slab with some space.  Prefer slabs that are partially
		 * used over those that are totally full.  This helps to reduce
		 * fragmentation.
		 */
		if (keg->uk_free > reserve) {
			if (!LIST_EMPTY(&keg->uk_part_slab)) {
				slab = LIST_FIRST(&keg->uk_part_slab);
			} else {
				slab = LIST_FIRST(&keg->uk_free_slab);
				LIST_REMOVE(slab, us_link);
				LIST_INSERT_HEAD(&keg->uk_part_slab, slab,
				    us_link);
			}
			MPASS(slab->us_keg == keg);
			return (slab);
		}

		/*
		 * M_NOVM means don't ask at all!
		 */
		if (flags & M_NOVM)
			break;

		if (keg->uk_maxpages && keg->uk_pages >= keg->uk_maxpages) {
			keg->uk_flags |= UMA_ZFLAG_FULL;
			/*
			 * If this is not a multi-zone, set the FULL bit.
			 * Otherwise slab_multi() takes care of it.
			 */
			if ((zone->uz_flags & UMA_ZFLAG_MULTI) == 0) {
				zone->uz_flags |= UMA_ZFLAG_FULL;
				zone_log_warning(zone);
				zone_maxaction(zone);
			}
			if (flags & M_NOWAIT)
				break;
			zone->uz_sleeps++;
			uma_mtx_sleep(&keg->uk_cond, &keg->uk_lock);
			continue;
		}
		slab = keg_alloc_slab(keg, zone, flags);
		/*
		 * If we got a slab here it's safe to mark it partially used
		 * and return.  We assume that the caller is going to remove
		 * at least one item.
		 */
		if (slab) {
			MPASS(slab->us_keg == keg);
			LIST_INSERT_HEAD(&keg->uk_part_slab, slab, us_link);
			return (slab);
		}
		/*
		 * We might not have been able to get a slab but another cpu
		 * could have while we were unlocked.  Check again before we
		 * fail.
		 */
		flags |= M_NOVM;
	}
	return (slab);
}

static uma_slab_t
zone_fetch_slab(uma_zone_t zone, uma_keg_t keg, int flags)
{
	uma_slab_t slab;

	if (keg == NULL) {
		keg = zone_first_keg(zone);
		KEG_LOCK(keg);
	}

	for (;;) {
		slab = keg_fetch_slab(keg, zone, flags);
		if (slab)
			return (slab);
		if (flags & (M_NOWAIT | M_NOVM))
			break;
	}
	KEG_UNLOCK(keg);
	return (NULL);
}

static void *
slab_alloc_item(uma_keg_t keg, uma_slab_t slab)
{
	void *item;
	uint8_t freei;

	MPASS(keg == slab->us_keg);
	uma_mtx_assert(&keg->uk_lock, MA_OWNED);

	freei = BIT_FFS(SLAB_SETSIZE, &slab->us_free) - 1;
	BIT_CLR(SLAB_SETSIZE, freei, &slab->us_free);
	item = slab->us_data + (keg->uk_rsize * freei);
	slab->us_freecount--;
	keg->uk_free--;

	/* Move this slab to the full list */
	if (slab->us_freecount == 0) {
		LIST_REMOVE(slab, us_link);
		LIST_INSERT_HEAD(&keg->uk_full_slab, slab, us_link);
	}

	return (item);
}

static int
zone_import(uma_zone_t zone, void **bucket, int max, int flags)
{
	uma_slab_t slab;
	uma_keg_t keg;
	int i;

	slab = NULL;
	keg = NULL;
	/* Try to keep the buckets totally full */
	for (i = 0; i < max; ) {
		if ((slab = zone->uz_slab(zone, keg, flags)) == NULL)
			break;
		keg = slab->us_keg;
		while (slab->us_freecount && i < max) { 
			bucket[i++] = slab_alloc_item(keg, slab);
			if (keg->uk_free <= keg->uk_reserve)
				break;
		}
		/* Don't grab more than one slab at a time. */
		flags &= ~M_WAITOK;
		flags |= M_NOWAIT;
	}
	if (slab != NULL)
		KEG_UNLOCK(keg);

	return i;
}

static uma_bucket_t
zone_alloc_bucket(uma_zone_t zone, void *udata, int flags)
{
	uma_bucket_t bucket;
	int max;

	/* Don't wait for buckets, preserve caller's NOVM setting. */
	bucket = bucket_alloc(zone, udata, M_NOWAIT | (flags & M_NOVM));
	if (bucket == NULL)
		return (NULL);

	max = MIN(bucket->ub_entries, zone->uz_count);
	bucket->ub_cnt = zone->uz_import(zone->uz_arg, bucket->ub_bucket,
	    max, flags);

	/*
	 * Initialize the memory if necessary.
	 */
	if (bucket->ub_cnt != 0 && zone->uz_init != NULL) {
		int i;

		for (i = 0; i < bucket->ub_cnt; i++)
			if (zone->uz_init(bucket->ub_bucket[i], zone->uz_size,
			    flags) != 0)
				break;
		/*
		 * If we couldn't initialize the whole bucket, put the
		 * rest back onto the freelist.
		 */
		if (i != bucket->ub_cnt) {
			zone->uz_release(zone->uz_arg, &bucket->ub_bucket[i],
			    bucket->ub_cnt - i);
#ifdef INVARIANTS
			bzero(&bucket->ub_bucket[i],
			    sizeof(void *) * (bucket->ub_cnt - i));
#endif
			bucket->ub_cnt = i;
		}
	}

	if (bucket->ub_cnt == 0) {
		bucket_free(zone, bucket, udata);
		atomic_add_long(&zone->uz_fails, 1);
		return (NULL);
	}

	return (bucket);
}

/*
 * Allocates a single item from a zone.
 *
 * Arguments
 *	zone   The zone to alloc for.
 *	udata  The data to be passed to the constructor.
 *	flags  M_WAITOK, M_NOWAIT, M_ZERO.
 *
 * Returns
 *	NULL if there is no memory and M_NOWAIT is set
 *	An item if successful
 */

static void *
zone_alloc_item(uma_zone_t zone, void *udata, int flags)
{
	void *item;

	item = NULL;

#ifdef UMA_DEBUG_ALLOC
	printf("INTERNAL: Allocating one item from %s(%p)\n", zone->uz_name, zone);
#endif
	if (zone->uz_import(zone->uz_arg, &item, 1, flags) != 1)
		goto fail;
	atomic_add_long(&zone->uz_allocs, 1);

	/*
	 * We have to call both the zone's init (not the keg's init)
	 * and the zone's ctor.  This is because the item is going from
	 * a keg slab directly to the user, and the user is expecting it
	 * to be both zone-init'd as well as zone-ctor'd.
	 */
	if (zone->uz_init != NULL) {
		if (zone->uz_init(item, zone->uz_size, flags) != 0) {
			zone_free_item(zone, item, udata, SKIP_FINI);
			goto fail;
		}
	}
	if (zone->uz_ctor != NULL) {
		if (zone->uz_ctor(item, zone->uz_size, udata, flags) != 0) {
			zone_free_item(zone, item, udata, SKIP_DTOR);
			goto fail;
		}
	}
	if (flags & M_ZERO)
		uma_zero_item(item, zone);

	return (item);

fail:
	atomic_add_long(&zone->uz_fails, 1);
	return (NULL);
}

/* See uma.h */
void
uma_zfree_arg(uma_zone_t zone, void *item, void *udata)
{
	uma_cache_t cache;
	uma_bucket_t bucket;
	int lockfail;
	int cpu;

#ifdef UMA_DEBUG_ALLOC_1
	printf("Freeing item %p to %s(%p)\n", item, zone->uz_name, zone);
#endif
        /* uma_zfree(..., NULL) does nothing, to match free(9). */
        if (item == NULL)
                return;
#ifdef DEBUG_MEMGUARD
	if (is_memguard_addr(item)) {
		if (zone->uz_dtor != NULL)
			zone->uz_dtor(item, zone->uz_size, udata);
		if (zone->uz_fini != NULL)
			zone->uz_fini(item, zone->uz_size);
		memguard_free(item);
		return;
	}
#endif
	if (zone->uz_dtor != NULL)
		zone->uz_dtor(item, zone->uz_size, udata);

	/*
	 * The race here is acceptable.  If we miss it we'll just have to wait
	 * a little longer for the limits to be reset.
	 */
	if (zone->uz_flags & UMA_ZFLAG_FULL)
		goto zfree_item;

	/*
	 * If possible, free to the per-CPU cache.  There are two
	 * requirements for safe access to the per-CPU cache: (1) the thread
	 * accessing the cache must not be preempted or yield during access,
	 * and (2) the thread must not migrate CPUs without switching which
	 * cache it accesses.  We rely on a critical section to prevent
	 * preemption and migration.  We release the critical section in
	 * order to acquire the zone mutex if we are unable to free to the
	 * current cache; when we re-acquire the critical section, we must
	 * detect and handle migration if it has occurred.
	 */
zfree_restart:
	cpu = curcpu;
	cache = &zone->uz_cpu[cpu];
	CACHE_LOCK(cache);

zfree_start:
	/*
	 * Try to free into the allocbucket first to give LIFO ordering
	 * for cache-hot datastructures.  Spill over into the freebucket
	 * if necessary.  Alloc will swap them if one runs dry.
	 */
	bucket = cache->uc_allocbucket;
	if (bucket == NULL || bucket->ub_cnt >= bucket->ub_entries)
		bucket = cache->uc_freebucket;
	if (bucket != NULL && bucket->ub_cnt < bucket->ub_entries) {
		KASSERT(bucket->ub_bucket[bucket->ub_cnt] == NULL,
		    ("uma_zfree: Freeing to non free bucket index."));
		bucket->ub_bucket[bucket->ub_cnt] = item;
		bucket->ub_cnt++;
		cache->uc_frees++;
        	CACHE_UNLOCK(cache);
		return;
	}

	/*
	 * We must go back the zone, which requires acquiring the zone lock,
	 * which in turn means we must release and re-acquire the critical
	 * section.  Since the critical section is released, we may be
	 * preempted or migrate.  As such, make sure not to maintain any
	 * thread-local state specific to the cache from prior to releasing
	 * the critical section.
	 */
	CACHE_UNLOCK(cache);
	if (zone->uz_count == 0 || bucketdisable)
		goto zfree_item;

	lockfail = 0;
	if (ZONE_TRYLOCK(zone) == 0) {
		/* Record contention to size the buckets. */
		ZONE_LOCK(zone);
		lockfail = 1;
	}
	cpu = curcpu;
	cache = &zone->uz_cpu[cpu];
	CACHE_LOCK(cache);

	/*
	 * Since we have locked the zone we may as well send back our stats.
	 */
	atomic_add_long(&zone->uz_allocs, cache->uc_allocs);
	atomic_add_long(&zone->uz_frees, cache->uc_frees);
	cache->uc_allocs = 0;
	cache->uc_frees = 0;

	bucket = cache->uc_freebucket;
	if (bucket != NULL && bucket->ub_cnt < bucket->ub_entries) {
		ZONE_UNLOCK(zone);
		goto zfree_start;
	}
	cache->uc_freebucket = NULL;
	/* We are no longer associated with this CPU. */
	CACHE_UNLOCK(cache);

	/* Can we throw this on the zone full list? */
	if (bucket != NULL) {
#ifdef UMA_DEBUG_ALLOC
		printf("uma_zfree: Putting old bucket on the free list.\n");
#endif
		/* ub_cnt is pointing to the last free item */
		KASSERT(bucket->ub_cnt != 0,
		    ("uma_zfree: Attempting to insert an empty bucket onto the full list.\n"));
		LIST_INSERT_HEAD(&zone->uz_buckets, bucket, ub_link);
	}

	/*
	 * We bump the uz count when the cache size is insufficient to
	 * handle the working set.
	 */
	if (lockfail && zone->uz_count < BUCKET_MAX)
		zone->uz_count++;
	ZONE_UNLOCK(zone);

#ifdef UMA_DEBUG_ALLOC
	printf("uma_zfree: Allocating new free bucket.\n");
#endif
	bucket = bucket_alloc(zone, udata, M_NOWAIT);
	if (bucket) {
		cpu = curcpu;
		cache = &zone->uz_cpu[cpu];
		CACHE_LOCK(cache);
		if (cache->uc_freebucket == NULL) {
			cache->uc_freebucket = bucket;
			goto zfree_start;
		}
		/*
		 * We lost the race, start over.  We have to drop our
		 * critical section to free the bucket.
		 */
		CACHE_UNLOCK(cache);
		bucket_free(zone, bucket, udata);
		goto zfree_restart;
	}

	/*
	 * If nothing else caught this, we'll just do an internal free.
	 */
zfree_item:
	zone_free_item(zone, item, udata, SKIP_DTOR);

	return;
}

static void
slab_free_item(uma_keg_t keg, uma_slab_t slab, void *item)
{
	uint8_t freei;

	uma_mtx_assert(&keg->uk_lock, MA_OWNED);
	MPASS(keg == slab->us_keg);

	/* Do we need to remove from any lists? */
	if (slab->us_freecount+1 == keg->uk_ipers) {
		LIST_REMOVE(slab, us_link);
		LIST_INSERT_HEAD(&keg->uk_free_slab, slab, us_link);
	} else if (slab->us_freecount == 0) {
		LIST_REMOVE(slab, us_link);
		LIST_INSERT_HEAD(&keg->uk_part_slab, slab, us_link);
	}

	/* Slab management. */
	freei = ((uintptr_t)item - (uintptr_t)slab->us_data) / keg->uk_rsize;
	BIT_SET(SLAB_SETSIZE, freei, &slab->us_free);
	slab->us_freecount++;

	/* Keg statistics. */
	keg->uk_free++;
}

static void
zone_release(uma_zone_t zone, void **bucket, int cnt)
{
	void *item;
	uma_slab_t slab;
	uma_keg_t keg;
	uint8_t *mem;
	int clearfull;
	int i;

	clearfull = 0;
	keg = zone_first_keg(zone);
	KEG_LOCK(keg);
	for (i = 0; i < cnt; i++) {
		item = bucket[i];
		mem = (uint8_t *)((uintptr_t)item & (~UMA_SLAB_MASK));
		if (zone->uz_flags & UMA_ZONE_HASH) {
			slab = hash_sfind(&keg->uk_hash, mem);
		} else {
			mem += keg->uk_pgoff;
			slab = (uma_slab_t)mem;
		}
		slab_free_item(keg, slab, item);
		if (keg->uk_flags & UMA_ZFLAG_FULL) {
			if (keg->uk_pages < keg->uk_maxpages) {
				keg->uk_flags &= ~UMA_ZFLAG_FULL;
				clearfull = 1;
			}

			/* 
			 * We can handle one more allocation. Since we're
			 * clearing ZFLAG_FULL, wake up all procs blocked
			 * on pages. This should be uncommon, so keeping this
			 * simple for now (rather than adding count of blocked 
			 * threads etc).
			 */
			pthread_cond_broadcast(&keg->uk_cond);
		}
	}
	KEG_UNLOCK(keg);
	if (clearfull) {
		ZONE_LOCK(zone);
		zone->uz_flags &= ~UMA_ZFLAG_FULL;
		pthread_cond_broadcast(&zone->uz_cond);
		ZONE_UNLOCK(zone);
	}

}

/*
 * Frees a single item to any zone.
 *
 * Arguments:
 *	zone   The zone to free to
 *	item   The item we're freeing
 *	udata  User supplied data for the dtor
 *	skip   Skip dtors and finis
 */
static void
zone_free_item(uma_zone_t zone, void *item, void *udata, enum zfreeskip skip)
{

	if (skip < SKIP_DTOR && zone->uz_dtor)
		zone->uz_dtor(item, zone->uz_size, udata);

	if (skip < SKIP_FINI && zone->uz_fini)
		zone->uz_fini(item, zone->uz_size);

	atomic_add_long(&zone->uz_frees, 1);
	zone->uz_release(zone->uz_arg, &item, 1);
}

/* See uma.h */
int
uma_zone_set_max(uma_zone_t zone, int nitems)
{
	uma_keg_t keg;

	keg = zone_first_keg(zone);
	if (keg == NULL)
		return (0);
	KEG_LOCK(keg);
	keg->uk_maxpages = (nitems / keg->uk_ipers) * keg->uk_ppera;
	if ((int)keg->uk_maxpages * keg->uk_ipers < nitems)
		keg->uk_maxpages += keg->uk_ppera;
	nitems = (keg->uk_maxpages / keg->uk_ppera) * keg->uk_ipers;
	KEG_UNLOCK(keg);

	return (nitems);
}

/* See uma.h */
int
uma_zone_get_max(uma_zone_t zone)
{
	int nitems;
	uma_keg_t keg;

	keg = zone_first_keg(zone);
	if (keg == NULL)
		return (0);
	KEG_LOCK(keg);
	nitems = (keg->uk_maxpages / keg->uk_ppera) * keg->uk_ipers;
	KEG_UNLOCK(keg);

	return (nitems);
}

/* See uma.h */
void
uma_zone_set_warning(uma_zone_t zone, const char *warning)
{

	ZONE_LOCK(zone);
	zone->uz_warning = warning;
	ZONE_UNLOCK(zone);
}

/* See uma.h */
void
uma_zone_set_maxaction(uma_zone_t zone, uma_maxaction_t maxaction)
{

	ZONE_LOCK(zone);
	TASK_INIT(&zone->uz_maxaction, 0, (task_fn_t *)maxaction, zone);
	ZONE_UNLOCK(zone);
}

/* See uma.h */
int
uma_zone_get_cur(uma_zone_t zone)
{
	int64_t nitems;
	int i;

	ZONE_LOCK(zone);
	nitems = zone->uz_allocs - zone->uz_frees;
	CPU_FOREACH(i) {
		/*
		 * See the comment in sysctl_vm_zone_stats() regarding the
		 * safety of accessing the per-cpu caches. With the zone lock
		 * held, it is safe, but can potentially result in stale data.
		 */
		nitems += zone->uz_cpu[i].uc_allocs -
		    zone->uz_cpu[i].uc_frees;
	}
	ZONE_UNLOCK(zone);

	return (nitems < 0 ? 0 : nitems);
}

/* See uma.h */
void
uma_zone_set_init(uma_zone_t zone, uma_init uminit)
{
	uma_keg_t keg;

	keg = zone_first_keg(zone);
	KASSERT(keg != NULL, ("uma_zone_set_init: Invalid zone type"));
	KEG_LOCK(keg);
	KASSERT(keg->uk_pages == 0,
	    ("uma_zone_set_init on non-empty keg"));
	keg->uk_init = uminit;
	KEG_UNLOCK(keg);
}

/* See uma.h */
void
uma_zone_set_fini(uma_zone_t zone, uma_fini fini)
{
	uma_keg_t keg;

	keg = zone_first_keg(zone);
	KASSERT(keg != NULL, ("uma_zone_set_fini: Invalid zone type"));
	KEG_LOCK(keg);
	KASSERT(keg->uk_pages == 0,
	    ("uma_zone_set_fini on non-empty keg"));
	keg->uk_fini = fini;
	KEG_UNLOCK(keg);
}

/* See uma.h */
void
uma_zone_set_zinit(uma_zone_t zone, uma_init zinit)
{

	ZONE_LOCK(zone);
	KASSERT(zone_first_keg(zone)->uk_pages == 0,
	    ("uma_zone_set_zinit on non-empty keg"));
	zone->uz_init = zinit;
	ZONE_UNLOCK(zone);
}

/* See uma.h */
void
uma_zone_set_zfini(uma_zone_t zone, uma_fini zfini)
{

	ZONE_LOCK(zone);
	KASSERT(zone_first_keg(zone)->uk_pages == 0,
	    ("uma_zone_set_zfini on non-empty keg"));
	zone->uz_fini = zfini;
	ZONE_UNLOCK(zone);
}

/* See uma.h */
/* XXX uk_freef is not actually used with the zone locked */
void
uma_zone_set_freef(uma_zone_t zone, uma_free freef)
{
	uma_keg_t keg;

	keg = zone_first_keg(zone);
	KASSERT(keg != NULL, ("uma_zone_set_freef: Invalid zone type"));
	KEG_LOCK(keg);
	keg->uk_freef = freef;
	KEG_UNLOCK(keg);
}

/* See uma.h */
/* XXX uk_allocf is not actually used with the zone locked */
void
uma_zone_set_allocf(uma_zone_t zone, uma_alloc allocf)
{
	uma_keg_t keg;

	keg = zone_first_keg(zone);
	KEG_LOCK(keg);
	keg->uk_allocf = allocf;
	KEG_UNLOCK(keg);
}

/* See uma.h */
void
uma_zone_reserve(uma_zone_t zone, int items)
{
	uma_keg_t keg;

	keg = zone_first_keg(zone);
	if (keg == NULL)
		return;
	KEG_LOCK(keg);
	keg->uk_reserve = items;
	KEG_UNLOCK(keg);

	return;
}

/* See uma.h */
void
uma_prealloc(uma_zone_t zone, int items)
{
	int slabs;
	uma_slab_t slab;
	uma_keg_t keg;

	keg = zone_first_keg(zone);
	if (keg == NULL)
		return;
	KEG_LOCK(keg);
	slabs = items / keg->uk_ipers;
	if (slabs * keg->uk_ipers < items)
		slabs++;
	while (slabs > 0) {
		slab = keg_alloc_slab(keg, zone, M_WAITOK);
		if (slab == NULL)
			break;
		MPASS(slab->us_keg == keg);
		LIST_INSERT_HEAD(&keg->uk_free_slab, slab, us_link);
		slabs--;
	}
	KEG_UNLOCK(keg);
}

/* See uma.h */
static void
uma_reclaim_locked(bool kmem_danger)
{

#ifdef UMA_DEBUG
	printf("UMA: vm asked us to release pages!\n");
#endif
	uma_sx_assert(&uma_drain_lock, SA_XLOCKED);
	bucket_enable();
	zone_foreach(zone_drain);
	if (kmem_danger) {
		cache_drain_safe(NULL);
		zone_foreach(zone_drain);
	}
	/*
	 * Some slabs may have been freed but this zone will be visited early
	 * we visit again so that we can free pages that are empty once other
	 * zones are drained.  We have to do the same for buckets.
	 */
	zone_drain(slabzone);
	bucket_zone_drain();
}

void
uma_reclaim(void)
{

	uma_sx_xlock(&uma_drain_lock);
	uma_reclaim_locked(false);
	uma_sx_xunlock(&uma_drain_lock);
}

static int uma_reclaim_needed;

void
uma_reclaim_wakeup(void)
{

	pthread_mutex_lock(&uma_drain_mtx);
	uma_reclaim_needed = 1;
	pthread_cond_broadcast(&uma_drain_cond);
	pthread_mutex_unlock(&uma_drain_mtx);	
}

void
uma_reclaim_worker(void *arg)
{

	for (;;) {
		pthread_mutex_lock(&uma_drain_mtx);
		while (!uma_reclaim_needed)
			pthread_cond_wait(&uma_drain_cond, &uma_drain_mtx);
		uma_reclaim_needed = 0;
		pthread_mutex_unlock(&uma_drain_mtx);
		uma_sx_xlock(&uma_drain_lock);
		uma_reclaim_locked(true);
		uma_sx_xunlock(&uma_drain_lock);
	}
}

/* See uma.h */
int
uma_zone_exhausted(uma_zone_t zone)
{
	int full;

	ZONE_LOCK(zone);
	full = (zone->uz_flags & UMA_ZFLAG_FULL);
	ZONE_UNLOCK(zone);
	return (full);	
}

int
uma_zone_exhausted_nolock(uma_zone_t zone)
{
	return (zone->uz_flags & UMA_ZFLAG_FULL);
}

void uma_zone_drain(uma_zone_t zone)
{
    zone_drain(zone);
}

static void
uma_zero_item(void *item, uma_zone_t zone)
{

	bzero(item, zone->uz_size);
}

void
uma_print_stats(void)
{
	zone_foreach(uma_print_zone);
}

static void
slab_print(uma_slab_t slab)
{
	printf("slab: keg %p, data %p, freecount %d\n",
		slab->us_keg, slab->us_data, slab->us_freecount);
}

static void
cache_print(uma_cache_t cache)
{
	printf("alloc: %p(%d), free: %p(%d)\n",
		cache->uc_allocbucket,
		cache->uc_allocbucket?cache->uc_allocbucket->ub_cnt:0,
		cache->uc_freebucket,
		cache->uc_freebucket?cache->uc_freebucket->ub_cnt:0);
}

static void
uma_print_keg(uma_keg_t keg)
{
	uma_slab_t slab;

	printf("keg: %s(%p) size %d(%d) flags %#x ipers %d ppera %d "
	    "out %d free %d limit %d\n",
	    keg->uk_name, keg, keg->uk_size, keg->uk_rsize, keg->uk_flags,
	    keg->uk_ipers, keg->uk_ppera,
	    (keg->uk_pages / keg->uk_ppera) * keg->uk_ipers - keg->uk_free,
	    keg->uk_free, (keg->uk_maxpages / keg->uk_ppera) * keg->uk_ipers);
	printf("Part slabs:\n");
	LIST_FOREACH(slab, &keg->uk_part_slab, us_link)
		slab_print(slab);
	printf("Free slabs:\n");
	LIST_FOREACH(slab, &keg->uk_free_slab, us_link)
		slab_print(slab);
	printf("Full slabs:\n");
	LIST_FOREACH(slab, &keg->uk_full_slab, us_link)
		slab_print(slab);
}

void
uma_print_zone(uma_zone_t zone)
{
	uma_cache_t cache;
	uma_klink_t kl;
	int i;

	printf("zone: %s(%p) size %d flags %#x\n",
	    zone->uz_name, zone, zone->uz_size, zone->uz_flags);
	LIST_FOREACH(kl, &zone->uz_kegs, kl_link)
		uma_print_keg(kl->kl_keg);
	CPU_FOREACH(i) {
		cache = &zone->uz_cpu[i];
		printf("CPU %d Cache:\n", i);
		cache_print(cache);
	}
}

