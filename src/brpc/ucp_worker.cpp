// Copyright (c) 2022 Netease.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Xu Yifeng

#include "brpc/ucp_ctx.h"
#include "brpc/ucp_worker.h"

#include "bthread/unstable.h"
#include "butil/logging.h"
#include "butil/pctrie.h"

#include <gflags/gflags.h>
#include <endian.h>
#include <limits.h>
#include <poll.h>
#include <sched.h>
#include <stdlib.h>
#include <sys/param.h>
#include <sys/time.h>

#include <ios>
#include <utility>

#define AM_ID  0

/*
 忙等待能降低时延进而提高吞吐。但是当在连接上没有流量时，还是会使用整个CPU
 核心。
 为了避免资源浪费，使用一种自适应polling算法用来处理空闲的网络连接。
 每当有数据发送出去时，会让worker等待一段时间做busy polling，这段时间是可以
 配置的，当前设置为60(us)，这是希望在这段时间内会有数据返回回来，其中在
 busy polling期间如果过了固定数量的循环次数，则使用pthread_yield()来让出CPU，
 这个循环次数是可以配置的，当前缺省设置为0, 最后如果过了这段时间没有收到
 数据，则线程停止busy polling，进入内核等待。
*/

namespace brpc {

DEFINE_bool(brpc_ucp_worker_busy_poll, true, "Enable/disable busy poll");
DEFINE_int32(brpc_ucp_worker_poll_time, 60, "Polling duration in microseconds)");
DEFINE_int32(brpc_ucp_worker_poll_yield, 0, "Thread yields after accumulated so many polling loops");
DEFINE_bool(brpc_ucp_deliver_out_of_order, true, "Out of order delivery");
DEFINE_bool(brpc_ucp_always_flush, false, "flush when disconnecting");

/*
  接收端读写NVME SGL也许需要4字节对齐的地址，这通过在接收端总是提供4字节
  对齐的接收缓冲区，并在发送端对attachment进行4字节对齐来达到。
*/
#define NVME_DWORD_ALIGN  4
#define NVME_MAX_ALIGN    64

uint64_t g_supported_features;
uint64_t g_required_features;

struct Hello {
    uint64_t required_features;
    uint64_t supported_features;

    Hello() 
        : required_features(0)
        , supported_features(0)
    {
    }

    bool decode(butil::IOBuf &buf) {
        uint64_t data;

        if (buf.cutn(&data, sizeof(data)) != sizeof(data))
            return false;
        required_features = le64toh(data);
        if (buf.cutn(&data, sizeof(data)) != sizeof(data))
            return false;
        supported_features = le64toh(data);
        return true;
    }

    bool encode(butil::IOBuf &buf) const {
        uint64_t data;

        data = htole64(required_features);
        if (buf.append(&data, sizeof(data)))
            return false;
        data = htole64(supported_features);
        if (buf.append(&data, sizeof(data)))
            return false;
        return true;
    }
};

static void *alloc_trie_node(struct butil::pctrie *ptree)
{
    void *node = malloc(butil::pctrie_node_size());
    if (node)
        butil::pctrie_zone_init(node, butil::pctrie_node_size(), 0);
    return node;
}

static void free_trie_node(struct butil::pctrie *ptree, void *node)
{
    free(node);
}

class UcpWorker::PingHandler : public EventCallback {
public:
    UcpConnectionRef conn;
    UcpAmMsg *msg;
    PingHandler(const UcpConnectionRef& c, UcpAmMsg *m)
        : conn(c), msg(m) {}
    virtual void do_request(int fd_or_id) {
        butil::IOBuf buf(butil::IOBuf::Movable(msg->buf));
        conn->worker_->StartSend(UCP_CMD_PONG, conn.get(), &buf, 0);
        UcpAmMsg::Release(msg);
        msg = NULL;
    }
    virtual ~PingHandler() {
        CHECK(msg == NULL) << "msg memory was not freed";
    } 
};

class UcpWorker::PongHandler : public EventCallback {
public:
    UcpConnectionRef conn;
    UcpAmMsg *msg;
    PongHandler(const UcpConnectionRef& c, UcpAmMsg *m)
        : conn(c), msg(m) {}
    virtual void do_request(int fd_or_id) {
        conn->HandlePong(msg);
        msg = NULL;
    }
    virtual ~PongHandler() {
        CHECK(msg == NULL) << "msg memory was not freed";
    } 
};

class UcpWorker::HelloHandler : public EventCallback {
public:
    UcpConnectionRef conn;
    UcpAmMsg *msg;
    HelloHandler(const UcpConnectionRef& c, UcpAmMsg *m)
        : conn(c), msg(m) {}
    virtual void do_request(int fd_or_id) {
        Hello hello;
        UcpWorker *worker = conn->GetWorker();
        if (!hello.decode(msg->buf)) {
            LOG(ERROR) << "Can not decode hello packet from "
                       << conn->remote_side_str_;
            conn->ucp_code_.store(UCS_ERR_MESSAGE_TRUNCATED);
            worker->SetDataReady(conn);
        } else {
            butil::IOBuf buf;
            hello.required_features = g_required_features;
            hello.supported_features = g_supported_features;
            if (!hello.encode(buf)) {
                LOG(ERROR) << "Can not encode hello packet";
                conn->ucp_code_.store(UCS_ERR_NO_MEMORY);
                worker->SetDataReady(conn);
            } else {
                worker->StartSend(UCP_CMD_HELLO_REPLY, conn.get(), &buf, 0);
                conn->Open();
            }
        }
        UcpAmMsg::Release(msg);
        msg = NULL;
    }
    virtual ~HelloHandler() {
        CHECK(msg == NULL) << "msg memory was not freed";
    } 
};

class UcpWorker::HelloReplyHandler : public EventCallback {
public:
    UcpConnectionRef conn;
    UcpAmMsg *msg;
    HelloReplyHandler(const UcpConnectionRef& c, UcpAmMsg *m)
        : conn(c), msg(m) {}
    virtual void do_request(int fd_or_id) {
        Hello hello;
        UcpWorker *worker = conn->GetWorker();
        int success = 1;
        if (!hello.decode(msg->buf)) {
            LOG(ERROR) << "Can not decode hello packet from "
                       << conn->remote_side_str_;
            conn->ucp_code_.store(UCS_ERR_MESSAGE_TRUNCATED);
            worker->SetDataReady(conn);
            success = 0;
        } else {
            butil::IOBuf buf;
            uint64_t missing;
            missing = (hello.required_features & ~g_supported_features);
            if (missing) {
                LOG(ERROR) << "Peer " << conn->remote_side_str_
                           << " requires feature set "
                           << std::hex << "0x" << missing
                           << " is not supported by me";
                conn->ucp_code_.store(UCS_ERR_UNSUPPORTED);
                worker->SetDataReady(conn);
                success = 0;
            }
            missing = (g_required_features & ~hello.supported_features);
            if (missing) {
                LOG(ERROR) << "Requires feature set "
                           << std::hex << "0x" << missing
                           << "not supported by " << conn->remote_side_str_;
                conn->ucp_code_.store(UCS_ERR_UNSUPPORTED);
                worker->SetDataReady(conn);
                success = 0;
            }
        }
        if (success)
            conn->Open();
        UcpAmMsg::Release(msg);
        msg = NULL;
    }

    virtual ~HelloReplyHandler() {
        CHECK(msg == NULL) << "msg memory was not freed";
    }
};

UcpWorker::UcpWorker(UcpWorkerPool *pool, int id)
    : status_(UNINITIALIZED)
    , id_(id)
    , worker_tid_(INVALID_BTHREAD)
    , event_fd_(-1)
    , ucp_worker_(NULL)
    , free_data_(NULL)
    , free_data_count_(0)
    , worker_active_(0)
    , wakeup_flag_(0)
    , send_list_(NULL)
{
    TAILQ_INIT(&msg_q_);
    TAILQ_INIT(&recv_comp_q_);
    // This can not be changed dynamically
    out_of_order_ = FLAGS_brpc_ucp_deliver_out_of_order;
    // Because UCX ep does not allow us to set private data,
    // so we have to map ep to connection object, here we use
    // pctrie to perform lookup, it should be faster than
    // STL map which may be a red-black tree.
    butil::pctrie_init(&conn_map_);
    CHECK(pad_buf_.resize(NVME_MAX_ALIGN) == 0);
}

UcpWorker::~UcpWorker()
{
    Stop();
    Join();
}

void *UcpWorker::operator new(size_t size)
{
    size = roundup(size, BAIDU_CACHELINE_SIZE);
    return aligned_alloc(BAIDU_CACHELINE_SIZE, size);
}

void UcpWorker::operator delete(void *ptr)
{
    free(ptr);
}

void UcpWorker::AddConnection(UcpConnection *conn)
{
    if (!butil::pctrie_lookup(&conn_map_, (uintptr_t)conn->ep_)) {
        butil::pctrie_insert(&conn_map_, (uintptr_t *)&conn->ep_,
            alloc_trie_node);
        // increase reference count
        conn_aux_map_.insert({conn->ep_, conn->shared_from_this()});
    }
}

void UcpWorker::RemoveConnection(const ucp_ep_h ep)
{
    auto p = butil::pctrie_lookup(&conn_map_, (uintptr_t)ep);
    if (p != NULL) {
        UcpConnection *conn = ucs_container_of(p, UcpConnection, ep_);
        CHECK(ep == conn->ep_) << "inconsistent member field ep_";
        butil::pctrie_remove(&conn_map_, (uintptr_t)ep, free_trie_node);
        // decrease reference count
        conn_aux_map_.erase(ep);
    }
}

UcpConnectionRef UcpWorker::FindConnection(const ucp_ep_h ep)
{
    auto p = butil::pctrie_lookup(&conn_map_, (uintptr_t)ep);
    if (p != NULL) {
        UcpConnection *conn = ucs_container_of(p, UcpConnection, ep_);
        return conn->shared_from_this();
    }
    return UcpConnectionRef();
}

int UcpWorker::Initialize()
{
    return 0;
}

int UcpWorker::Start()
{
    ucp_worker_h w;
    ucs_status_t stat;
    int efd = -1;

    BAIDU_SCOPED_LOCK(mutex_);
    if (status_ == UNINITIALIZED) {
        if (Initialize() != 0) {
            LOG(FATAL) << "Fail to initialize UcpWorker";
            return -1;
        }
        status_ = READY;
    }
    if (status_ != READY) {
        LOG(FATAL) << "UcpWorker hasn't stopped yet: status=" << status();
        return -1;
    }

    // Create ucp worker
    char name[64];
    snprintf(name, sizeof(name), "io-%d", id_);
    if (create_ucp_worker(get_or_create_ucp_ctx()->context(), &w, 0, name,
         &efd)) {
        return -1;
    }

    event_fd_ = efd;
    ucp_worker_ = w;

    // Need to arm ucp_worker before being polled
    stat = ucp_worker_arm(w);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucx_worker_arm failed ("
                   << ucs_status_string(stat) << ")";
        goto destroy;
    }

    if (!SetAmCallback()) {
        goto destroy;
    }

    if (pthread_create(&worker_tid_, NULL, RunWorker, this) != 0) {
        LOG(FATAL) << "Fail to start worker thread";
        goto destroy;
    }

    status_ = RUNNING;
    return 0;

destroy:
    ucp_worker_destroy(ucp_worker_);
    ucp_worker_ = NULL;
    event_fd_ = -1;
    return -1;
}

void UcpWorker::Stop()
{
    BAIDU_SCOPED_LOCK(mutex_);
    if (status_ != RUNNING) {
        return;
    }
    status_ = STOPPING;

    if (ucp_worker_) {
        ucs_status_t stat = ucp_worker_signal(ucp_worker_);
        if (stat != UCS_OK) {
            LOG(ERROR) << "ucp_worker_signal error ("
                       << ucs_status_string(stat) << ")";
        }
    }
}

void UcpWorker::Join()
{
    std::unique_lock<bthread::Mutex> mu(mutex_);
    if (status_ != STOPPING) {
        return;
    }
    mu.unlock();

    pthread_join(worker_tid_, NULL);

    ucp_worker_destroy(ucp_worker_);
    ucp_worker_ = NULL;

    {
        BAIDU_SCOPED_LOCK(mutex_);
        status_ = READY;
    }
}

void UcpWorker::Wakeup()
{
    if (wakeup_flag_.load(std::memory_order_relaxed))
        return;
    wakeup_flag_.exchange(1, std::memory_order_relaxed);
    ucs_status_t stat = ucp_worker_signal(ucp_worker_);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucp_worker_signal error ("
                   << ucs_status_string(stat) << ")";
    }
}

void UcpWorker::MaybeWakeup()
{
    if (FLAGS_brpc_ucp_worker_busy_poll)
        return;
    if (pthread_self() != worker_tid_) {
        Wakeup();
    }
}

void UcpWorker::DispatchExternalEvent(EventCallback *e)
{
    mutex_.lock();
    external_events_.push_back(e);
    mutex_.unlock();
    MaybeWakeup();
}

void UcpWorker::DispatchExternalEventLocked(EventCallback *e)
{
    external_events_.push_back(e);
    MaybeWakeup();
}

void UcpWorker::InvokeExternalEvents()
{
    // assert(mutex_.is_locked_by_me());
    while (!external_events_.empty()) {
        auto e = external_events_.front();
        external_events_.pop_front();
        mutex_.unlock();
        if (e) {
            e->do_request(0);
            delete e;
        }
        // e is destructed here
        mutex_.lock();
    }
    // assert(mutex_.is_locked_by_me());
}

bool UcpWorker::SetAmCallback()
{
    ucp_am_handler_param_t  param;
    ucs_status_t            status;

    memset(&param, 0, sizeof(param));
    param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                       UCP_AM_HANDLER_PARAM_FIELD_CB |
                       UCP_AM_HANDLER_PARAM_FIELD_ARG|
                       UCP_AM_HANDLER_PARAM_FIELD_FLAGS;
    param.id         = AM_ID;
    param.cb         = AmCallback;
    param.arg        = this;
    param.flags      = UCP_AM_FLAG_WHOLE_MSG | UCP_AM_FLAG_PERSISTENT_DATA;
    status           = ucp_worker_set_am_recv_handler(ucp_worker_, &param);
    if (status != UCS_OK) {
        LOG(ERROR) << "Can not set am handler";
        return false;
    }
    return true;
}

void* UcpWorker::RunWorker(void *arg)
{
    UcpWorker *w = (UcpWorker *)arg;
    w->DoRunWorker();
    return 0;
}

void UcpWorker::DoRunWorker()
{
    struct pollfd poll_info;
    struct timeval tv_start{0,0}, tv_end{0,0}, tv_int{0,0}, tv_now{0,0};
    int count = 0;

    pthread_setname_np(pthread_self(), "ucp_worker");
    tv_int.tv_sec = 0;
    tv_int.tv_usec = FLAGS_brpc_ucp_worker_poll_time;
    while (status_ != STOPPING) {
        if (!FLAGS_brpc_ucp_worker_busy_poll) {
            if (!wakeup_flag_.exchange(0, std::memory_order_relaxed)) {
                poll_info.fd = event_fd_;
                poll_info.events = POLLIN;
                poll(&poll_info, 1, 1000);
            }
            worker_active_.store(1, std::memory_order_relaxed);
            gettimeofday(&tv_start, NULL);
            timeradd(&tv_start, &tv_int, &tv_end);
            count = 0;
        }
again:
        mutex_.lock();
        while (ucp_worker_progress(ucp_worker_))
            ;
        DispatchAmMsgQ();
        DispatchRecvCompQ();
        KeepSendRequest();
        DispatchDataReady();
        CheckExitingEp();
        RecycleWorkerData();
        InvokeExternalEvents();
        ucs_status_t stat = !FLAGS_brpc_ucp_worker_busy_poll ?
            ucp_worker_arm(ucp_worker_) : UCS_OK;
        mutex_.unlock();
        if (stat == UCS_ERR_BUSY) /* some events are arrived already */
            goto again;
        if (!FLAGS_brpc_ucp_worker_busy_poll) {
            count++;
            gettimeofday(&tv_now, NULL);
            if (timercmp(&tv_now, &tv_end, <)) {
                if (BAIDU_UNLIKELY(FLAGS_brpc_ucp_worker_poll_yield &&
                    count >= FLAGS_brpc_ucp_worker_poll_yield)) {
                    sched_yield();
                }
                goto again;
            }

            worker_active_.store(-1, std::memory_order_relaxed);
        }
        CHECK(stat == UCS_OK) << "ucx_worker_arm failed ("
            << ucs_status_string(stat) << ")";
    }
    worker_active_.store(0, std::memory_order_relaxed);
}

static inline void MsgHeaderIn(MsgHeader *host, const MsgHeader *net)
{
    host->ver = le16toh(net->ver);
    host->cmd = le16toh(net->cmd);
    host->pad = le16toh(net->pad);
    host->reserve = le16toh(net->reserve);
    host->sn = le64toh(net->sn);
}

static inline void MsgHeaderOut(const MsgHeader *host, MsgHeader *net)
{
    net->ver = htole16(host->ver);
    net->cmd = htole16(host->cmd);
    net->pad = htole16(host->pad);
    net->reserve = htole16(host->reserve);
    net->sn = htole64(host->sn);
}

ucs_status_t UcpWorker::AmCallback(void *arg,
     const void *header, size_t header_length, void *data, size_t length,
     const ucp_am_recv_param_t *param)
{
    UcpWorker *w = (UcpWorker *)arg;
    return w->DoAmCallback(header, header_length, data, length, param);
}

ucs_status_t UcpWorker::DoAmCallback(
     const void *header, size_t header_length, void *data, size_t length,
     const ucp_am_recv_param_t *param)
{
    // assert(mutex_.is_locked_by_me());

    if (BAIDU_UNLIKELY(!(param->recv_attr & UCP_AM_RECV_ATTR_FIELD_REPLY_EP))) {
        LOG(ERROR) << "UCP_AM_RECV_ATTR_FIELD_REPLY_EP not set";
        return UCS_OK;
    }

    UcpConnectionRef conn = FindConnection(param->reply_ep);
    if (BAIDU_UNLIKELY(!conn)) {
        LOG(ERROR) << "Can not find ep in conn_map_";
        return UCS_OK;
    }

    MsgHeader *mh = (MsgHeader *)header;
    if (BAIDU_UNLIKELY(mh == NULL)) {
        conn->ucp_code_.store(UCS_ERR_MESSAGE_TRUNCATED);
        SetDataReadyLocked(conn);
        LOG(ERROR) << "header is NULL";
        return UCS_OK;
    } 
    if (BAIDU_UNLIKELY(header_length < sizeof(*mh))) {
        conn->ucp_code_.store(UCS_ERR_MESSAGE_TRUNCATED);
        SetDataReadyLocked(conn);
        LOG(ERROR) << "header_length is less than " << sizeof(*mh);
        return UCS_OK;
    }
    if (BAIDU_UNLIKELY(le16toh(mh->ver) != UCP_VER_0)) {
        conn->ucp_code_.store(UCS_ERR_INVALID_PARAM);
        SetDataReadyLocked(conn);
        LOG(ERROR) << "Unknown command verion: " << le16toh(mh->ver);
        return UCS_OK;
    }
    if (BAIDU_UNLIKELY(le16toh(mh->cmd) > UCP_CMD_MAX)) {
        conn->ucp_code_.store(UCS_ERR_INVALID_PARAM);
        SetDataReadyLocked(conn);
        LOG(ERROR) << "Unknown command id: " << le16toh(mh->cmd);
        return UCS_OK;
    }
    if (BAIDU_UNLIKELY(!(param->recv_attr & (UCP_AM_RECV_ATTR_FLAG_DATA |
                         UCP_AM_RECV_ATTR_FLAG_RNDV)))) {
        conn->ucp_code_.store(UCS_ERR_MESSAGE_TRUNCATED);
        SetDataReadyLocked(conn);
        LOG(ERROR) << "Neither UCP_AM_RECV_ATTR_FIELD_DATA nor UCP_AM_RECV_ATTR_FLAG_RNDV is set";
        return UCS_OK;
    }

    if (BAIDU_UNLIKELY(conn->state_ == UcpConnection::STATE_CLOSED)) {
        LOG(ERROR) << "Received am from" << conn->remote_side_str_
                   << " ,but connection was already closed";
        return UCS_OK;
    }

    UcpAmMsg *msg = UcpAmMsg::Allocate();
    if (BAIDU_UNLIKELY(msg == NULL)) {
        LOG(FATAL) << "can not allocate UcpMsg";
        conn->ucp_code_.store(UCS_ERR_NO_MEMORY);
        SetDataReadyLocked(conn);
        return UCS_OK;
    }
    MsgHeaderIn(&msg->header, mh);
    msg->data = data;
    msg->length = length;
    if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) {
        msg->set_flag(AMF_RNDV);
    }
    msg->set_flag(AMF_MSG_Q);
    msg->conn = std::move(conn);
    TAILQ_INSERT_TAIL(&msg_q_, msg, link);

    return UCS_INPROGRESS;
}

void UcpWorker::ReleaseWorkerData(void *data, void *arg)
{
    UcpWorker *w = (UcpWorker *)arg;
    void **ptr = (void **)data;

    do {
        *ptr = w->free_data_.load(butil::memory_order_relaxed);
    } while (!w->free_data_.compare_exchange_strong(*ptr, data,
             butil::memory_order_relaxed));
    if (++w->free_data_count_ > 100) {
        w->free_data_count_ = 0;
        w->MaybeWakeup();
    }
}

void UcpWorker::RecycleWorkerData()
{
    if (!free_data_.load(butil::memory_order_relaxed))
        return;
    free_data_count_ = 0;
    void *p = free_data_.exchange(NULL, butil::memory_order_release);
    while (p) {
        void *next = *(void **)p;
        ucp_am_data_release(ucp_worker_, p);
        p = next;
    }
}

static inline void remove_from_msg_q(UcpAmList *list, UcpAmMsg *msg)
{
#ifdef UCP_WORKER_Q_DEBUG
    CHECK(msg->has_flag(AMF_MSG_Q)) << "not on msg queue";
#endif
    TAILQ_REMOVE(list, msg, link);
    msg->clear_flag(AMF_MSG_Q);
}

static inline void insert_into_comp_q(UcpAmList *list, UcpAmMsg *msg)
{
#ifdef UCP_WORKER_Q_DEBUG
    CHECK(!msg->has_flag(AMF_COMP_Q)) << "already on complete queue";
#endif
    TAILQ_INSERT_TAIL(list, msg, comp_link);
    msg->set_flag(AMF_COMP_Q);
}

static inline void remove_from_comp_q(UcpAmList *list, UcpAmMsg *msg)
{
#ifdef UCP_WORKER_Q_DEBUG
    CHECK(msg->has_flag(AMF_COMP_Q)) << "not on complete queue";
#endif
    TAILQ_REMOVE(list, msg, comp_link);
    msg->clear_flag(AMF_COMP_Q);
}

static inline void remove_from_recv_q(UcpAmList *list, UcpAmMsg *msg)
{
#ifdef UCP_WORKER_Q_DEBUG
    CHECK(msg->has_flag(AMF_RECV_Q)) << "not on receive queue";
#endif
    TAILQ_REMOVE(list, msg, link);
    msg->clear_flag(AMF_RECV_Q);
}

void UcpWorker::DispatchAmMsgQ()
{
    UcpAmMsg *msg, *elem;
    ucp_request_param_t param;
    void *buf;
    size_t len; 
    int rc;
    UcpAmList tmp_list;

    // assert(mutex_.is_locked_by_me());
    if (TAILQ_EMPTY(&msg_q_)) {
        return;
    }

    TAILQ_INIT(&tmp_list);
    TAILQ_CONCAT(&tmp_list, &msg_q_, link);
    // msg_q_ is inited by TAILQ_CONCAT

    while (!TAILQ_EMPTY(&tmp_list)) {
        msg = TAILQ_FIRST(&tmp_list);
        remove_from_msg_q(&tmp_list, msg);
        UcpConnectionRef conn = msg->conn;
        if (!msg->has_flag(AMF_RNDV)) {
            if (msg->length >= sizeof(void *)) {
                /* For buffer larger than size of pointer, we can
                 * chain them together with a single link list,
                 * the data buffer is not copied.
                 */
                msg->buf.append_user_data(msg->data, msg->length,
                    ReleaseWorkerData, this);
            } else {
                /* Too small to store a pointer, copy and release
                 * the data buffer immediately.
                 */
                if (msg->data) {
                    if (msg->length) {
                        msg->buf.append(msg->data, msg->length);
                    }
                    ucp_am_data_release(ucp_worker_, msg->data);
                }
            }
            msg->req = nullptr;
            goto request_done;
        }
 
        rc = msg->buf.prepare_buffer(msg->length, INT_MAX,
                &msg->iov, &msg->nvec);
        if (rc == -1) {
            LOG(FATAL) << "prepare failure";
        }

        buf = (msg->nvec == 1) ? msg->iov[0].buffer : (void *)&msg->iov[0];
        len = (msg->nvec == 1) ? msg->iov[0].length : msg->nvec;
        param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK  |
                             UCP_OP_ATTR_FIELD_DATATYPE  |
                             UCP_OP_ATTR_FIELD_USER_DATA;
        param.datatype = (msg->nvec == 1) ? ucp_dt_make_contig(1) :
                         ucp_dt_make_iov();
        param.cb.recv_am = AmRecvCallback;
        param.user_data = msg;
        msg->req = ucp_am_recv_data_nbx(ucp_worker_,
                        msg->desc, buf, len, &param);
request_done:
#ifdef UCP_WORKER_Q_DEBUG
        CHECK(!msg->has_flag(AMF_RECV_Q)) << "already on receive queue";
#endif
        TAILQ_FOREACH_REVERSE(elem, &conn->recv_q_, UcpAmList, link) {
            if (msg->header.sn > elem->header.sn) {
                TAILQ_INSERT_AFTER(&conn->recv_q_, elem, msg, link);
                msg->set_flag(AMF_RECV_Q);
                break;
            }
        }

        if (!msg->has_flag(AMF_RECV_Q)) {
            TAILQ_INSERT_HEAD(&conn->recv_q_, msg, link);
            msg->set_flag(AMF_RECV_Q);
        }

        if (msg->req == nullptr) {
            msg->code = UCS_OK;
            insert_into_comp_q(&recv_comp_q_, msg);
        } else if (UCS_PTR_IS_ERR(msg->req)) {
            ucs_status_t st = UCS_PTR_STATUS(msg->req);
            msg->code = st;
            insert_into_comp_q(&recv_comp_q_, msg);
            LOG(INFO) << "Failed to call ucp_am_recv_nbx (" << ucs_status_string(st) << ")";
        }
    }
}

void UcpWorker::AmRecvCallback(void *request, ucs_status_t status,
    size_t length, void *user_data)
{
    UcpAmMsg *msg = (UcpAmMsg *)user_data;
    msg->conn->worker_->DoAmRecvCallback(msg, status, length);
}

void UcpWorker::DoAmRecvCallback(UcpAmMsg *msg, ucs_status_t status,
    size_t length)
{
    // assert(mutex_.is_locked_by_me());
    ucp_request_free(msg->req);
    msg->code = status;
    if (BAIDU_UNLIKELY(msg->length != length)) {
        LOG(WARNING) << "strange, length not equal, expect "
                     << msg->length << ", actual " << length;
    }
    insert_into_comp_q(&recv_comp_q_, msg);
    if (BAIDU_UNLIKELY(status != UCS_OK)) {
        LOG(ERROR ) << "Receive with error ("
                    << ucs_status_string(status) << ")"
                    << " from " << msg->conn->remote_side_str_;
    }
}

void UcpWorker::DispatchRecvCompQ()
{
    UcpAmList tmp_list;
    // assert(mutex_.is_locked_by_me());

    if (TAILQ_EMPTY(&recv_comp_q_)) {
        return;
    }
    TAILQ_INIT(&tmp_list);
    TAILQ_CONCAT(&tmp_list, &recv_comp_q_, comp_link);
    // recv_comp_q_ is inited by TAILQ_CONCAT

    while (!TAILQ_EMPTY(&tmp_list)) {
        UcpAmMsg *msg = TAILQ_FIRST(&tmp_list);
        UcpConnectionRef conn = msg->conn;

#ifdef UCP_WORKER_Q_DEBUG
        CHECK(!msg->has_flag(AMF_FINISH)) << "finish should not set";
#endif
        msg->set_flag(AMF_FINISH);

        remove_from_comp_q(&tmp_list, msg);

        if (msg->code == UCS_OK) {
            if (msg->has_flag(AMF_RNDV))
                msg->buf.append_from_buffer(msg->length);
            /* Remove pad bytes */
            if (msg->header.pad)
                msg->buf.pop_front(msg->header.pad);
        }

        bool avail = false;
        if (out_of_order_) {
            remove_from_recv_q(&conn->recv_q_, msg);
            SaveInputMessage(conn, msg); 
            avail = true;
        } else {
            avail = CheckConnRecvQ(conn);
        }

        if (avail) {
            SetDataReadyLocked(conn);
        }
    }
}

bool UcpWorker::CheckConnRecvQ(const UcpConnectionRef& conn)
{
    bool avail = false;

    while (!TAILQ_EMPTY(&conn->recv_q_)) {
        UcpAmMsg *msg = TAILQ_FIRST(&conn->recv_q_);
        if (!msg->has_flag(AMF_FINISH))
            break;
        if (conn->state_ == UcpConnection::STATE_CLOSED) {
            TAILQ_REMOVE(&conn->recv_q_, msg, link);
            UcpAmMsg::Release(msg);
            continue;
        }
        if (msg->header.sn != conn->expect_sn_)
            break;
        avail = true;
        CHECK(!msg->has_flag(AMF_COMP_Q)) << "still on complete queue";

        remove_from_recv_q(&conn->recv_q_, msg);

        SaveInputMessage(conn, msg);
        conn->expect_sn_++;
    }
    return avail;
}

void UcpWorker::SaveInputMessage(const UcpConnectionRef &conn, UcpAmMsg *msg)
{
    UcpAmMsg *ptr;

    switch(msg->header.cmd) {
    case UCP_CMD_BRPC: 
        do {
            ptr = conn->ready_list_.load(butil::memory_order_relaxed);
            msg->link.tqe_next = ptr;
        } while (!conn->ready_list_.compare_exchange_strong(ptr, msg,
                    butil::memory_order_release));
        break;
    case UCP_CMD_PING:
        HandlePing(conn, msg);
        break;
    case UCP_CMD_PONG:
        HandlePong(conn, msg);
        break;
    case UCP_CMD_HELLO:
        HandleHello(conn, msg);
        break;
    case UCP_CMD_HELLO_REPLY:
        HandleHelloReply(conn, msg);
        break;
    default:
        UcpAmMsg::Release(msg);
        LOG(ERROR) << "From " << conn->remote_side_str_ << " ,unknown command " << msg->header.cmd;
    }
}

void UcpWorker::MergeInputMessage(UcpConnection *conn)
{
    UcpAmMsg *msg = NULL, *prev = NULL, *next = NULL;

    // assert(mutex_.is_locked_by_me());
    if (!conn->ready_list_.load(butil::memory_order_relaxed))
        return;

    msg = conn->ready_list_.exchange(NULL, butil::memory_order_acquire);
    // Reverse the list to dispatch it in order
    prev = NULL;
    while (msg) {
        next = msg->link.tqe_next;
        msg->link.tqe_next = prev;
        prev = msg;
        msg = next;
    }
    for (msg = prev; msg; msg = next) {
        next = msg->link.tqe_next;
        // If receiving error already occurred, skip any left messages
        if (!conn->ucp_recv_code_.load(butil::memory_order_relaxed)) {
            if (msg->code == UCS_OK) {
                conn->in_buf_.append(butil::IOBuf::Movable(msg->buf));
            } else {
                conn->ucp_recv_code_.store(msg->code);
                conn->ucp_code_.store(msg->code);
            }
        }
        UcpAmMsg::Release(msg);
    }
}

int UcpWorker::Accept(UcpConnection *conn, ucp_conn_request_h req)
{
    BAIDU_SCOPED_LOCK(mutex_);
    int ret = CreateUcpEp(conn, req);
    if (ret == 0) {
        conn->state_ = UcpConnection::STATE_WAIT_HELLO;
        AddConnection(conn);
        MaybeWakeup();
    }
    return ret;
}

int UcpWorker::Connect(UcpConnection *conn, const butil::EndPoint &peer)
{
    BAIDU_SCOPED_LOCK(mutex_);
    int ret = create_ucp_ep(ucp_worker_, peer, ErrorCallback, this, &conn->ep_);
    if (ret == 0) {
        conn->state_ = UcpConnection::STATE_HELLO;
        AddConnection(conn);
        conn->remote_side_ = peer;
        auto str = butil::endpoint2str(peer);
        conn->remote_side_str_ = str.c_str();
        SendHello(conn);
        MaybeWakeup();
    }
    return ret;
}

void UcpWorker::ErrorCallback(void *arg, ucp_ep_h ep, ucs_status_t status)
{
    UcpWorker *w = static_cast<UcpWorker *>(arg);
    // assert(w->mutex_.is_locked());
    UcpConnectionRef conn = w->FindConnection(ep);
    if (!conn) {
        LOG(ERROR) << "Can not find ep in ErrorCallback, may be moved";
        return;
    }
    butil::EndPointStr str = endpoint2str(conn->remote_side_);
    LOG(ERROR) << "Error occurred on remote side " << str.c_str()
               << " (" << ucs_status_string(status) << ")";
    conn->ucp_code_.store(status ? status : UCS_ERR_IO_ERROR);
    w->SetDataReadyLocked(conn);
}

void UcpWorker::CancelRequests(UcpConnection *conn)
{
    UcpAmMsg *msg, *next = NULL;

    for (msg = TAILQ_FIRST(&conn->recv_q_); msg; msg = next) {
        next = TAILQ_NEXT(msg, link);
        if (!msg->has_flag(AMF_FINISH)) {
            ucp_request_cancel(ucp_worker_, msg->req);
        } else {
            TAILQ_REMOVE(&conn->recv_q_, msg, link);
            UcpAmMsg::Release(msg); 
        }
    }
}

// Release the connection object indexed by ep, and gracefully disconnect it
void UcpWorker::Release(UcpConnection *conn)
{
    ucs_status_ptr_t request;
    ucp_ep_h ep = conn->ep_;

    // Lock the worker
    BAIDU_SCOPED_LOCK(mutex_);

    conn->state_ = UcpConnection::STATE_CLOSED;

    RemoveConnection(conn->ep_);

    CancelRequests(conn);

    int mode = UCP_EP_CLOSE_MODE_FORCE;
    request = ucp_ep_close_nb(ep, mode);
    if (request == NULL) {
        conn->ep_ = NULL;
        LOG(INFO) << "ucp_ep_close_nb(" << conn->remote_side_str_
                  << ") success";
        return;
    } else if (UCS_PTR_IS_ERR(request)) {
        conn->ep_ = NULL;
        LOG(ERROR) << "ucp_ep_close_nb(" << conn->remote_side_str_
                   << ") failed with status ("
                   << ucs_status_string(UCS_PTR_STATUS(request)) << ")";
        return;
    }
    // Remember it, we will check it later
    ExitingEp e;
    e.ep = ep;
    e.req = request;
    e.conn = conn->shared_from_this();
    exiting_ep_.push_back(e);
    MaybeWakeup();
}

void UcpWorker::CheckExitingEp()
{
    // assert(mutex_.is_locked_by_me());
    exiting_ep_list_t::iterator it, next;
    for (it = exiting_ep_.begin(); it != exiting_ep_.end(); it = next) {
        CHECK(it->magic == 1234) << "magic bad";
        next = it;
        next++;
        ucs_status_t status;
        ucs_status_ptr_t request = it->req;
        UcpConnectionRef conn = it->conn;
        status = ucp_request_check_status(request);
        if (status == UCS_INPROGRESS)
            continue;
        LOG(INFO) << "ucp_ep_close_nb(" << conn->remote_side_str_
                  << ") success later";
        ucp_request_release(request);
        conn->ep_ = NULL;
        next = exiting_ep_.erase(it);
    }
}

int UcpWorker::CreateUcpEp(UcpConnection *conn, ucp_conn_request_h req)
{
    int rc = create_ucp_ep(ucp_worker_, req, ErrorCallback, this, &conn->ep_);
    if (rc == 0) {
        ucp_ep_attr_t attr;
        attr.field_mask = UCP_EP_ATTR_FIELD_REMOTE_SOCKADDR;
        ucs_status_t st = ucp_ep_query(conn->ep_, &attr);
        if (st == UCS_OK) {
            conn->remote_side_ =
                butil::EndPoint(*(sockaddr_in*)&attr.remote_sockaddr,
                                butil::EndPoint::UCP);
            auto str = butil::endpoint2str(conn->remote_side_);
            conn->remote_side_str_ = str.c_str();
        }
    }
    return rc;
}

ssize_t UcpWorker::StartRecv(UcpConnection *conn)
{
    SetDataReady(conn);  
    return 0;
}

void UcpWorker::DispatchDataReady()
{
    // assert(w->mutex_.is_locked());
    std::queue<UcpConnectionRef> tmp;

    if (data_ready_.empty()) {
        return;
    }

    tmp.swap(data_ready_);
    mutex_.unlock();
    while (!tmp.empty()) {
        auto conn = tmp.front();
        tmp.pop();
        conn->DataReady();
    }
    mutex_.lock();
}

void UcpWorker::SetDataReady(UcpConnection* conn)
{
    BAIDU_SCOPED_LOCK(mutex_);

    SetDataReadyLocked(conn);
}

void UcpWorker::SetDataReadyLocked(UcpConnection *conn)
{
    if (!conn->data_ready_flag_) {
        data_ready_.push(conn->shared_from_this());
        conn->data_ready_flag_ = true;
        MaybeWakeup();
    }
}

void UcpWorker::AmSendCb(void *request, ucs_status_t status, void *user_data)
{
    // assert(w->mutex_.is_locked());
    UcpAmSendInfo *msg = (UcpAmSendInfo *)user_data;
    UcpConnectionRef conn = msg->conn;

    TAILQ_REMOVE(&conn->send_q_, msg, link);
    if (status != UCS_OK) {
        conn->ucp_code_.store(status);
        conn->worker_->SetDataReadyLocked(msg->conn);
    }
    
    ucp_request_free(msg->req);
    UcpAmSendInfo::Release(msg);
}

void UcpWorker::SetupSendRequestParam(const UcpAmSendInfo *msg,
    ucp_request_param_t *param, void **buf, size_t *len)
{
    memset(param, 0, sizeof(*param));
    param->op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA|
                          UCP_OP_ATTR_FIELD_FLAGS;
    param->flags = UCP_AM_SEND_FLAG_REPLY;
    param->datatype = (msg->nvec == 1) ? ucp_dt_make_contig(1) :
                     ucp_dt_make_iov();
    param->user_data = (void *)(msg);
    param->cb.send = AmSendCb;
    *buf = (msg->nvec == 1) ? msg->iov[0].buffer : (void *)&msg->iov[0];
    *len = (msg->nvec == 1) ? msg->iov[0].length : msg->nvec;
}

void UcpWorker::SendRequest(UcpAmSendInfo *msgs[], int size)
{
    UcpAmSendInfo *ptr, *head, *tail;

    for (int i = size-1; i > 0; --i) {
         msgs[i]->link.tqe_next = msgs[i-1];
    }
    head = msgs[size-1];
    tail = msgs[0];

    // Just hang the requests on send_list_, caller should
    // signal worker to send 
    do {
        // here we don't read other thread's data, so we use
        // relaxed memory order
        ptr = send_list_.load(butil::memory_order_relaxed);
        tail->link.tqe_next = ptr;
    } while (!send_list_.compare_exchange_strong(ptr, head,
             butil::memory_order_release));
}

bool UcpWorker::KeepSendRequest(void)
{
    ucp_request_param_t param;
    UcpAmSendInfo *msg = NULL, *prev = NULL, *next = NULL;
    void *buf = NULL;
    size_t len = 0;

    // assert(mutex_.is_locked_by_me());
    if (!send_list_.load(butil::memory_order_relaxed))
        return false;
    msg = send_list_.exchange(NULL, butil::memory_order_acquire);

    // Reverse the list to dispatch it in order
    prev = NULL;
    while (msg) {
        next = msg->link.tqe_next;
        msg->link.tqe_next = prev;
        prev = msg;
        msg = next;
    }

    for (msg = prev; msg; msg = next) {
        next = msg->link.tqe_next;

        UcpConnectionRef &conn = msg->conn;
        if (BAIDU_UNLIKELY(conn->state_ == UcpConnection::STATE_CLOSED)) {
            /* already closed */
            UcpAmSendInfo::Release(msg);
            continue;
        }
        SetupSendRequestParam(msg, &param, &buf, &len);
        TAILQ_INSERT_TAIL(&conn->send_q_, msg, link);

        msg->req = ucp_am_send_nbx(conn->ep_, AM_ID, &msg->header,
            sizeof(msg->header), buf, len, &param);
        if (msg->req == NULL) {
            TAILQ_REMOVE(&conn->send_q_, msg, link);
            UcpAmSendInfo::Release(msg);
        } else if (UCS_PTR_IS_ERR(msg->req)) {
            TAILQ_REMOVE(&conn->send_q_, msg, link);
            ucs_status_t st = UCS_PTR_STATUS(msg->req);
            conn->ucp_code_.store(st);
            SetDataReadyLocked(conn);
            UcpAmSendInfo::Release(msg);
            LOG(ERROR) << "Failed to ucp_am_send_nbx("
                       << ucs_status_string(st) << ")";
        }
    }
    return true;
}

ssize_t UcpWorker::StartSend(int cmd, UcpConnection *conn, butil::IOBuf *buf,
    size_t attachment_off)
{
    butil::IOBuf *data_list[1] = {buf};
    return StartSend(cmd, conn, data_list, &attachment_off, 1);
}

ssize_t UcpWorker::StartSend(int cmd, UcpConnection *conn,
    butil::IOBuf * const data_list[], size_t const attachment_off_list[],
    int ndata)
{
    BAIDU_SCOPED_LOCK(conn->send_mutex_);

    // Use batch submit to reduce atomic operations
    const int BATCH_SEND_SIZE = 6;
    UcpAmSendInfo *batch[BATCH_SEND_SIZE];
    int batch_num = 0;
    size_t total = 0;
    int err = 0;

    if (conn->ucp_code_.load(butil::memory_order_relaxed)) {
        errno = ENOTCONN;
        return -1;
    }
    if (data_list == NULL || ndata == 0)
        return 0;

    for (int i = 0; i < ndata; ++i) {
        MsgHeader header;
        UcpAmSendInfo *msg = UcpAmSendInfo::Allocate();
        if (msg == NULL) {
            LOG(ERROR) << "Cannot allocate UcpAmSendInfo";
            err = ENOMEM;
            break;
        }
        msg->nvec = 0;
        size_t attach_off = attachment_off_list[i];
        uint16_t to_pad = NVME_DWORD_ALIGN - (attach_off & (NVME_DWORD_ALIGN - 1));
        if (to_pad)
            pad_buf_.fill_ucp_iov(&msg->iov, &msg->nvec, to_pad);
        msg->buf.append(butil::IOBuf::Movable(*data_list[i]));
        msg->buf.fill_ucp_iov(&msg->iov, &msg->nvec, ULONG_MAX);
        header.init(cmd, to_pad, conn->next_send_sn_);
        MsgHeaderOut(&header, &msg->header);
        msg->conn = conn->shared_from_this();

        total += msg->buf.length();
        batch[batch_num++] = msg;
        if (batch_num == BATCH_SEND_SIZE) {
            SendRequest(batch, batch_num);
            batch_num = 0;
        }
        conn->next_send_sn_++;
    }
    if (batch_num != 0) {
        SendRequest(batch, batch_num);
    }

    if (pthread_self() != worker_tid_) {
        if (FLAGS_brpc_ucp_worker_busy_poll)
            goto out;
        if (worker_active_.load(std::memory_order_relaxed)) {
            MaybeWakeup();
            goto out;
        }
        if (!mutex_.try_lock()) {
            MaybeWakeup();
            goto out;
        }
        int have = KeepSendRequest();
        mutex_.unlock();
        if (have)
            MaybeWakeup();
    } else {
        MaybeWakeup();
    }

out:
    errno = err;
    return total ? total : -1;
}

bool UcpWorker::SendHello(UcpConnection *conn)
{
    // assert(w->mutex_.is_locked());
    butil::IOBuf buf;
    Hello hello;

    if (!hello.encode(buf)) {
        LOG(ERROR) << "can not encode Hello packet";
        conn->ucp_code_.store(UCS_ERR_NO_MEMORY);
        UcpConnectionRef ref(conn);
        SetDataReadyLocked(ref);
        return false;
    }

    ssize_t len = buf.length();
    return StartSend(UCP_CMD_HELLO, conn, &buf, 0) == len;
}

void UcpWorker::HandlePing(const UcpConnectionRef &conn, UcpAmMsg *msg)
{
    DispatchExternalEventLocked(new PingHandler(conn, msg));
}

void UcpWorker::HandlePong(const UcpConnectionRef &conn, UcpAmMsg *msg)
{
    DispatchExternalEventLocked(new PongHandler(conn, msg));
}

void UcpWorker::HandleHello(const UcpConnectionRef &conn, UcpAmMsg *msg)
{
    DispatchExternalEventLocked(new HelloHandler(conn, msg));
}

void UcpWorker::HandleHelloReply(const UcpConnectionRef &conn, UcpAmMsg *msg)
{
    DispatchExternalEventLocked(new HelloReplyHandler(conn, msg));
}

} // namespace brpc
