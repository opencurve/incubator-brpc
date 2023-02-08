// Copyright (c) 2022 Netease, Inc.
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

#include "butil/macros.h"
#include "butil/fd_utility.h"
#include "brpc/socket.h"
#include "bthread/unstable.h"

#include "ucp_cm.h"
#include "ucp_worker_pool.h"

#include <gflags/gflags.h>
#include <fcntl.h>              /* Obtain O_* constant definitions */
#include <poll.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

//
// UcpCM创建的UcpConnection将以fd的方式返回，fd是一个pipe写端的句柄。
// 使用fd的目的是因为socket很多方面用到fd，并且SocketOptions就是传递fd作为
// 创建socket的参数。为了方便，固使用fd。
// Socket层面只需要通过这个fd查询到UcpConnection对象，并使用这个对象的io接口。
// 
namespace brpc {

// 系统一共有多少个Ucp Worker，基本上一个就够了。
DEFINE_int32(brpc_ucp_workers, 1, "Number of ucp workers");

static pthread_once_t g_ucp_cm_init = PTHREAD_ONCE_INIT;
static UcpCm *g_cm;

static void init_cm(void)
{
    UcpCm *cm = new UcpCm;
    int rc = cm->Start(FLAGS_brpc_ucp_workers);
    CHECK(rc == 0) << "Failed to start ucp connection manager";
    g_cm = cm;
}

UcpCm *
get_or_create_ucp_cm(void)
{
    pthread_once(&g_ucp_cm_init, init_cm);
    return g_cm;
}

UcpCm::UcpCm()
    : pool_(NULL)
    , status_(UNINITIALIZED)
    , fd_tid_(INVALID_BTHREAD)
    , epfd_(-1)
{
    pipe_fds_[0] = pipe_fds_[1] = -1;
}

UcpCm::~UcpCm()
{
    Stop();
    UnInitialize();
}

int UcpCm::Initialize()
{
    epfd_ = epoll_create1(O_CLOEXEC);
    if (epfd_ == -1) {
        PLOG(ERROR) << "epoll_create1 failed";
        return -1;
    }
    if (pipe2(pipe_fds_, O_CLOEXEC)) {
        PLOG(ERROR) << "pipe failed";
        close(epfd_);
        epfd_ = -1;
        return -1;
    }
    
    return 0;
}

void UcpCm::UnInitialize()
{
    if (pipe_fds_[0] != -1)
        close(pipe_fds_[0]);
    if (pipe_fds_[1] != -1)
        close(pipe_fds_[1]);
    if (epfd_ != 1)
        close(epfd_);
    epfd_ = pipe_fds_[0] = pipe_fds_[1] = -1;
}

int UcpCm::Start(int nworkers)
{
    BAIDU_SCOPED_LOCK(mutex_);
    if (status_ == UNINITIALIZED) {
        if (Initialize() != 0) {
            LOG(FATAL) << "Fail to initialize UcpCm";
            return -1;
        }
        status_ = READY;
    }
    if (status_ != READY) {
        LOG(FATAL) << "UcpCm hasn't stopped yet, status=" << status();
        return -1;
    }
    pool_ = new UcpWorkerPool(nworkers);
    if (pool_->Start()) {
        delete pool_;
        pool_ = NULL;
        return -1;
    }

    if (StartFdThread()) {
        goto stop_pool;
    }

    status_ = RUNNING;
    return 0;

stop_pool:
    pool_->Stop();
    delete pool_;
    pool_ = NULL;
    return -1;
}

void UcpCm::Stop()
{
    mutex_.lock();
    if (status_ != RUNNING) {
        mutex_.unlock();
        return;
    }
    status_ = STOPPING;
    mutex_.unlock();

    pool_->Stop();
    delete pool_;
    pool_ = NULL;

    StopFdThread();

    mutex_.lock();
    status_ = READY; 
    mutex_.unlock();
}

int UcpCm::Accept(ucp_conn_request_h req)
{
    ConnectionOptions opt;

    opt.req = req;
    return DoConnect(opt);
}

int UcpCm::Connect(const butil::EndPoint &peer)
{
    ConnectionOptions opt;

    opt.endpoint = peer;
    return DoConnect(opt);
}

int UcpCm::DoConnect(const ConnectionOptions& opts)
{
    UcpWorker *worker = pool_->GetWorker();
    struct stat st;
    int fds[2];
    int saved_errno;
    int err = 0;

    if (pipe2(fds, O_CLOEXEC|O_NONBLOCK)) {
        saved_errno = errno;
        PLOG(ERROR) << "pipe2() failed";
        errno = saved_errno;
        return -1;
    }

    if (fstat(fds[1], &st)) {
        saved_errno = errno;
        PLOG(ERROR) << "fstat() failed";
        ::close(fds[0]);
        ::close(fds[1]);
        errno = saved_errno;
        return -1;
    }
 
    // We allocate object by operator new which is aligned to cache line,
    // don't use make_shared
    UcpConnection *conn_ptr = new UcpConnection(this, worker);
    UcpConnectionRef conn(conn_ptr);
    conn->this_ptr_ = std::weak_ptr<UcpConnection>(conn);

    Fd0Item item0;
    Fd1Item item1;

    mutex_.lock();
    item0.fd1 = fds[1];
    item0.ino = st.st_ino;
    fd_conns_0_[fds[0]] = item0;

    item1.conn = conn;
    item1.mode = st.st_mode;
    item1.ino  = st.st_ino;
    ReplaceFd1(fds[1], item1);
    mutex_.unlock();

    if (opts.req)
        err = conn->Accept(opts.req);
    else
        err = conn->Connect(opts.endpoint);

    if (err) {
        err = ECONNRESET;
        goto error;
    }

    epoll_event evt;
    evt.data.fd = fds[0];
    evt.events = EPOLLIN;
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fds[0], &evt)) {
        PLOG(ERROR) << "epoll_ctl failed";
        err = errno;
        goto error;
    }
    return fds[1];

error:
    mutex_.lock();
    fd_conns_0_.erase(fds[0]);
    fd_conns_1_.erase(fds[1]);
    mutex_.unlock();
    ::close(fds[0]);
    ::close(fds[1]);
    errno = err;
    return -1; 
}

int UcpCm::StartFdThread()
{
    if (bthread_start_background(&fd_tid_, NULL, RunFdThread, this)) {
        return -1;
    }
    return 0;
}

void UcpCm::StopFdThread()
{
    epoll_event evt;

    evt.events = EPOLLOUT;
    evt.data.fd = pipe_fds_[1];
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, pipe_fds_[1], &evt)) {
        PLOG(FATAL) << "epoll_ctl_add failed";
    }
    bthread_join(fd_tid_, NULL);
    fd_tid_ = INVALID_BTHREAD;
    if (epoll_ctl(epfd_, EPOLL_CTL_DEL, pipe_fds_[1], NULL)) {
        PLOG(FATAL) << "epoll_ctl_del failed";
    }
}

void *UcpCm::RunFdThread(void *arg)
{
    UcpCm *cm = (UcpCm *)arg;
    cm->DoRunFdThread();
    return NULL;
}

void UcpCm::DoRunFdThread()
{
    for (;;) {
        bthread_fd_wait(epfd_, EPOLLIN);

        epoll_event e[32];
        int n = epoll_wait(epfd_, e, ARRAY_SIZE(e), 0);
        for (int i = 0; i < n; ++i) {
            if (e[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP)) {
                HandleFdInput(e[i].data.fd);
            }
        }

        for (int i = 0; i < n; ++i) {
            if (e[i].events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) {
                if (e[i].data.fd == pipe_fds_[1])
                    goto out;
            }
        }
    }
out:
    ;
}

void UcpCm::HandleFdInput(int fd0)
{
    // lock guard
    std::unique_lock<bthread::Mutex> lg(mutex_);

    auto it0 = fd_conns_0_.find(fd0);
    if (it0 == fd_conns_0_.end()) {
        // not found in table 0, remove fd0
        lg.unlock();
        ::close(fd0);
        LOG(ERROR) << "fd0 not found in fd_conns_0_ map, fd=" << fd0;
        return;
    }
    Fd0Item item0 = it0->second;
    int fd1 = it0->second.fd1;
    auto it1 = fd_conns_1_.find(fd1);
    if (it1 == fd_conns_1_.end()) {
        // not found fd1 in table 1, remove fd0, this is unlikely
        fd_conns_0_.erase(fd0);
        lg.unlock();
        ::close(fd0);
        LOG(ERROR) << "fd1 not found in fd_conns_1_ map";
        return;
    }

    if (item0.ino != it1->second.ino) {
        // the entry is replaced by a new connection, remove fd0 now
        fd_conns_0_.erase(fd0);
        lg.unlock();
        ::close(fd0);
        LOG(INFO) << "detected that fd1item was replaced";
        return;
    }

    UcpConnectionRef conn = it1->second.conn;
 
    char buf[64];
    int n = read(fd0, buf, sizeof(buf));
    if (n == 0) { // EOF, peer closed
        fd_conns_0_.erase(fd0);
        fd_conns_1_.erase(fd1);
        lg.unlock();
        ::close(fd0);
        // don't close fd1, it is owned by user
        conn->Close();
        return;
    } else if (n > 0) {
        LOG(ERROR) << "written to monitor fd detected";
    } else if (errno != EAGAIN && errno != EINTR) {
        PLOG(ERROR) << "read failed";
    }
    lg.unlock();
}

UcpConnectionRef UcpCm::GetConnection(int fd1)
{
    UcpConnectionRef conn;
    struct stat b;

    if (fstat(fd1, &b))
        return conn;

    BAIDU_SCOPED_LOCK(mutex_);
    auto it = fd_conns_1_.find(fd1);
    if (it == fd_conns_1_.end()) {
        return conn;
    }

    if (b.st_mode == it->second.mode &&
        b.st_ino == it->second.ino) {
        return it->second.conn;
    }

    return conn;
}

void UcpCm::ReplaceFd1(int fd1, const Fd1Item &item)
{
    // assert(mutex_.is_locked_by_me());
    auto it1 = fd_conns_1_.find(fd1);
    if (it1 == fd_conns_1_.end()) {
        fd_conns_1_[fd1] = item;
        return;
    }

    // Replace existing connection, note we don't
    // close fd1, because it is owned by user.
    UcpConnectionRef conn = it1->second.conn;
    fd_conns_1_[fd1] = item;

    mutex_.unlock();
    conn->Close();
    conn.reset();
    mutex_.lock();
}

} // namespace brpc
