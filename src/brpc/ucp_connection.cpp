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

#include "brpc/ucp_connection.h"
#include "brpc/ucp_cm.h"
#include "brpc/ucp_worker.h"
#include "brpc/socket.h"
#include "bvar/bvar.h"
#include "butil/uma/uma/uma.h"

#include <gflags/gflags.h>
#include <ucs/datastruct/list.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

// 1. 为了在IO路径上减少时延，使用resource来分配UcpAmMsg和UcpAmSendInfo
// 2. UcpConnection中的mutex只在关闭时使用wrlock, 其他部分使用rdlock

namespace brpc {

DEFINE_int32(brpc_ucp_ping_timeout, 10, "Number of seconds");
DEFINE_uint32(brpc_ucp_iov_reserve, 64, "Number of iov elements are cached");

bthread_attr_t ucp_consumer_thread_attr = BTHREAD_ATTR_NORMAL;

// to avoid be destructed, dynamically allocated it
static bvar::Adder<int> *g_ucp_conn;
static uma_zone_t am_msg_zone;
static uma_zone_t am_send_info_zone;

BAIDU_GLOBAL_INIT() {
    am_msg_zone = uma_zcreate("ucp::am_msg", sizeof(UcpAmMsg),
         NULL, NULL, UcpAmMsg::init, UcpAmMsg::fini, UMA_ALIGN_PTR, 0);
    am_send_info_zone = uma_zcreate("ucp::am_send_info", sizeof(UcpAmSendInfo),
         NULL, NULL, UcpAmSendInfo::init, UcpAmSendInfo::fini, UMA_ALIGN_PTR, 0);
    g_ucp_conn = new bvar::Adder<int>("ucp_connection_count");
}

UcpAmMsg::UcpAmMsg()
{
    header.init();
    data = nullptr;
    length = 0;
    nvec = 0;
    code = UCS_OK;
    req = nullptr;
    flags = 0;
}

int UcpAmMsg::init(void *mem, int size, int flags)
{
    new (mem) UcpAmMsg;
    return 0;
}

void UcpAmMsg::fini(void *mem, int size)
{
    UcpAmMsg *msg = static_cast<UcpAmMsg *>(mem);
    msg->UcpAmMsg::~UcpAmMsg();
}

UcpAmMsg *UcpAmMsg::Allocate(void)
{
    return (UcpAmMsg *)uma_zalloc(am_msg_zone, 0);   
}

void UcpAmMsg::Release(UcpAmMsg *o)
{
    o->conn.reset();
    o->header.init();
    o->data = nullptr;
    o->length = 0;
    o->nvec = 0;
    o->code = UCS_OK;
    o->req = nullptr;

#ifdef UCP_WORKER_Q_DEBUG
    CHECK(!o->has_flag(AMF_MSG_Q)) << "still on msg q";
    CHECK(!o->has_flag(AMF_RECV_Q)) << "still on receive q";
    CHECK(!o->has_flag(AMF_COMP_Q)) << "still on complete q";
#endif
 
    o->flags = 0;
    o->buf.clear();
    if (o->iov.size() > FLAGS_brpc_ucp_iov_reserve) {
        o->iov.resize(FLAGS_brpc_ucp_iov_reserve);
        o->iov.shrink_to_fit();
    }
    uma_zfree(am_msg_zone, o);
}

UcpAmSendInfo::UcpAmSendInfo()
{
    header.init();
    code = UCS_OK;
    req = nullptr;
    nvec = 0;
}

int UcpAmSendInfo::init(void *mem, int size, int flags)
{
    new (mem) UcpAmSendInfo;
    return 0;
}

void UcpAmSendInfo::fini(void *mem, int size)
{
    UcpAmSendInfo *info = static_cast<UcpAmSendInfo *>(mem);
    info->UcpAmSendInfo::~UcpAmSendInfo();
}

UcpAmSendInfo *UcpAmSendInfo::Allocate(void)
{
    return (UcpAmSendInfo *)uma_zalloc(am_send_info_zone, 0);   
}

void UcpAmSendInfo::Release(UcpAmSendInfo *o)
{
    o->conn.reset();
    o->header.init();
    o->code = UCS_OK;
    o->req = nullptr;
    o->nvec = 0;
    o->buf.clear();
    if (o->iov.size() > FLAGS_brpc_ucp_iov_reserve) {
        o->iov.resize(FLAGS_brpc_ucp_iov_reserve);
        o->iov.shrink_to_fit();
    }

    uma_zfree(am_send_info_zone, o);
}

UcpConnection::UcpConnection(UcpCm *cm, UcpWorker *w)
    : cm_(cm)
    , worker_(w)
    , ep_(NULL)
    , ucp_code_(UCS_OK)
    , ucp_recv_code_(UCS_OK)
    , socket_id_(-1)
    , socket_id_set_(false)
    , conn_was_reset_(false)
    , state_(STATE_NONE)
    , data_ready_flag_(false)
    , expect_sn_(0)
    , ready_list_(NULL)
{
    ping_seq_ = 0;
    bthread_rwlock_init(&mutex_, NULL);
    remote_side_.set_ucp();
    TAILQ_INIT(&recv_q_);
    TAILQ_INIT(&send_q_);
    next_send_sn_ = 0;
    *g_ucp_conn << 1;
}

UcpConnection::~UcpConnection()
{
    // Free all output data
    for (size_t i = 0; i < delayed_data_q_.size(); ++i) {
        delete delayed_data_q_[i];
    }

    if (ep_) {
        LOG(ERROR) << "ep_ should be NULL";
    }
    bthread_rwlock_destroy(&mutex_);
    DLOG(INFO) << __func__ << " " << this;
    *g_ucp_conn << -1;
}

void *UcpConnection::operator new(size_t size)
{
    size = roundup(size, BAIDU_CACHELINE_SIZE);
    return ::aligned_alloc(BAIDU_CACHELINE_SIZE, size);
}

void UcpConnection::operator delete(void *ptr)
{
    ::free(ptr);
}

int UcpConnection::Accept(ucp_conn_request_h req)
{
    bthread::v2::wlock_guard g(mutex_);
    // state_ is changed by Worker
    return worker_->Accept(this, req);
}

int UcpConnection::Connect(const butil::EndPoint &peer)
{
    bthread::v2::wlock_guard g(mutex_);
    // state_ is changed by Worker
    return worker_->Connect(this, peer);
}

void UcpConnection::Close()
{
    bthread::v2::wlock_guard g(mutex_);

    if (state_ == STATE_NONE || state_ == STATE_CLOSED) {
        return;
    }
    // state_ is changed by Worker
    worker_->Release(this);
    WakePing();
}

SocketId UcpConnection::GetSocketId() const
{
    return socket_id_;
}  

void UcpConnection::WakePing()
{
    std::unique_lock<bthread::Mutex> lg(ping_mutex_);
    ping_cond_.notify_all();
}

void UcpConnection::SetSocketId(SocketId id)
{
    bthread::v2::wlock_guard g(mutex_);

    socket_id_ = id;
    socket_id_set_ = true;
    if (ucp_code_.load()) {
        worker_->SetDataReady(this);
    } else if (state_ != STATE_CLOSED) {
        worker_->StartRecv(this);
    }
}

// called by UcpWorker to notify brpc Socket when data is ready
void UcpConnection::DataReady()
{
    data_ready_flag_ = false;
    // If state is STATE_CLOSED, we don't need to notify upper layer,
    // because only upper layer sets state to STATE_CLOSED with Close()
    // function.
    if (state_ != STATE_CLOSED) {
        if (socket_id_set_)
            Socket::StartInputEvent(socket_id_, EPOLLIN,
                                    ucp_consumer_thread_attr);
        else {
            DLOG(WARNING) << "brpc::Socket id is not set, "
                             "DataReady does not notify the socket";
        }
    }

    // Ping() is interested in error condition
    if (ucp_code_.load(butil::memory_order_relaxed)) {
        WakePing();
    }
}

ssize_t UcpConnection::Read(butil::IOBuf *out, size_t size_hint)
{
    ssize_t rc;
    bthread::v2::rlock_guard g(mutex_);
 
    if (state_ == STATE_HELLO || state_ == STATE_WAIT_HELLO) {
        // Delay receiving data, if IO error occurred, return EOF
        if (ucp_code_.load())
            return 0;
        // Try again later
        errno = EAGAIN;
        return -1;
    }

    // Connection closed, return EOF
    if (state_ != STATE_OPEN) {
        LOG(ERROR) << "Read with closed state";
        return 0;
    }

    worker_->MergeInputMessage(this);

    rc = in_buf_.cutn(out, size_hint); 

    if (rc == 0) {
        if (ucp_code_.load()) // IO error happened, return EOF
            return 0;
    }

    if (rc == 0) {
        rc = -1;
        errno = EAGAIN;
    }
 
    return rc;
}

ssize_t UcpConnection::Write(butil::IOBuf *buf, size_t attachment_off)
{
    butil::IOBuf *data_list[1];
    data_list[0] = buf;

    return Write(data_list, &attachment_off, 1);
}

ssize_t UcpConnection::Write(butil::IOBuf *data_list[],
    size_t attachment_off_list[], int ndata)
{
    bthread::v2::rlock_guard g(mutex_);

    if (state_ == STATE_HELLO || state_ == STATE_WAIT_HELLO) {
        // Delay sending data until hello is replied
        size_t total = 0;
        BAIDU_SCOPED_LOCK(send_mutex_);
        for (int i = 0; i < ndata; ++i) {
            butil::IOBuf *buf = new butil::IOBuf(butil::IOBuf::Movable(*data_list[i]));
            delayed_data_q_.push_back(buf);
            delayed_off_q_.push_back(attachment_off_list[i]);
            total += buf->length();
        }
        return total;
    }

    if (state_ != STATE_OPEN) {
        errno = ENOTCONN;
        return -1;
    }

    if (ucp_code_.load()) {
        errno = ECONNRESET;
        return -1;
    }

    ssize_t len = worker_->StartSend(UCP_CMD_BRPC, this, data_list,
        attachment_off_list, ndata);
    if (ucp_code_.load()) {
        errno = ECONNRESET;
        return -1;
    }
    return len;
}

// Called by UcpWorker to change state to STAE_OPEN
void UcpConnection::Open()
{
    bthread::v2::wlock_guard g(mutex_);
    if (state_ == STATE_HELLO || state_ == STATE_WAIT_HELLO) {
        state_ = STATE_OPEN;

        if (delayed_data_q_.size() != delayed_off_q_.size()) {
            LOG(FATAL) << "sizes of delayed queues are not same";
        }

        if (delayed_data_q_.size() != 0) {
            (void) worker_->StartSend(UCP_CMD_BRPC, this,
                    delayed_data_q_.data(), delayed_off_q_.data(),
                    delayed_off_q_.size());
            for (size_t i = 0; i < delayed_data_q_.size(); ++i) {
                delete delayed_data_q_[i];
            }
            delayed_data_q_.resize(0);
            delayed_off_q_.resize(0);
        }

        WakePing();
        DataReady();
    }
}

int UcpConnection::Ping(const timespec* abstime)
{
    int rc = DoPing(abstime);
    if (rc) {
        int err = errno;
        LOG(ERROR) << "Ping " << remote_side_str_ << " (" << strerror(err) << ")";
        errno = err;
    }
    return rc;
}

int UcpConnection::WaitOpen(const timespec *abstime)
{
    int rc = 0;

    std::unique_lock<bthread::Mutex> pg(ping_mutex_);
    while (rc == 0 && (state_ == STATE_HELLO || state_ == STATE_WAIT_HELLO) &&
           ucp_code_.load() == 0 && ucp_recv_code_.load() == 0) {
           rc = ping_cond_.wait_until(pg, *abstime);
    }
    return rc;
}

int UcpConnection::DoPing(const timespec* abstime)
{
    butil::IOBuf buf;
    struct timespec ts;

    if (abstime == NULL) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += FLAGS_brpc_ucp_ping_timeout;
        abstime = &ts;
    }

    if (state_ == STATE_HELLO || state_ == STATE_WAIT_HELLO) {
        int err = WaitOpen(abstime);
        if (err != 0)
            return err;
    }

    bthread::v2::rlock_guard lg(mutex_);
    if (state_ != STATE_OPEN) {
        errno = ENOTCONN;
        return -1;
    }

    if (ucp_code_.load()) {
        errno = ECONNRESET;
        return -1;
    }

    buf.append("ping", 5);

    ping_mutex_.lock();
    int old = ping_seq_;
    ping_mutex_.unlock();

    ssize_t rc = worker_->StartSend(UCP_CMD_PING, this, &buf, 0);
    if (rc == -1) {
        return -1;
    }

    lg.unlock();

    rc = 0;
    std::unique_lock<bthread::Mutex> pg(ping_mutex_);
    while (rc == 0 && old == ping_seq_ && state_ != STATE_CLOSED &&
           ucp_code_.load() == 0 && ucp_recv_code_.load() == 0) {
        rc = ping_cond_.wait_until(pg, *abstime);
    }
    if (rc == 0) {
        if (state_ == STATE_CLOSED)
            rc = ENOTCONN;
        else if (ucp_code_.load() || ucp_recv_code_.load())
            rc = ECONNRESET;
    }
    errno = rc;
    return rc ? -1 : 0;
}

void UcpConnection::HandlePong(UcpAmMsg *msg)
{
    std::unique_lock<bthread::Mutex> lg(ping_mutex_);
    ping_seq_++;
    ping_cond_.notify_all();
    UcpAmMsg::Release(msg);
}

} // namespace brpc
