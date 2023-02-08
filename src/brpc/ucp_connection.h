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

#ifndef BRPC_UCP_CONNECTION_H
#define BRPC_UCP_CONNECTION_H

#include "butil/macros.h"
#include "butil/endpoint.h"
#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "bthread/mutex.h"
#include "bthread/rwlock_v2.h"
#include "bthread/condition_variable.h"
#include "brpc/socket_id.h"
#include <ucp/api/ucp.h>
#include <sys/queue.h>
#include <assert.h>
#include <memory>
#include <vector>

namespace brpc {

class UcpCm;
class UcpWorker;
class UcpConnection;

typedef std::shared_ptr<UcpConnection> UcpConnectionRef;

enum {
    UCP_VER_0
};

enum {
    UCP_CMD_BRPC,
    UCP_CMD_PING,
    UCP_CMD_PONG,
    UCP_CMD_HELLO,
    UCP_CMD_HELLO_REPLY,
#define UCP_CMD_MAX UCP_CMD_HELLO_REPLY
};

struct MsgHeader {
    uint16_t ver;
    uint16_t cmd;
    uint16_t pad;
    uint16_t reserve;
    uint64_t sn;

    void init() {
        ver = UCP_VER_0;
        cmd = UCP_CMD_BRPC;
        pad = 0;
        reserve = 0;
        sn = -1L;
    }

    void init(uint16_t a_cmd, uint16_t a_pad, uint64_t a_sn) {
        ver = UCP_VER_0;
        cmd = a_cmd;
        pad = a_pad;
        reserve = 0;
        sn  = a_sn;
    }
};

struct UcpAmMsg;
struct UcpAmSendInfo;

typedef TAILQ_HEAD(UcpAmList, UcpAmMsg) UcpAmList;
typedef TAILQ_HEAD(UcpAmSendList, UcpAmSendInfo) UcpAmSendList;

// UcpAmMsg flags
enum {
    AMF_MSG_Q,
    AMF_RECV_Q,
    AMF_COMP_Q,
    AMF_RNDV,
    AMF_FINISH,
};

struct UcpAmMsg {
    TAILQ_ENTRY(UcpAmMsg) link;
    TAILQ_ENTRY(UcpAmMsg) comp_link;
    UcpConnectionRef conn;
    MsgHeader header;
    union {
        void *desc;
        void *data;
    };
    uint64_t length;
    butil::IOPortal buf;
    butil::iobuf_ucp_iov_t iov;
    int nvec;
    int flags;
    ucs_status_t code;
    void *req;

    void set_flag(int f) { flags |= UCS_BIT(f); }
    void clear_flag(int f) { flags &= ~UCS_BIT(f); }
    bool has_flag(int f) const { return !!(flags & UCS_BIT(f)); }

    static UcpAmMsg *Allocate(void);
    static void Release(UcpAmMsg *o);

    static int  init(void *mem, int size, int flags);
    static void fini(void *mem, int size);

private:
    UcpAmMsg();
    void operator delete( void * ) {}
};

struct UcpAmSendInfo {
    TAILQ_ENTRY(UcpAmSendInfo) link;
    UcpConnectionRef conn;
    butil::IOPortal buf;
    butil::iobuf_ucp_iov_t iov;
    MsgHeader header;
    ucs_status_t code;
    void *req;
    int nvec;

    static UcpAmSendInfo *Allocate(void);
    static void Release(UcpAmSendInfo *o);

    static int init(void *mem, int size, int flags);
    static void fini(void *mem, int size);

private:
    UcpAmSendInfo();
    void operator delete( void * ) {}
};

class UcpConnection {
public:
    UcpConnection(UcpCm* cm, UcpWorker *w);
    virtual ~UcpConnection();

    UcpCm *GetCm() const { return cm_; }
    UcpWorker *GetWorker() const { return worker_; }
    void SetSocketId(SocketId id);
    SocketId GetSocketId() const;
    ssize_t Read(butil::IOBuf *out, size_t n); 
    ssize_t Write(butil::IOBuf *buf, size_t attachment_off); 
    ssize_t Write(butil::IOBuf *data_list[], size_t attachment_off_list[], int ndata);
    int Ping(const timespec* abstime);

    void *operator new(size_t);
    void operator delete(void *);

    std::shared_ptr<UcpConnection> shared_from_this() const {
        return this_ptr_.lock();
    }

private:
    void Open();
    void Close();
    int Accept(ucp_conn_request_h req);
    int Connect(const butil::EndPoint &peer);
    void DataReady();
    void HandlePong(UcpAmMsg *msg);
    int  WaitOpen(const struct timespec *abstime);
    void WakePing(); 
    int DoPing(const struct timespec *abstime);

private:
    enum {
        STATE_NONE,
        STATE_HELLO,
        STATE_WAIT_HELLO,
        STATE_OPEN,
        STATE_CLOSED,
    };

    mutable bthread::v2::bthread_rwlock_t mutex_;

    // rarely changed data
    std::weak_ptr<UcpConnection> this_ptr_ BAIDU_CACHELINE_ALIGNMENT;
    UcpCm *cm_;
    UcpWorker *worker_;
    ucp_ep_h ep_;
    butil::atomic<ucs_status_t> ucp_code_;
    butil::atomic<ucs_status_t> ucp_recv_code_;
    butil::atomic<SocketId> socket_id_;
    bool socket_id_set_;
    int state_;
    // Cached remote address
    butil::EndPoint remote_side_;
    std::string remote_side_str_;
    std::vector<butil::IOBuf *> delayed_data_q_;
    std::vector<size_t> delayed_off_q_;

    bthread::Mutex ping_mutex_;
    bthread::ConditionVariable ping_cond_;
    int ping_seq_;

    // Accessed by brpc receiver thread
    butil::IOPortal in_buf_ BAIDU_CACHELINE_ALIGNMENT;

    // Accessed by brpc sender thread
    mutable bthread::Mutex send_mutex_;
    uint64_t next_send_sn_;
 
    // Accessed by UcpWorker
    bool data_ready_flag_ BAIDU_CACHELINE_ALIGNMENT;
    uint64_t expect_sn_;
    UcpAmList recv_q_;
    UcpAmSendList send_q_;

    union {
        // Accessed both by brpc receiver thread and UcpWorker
        butil::atomic<UcpAmMsg *> ready_list_;
        char cacheline__[BAIDU_CACHELINE_SIZE];
    } BAIDU_CACHELINE_ALIGNMENT;

    DISALLOW_COPY_AND_ASSIGN(UcpConnection);

    friend class UcpCm;
    friend class UcpWorker;
    friend class UcpHelloHandler;
};

} // namespace brpc
#endif
