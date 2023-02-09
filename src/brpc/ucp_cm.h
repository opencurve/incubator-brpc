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

#ifndef BRPC_UCP_CM_H
#define BRPC_UCP_CM_H

#include "bthread/mutex.h"
#include "bthread/condition_variable.h"
#include "brpc/socket_id.h"
#include "brpc/ucp_connection.h"

#include <sys/types.h>
#include <queue>

namespace brpc {

class UcpWorkerPool;

// Connection manager class
class UcpCm {
public:
    enum Status {
        UNINITIALIZED = 0,
        READY = 1,
        RUNNING = 2,
        STOPPING = 3,
    };

    UcpCm();
    ~UcpCm();
    int Start(int nworkers);
    void Stop();
    Status status() const { return status_; }
    // we create a pipe, and use write-end fd to represent a connection,
    // the write-end fd is given to user, and user must close it explicitly.
    // when the write-end is closed, the manager will be notified, and it
    // will close the connection in background.
    int Accept(ucp_conn_request_h req);
    int Connect(const butil::EndPoint &peer);
    UcpConnectionRef GetConnection(int fd1);

private:
    struct ConnectionOptions {
        ucp_conn_request_h req;
        butil::EndPoint endpoint;
        ConnectionOptions() { req = NULL; }
    };

    struct Fd0Item {
        int fd1;
        int64_t cookie_;

        Fd0Item() : cookie_(0) {}
    };

    struct Fd1Item {
        UcpConnectionRef conn;
        int64_t cookie_;

        Fd1Item() : cookie_(0) {}
    };

    static void* RunFdThread(void *arg);
    int Initialize();
    void UnInitialize();
    void DoRunEventLoop();
    void DoRunFdThread();
    void HandleFdInput(int fd);
    int StartFdThread();
    void StopFdThread();
    int DoConnect(const ConnectionOptions& opts);
    void ReplaceFd1(int fd1, const Fd1Item &item);

    bthread::Mutex mutex_;
    UcpWorkerPool *pool_;
    Status status_;
    bthread_t fd_tid_;
    int epfd_;
    int pipe_fds_[2];
    int64_t gen_;
    std::map<int, Fd0Item> fd_conns_0_;
    std::map<int, Fd1Item> fd_conns_1_;

    DISALLOW_COPY_AND_ASSIGN(UcpCm);

    friend class Socket;
    friend class UcpConnection;
};

UcpCm* get_or_create_ucp_cm();

} // namespace brpc
#endif
