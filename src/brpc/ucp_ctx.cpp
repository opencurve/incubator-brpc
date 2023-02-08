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

// Authors: Xu Yifeng @ netease

#include "brpc/ucp_ctx.h"
#include "butil/logging.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>

#include <deque>
#include <memory>
#include <utility>

#include <gflags/gflags.h>

namespace brpc {

DEFINE_int32(brpc_set_cpu_latency, -1, "Set cpu latency in microseconds");

static UcpContext *g_ucp_ctx;
static pthread_once_t g_ucp_ctx_init = PTHREAD_ONCE_INIT;

//
// Disabling cpu power-saving mode may reduce DMA latency. If you don't
// use busy-polling mode, you may turn off power-saving mode, normally
// you should set it to C0 with --brpc_set_cpu_latency=0.
// Note that the /dev/cpu_dma_latency can only be written by root,
// otherwise you should chmod its access right for other people.
//

static int set_cpu_latency(void)
{
    const int latency = FLAGS_brpc_set_cpu_latency;
    int err = 0;

    if (latency < 0)
        return -1;

    LOG(INFO) << "Setting cpu latency to " << latency << "us";
    butil::fd_guard fd(open("/dev/cpu_dma_latency", O_WRONLY | O_CLOEXEC));
    if (fd < 0) {
        err = errno;
        goto err_out;
    }
    if (write(fd, &latency, sizeof(latency)) != sizeof(latency)) {
        err = errno;
        goto err_out;
    }
    return fd.release();

err_out:
    LOG(ERROR) << "open /dev/cpu_dma_latency " << strerror(err)
               << " - need root permissions";
    errno = err;
    return -1;
}

static void init_ucx_ctx() {
    LOG(INFO) << "Running with ucp library version: " << ucp_get_version_string();

    g_ucp_ctx = new UcpContext;
    if (g_ucp_ctx->init()) {
        abort();
    }
}

UcpContext* get_or_create_ucp_ctx() {
    pthread_once(&g_ucp_ctx_init, init_ucx_ctx);
    return g_ucp_ctx;
}

UcpContext::UcpContext()
{
    context_ = NULL;
}

UcpContext::~UcpContext()
{
    fini();
}

void UcpContext::fini()
{
    if (context_) {
        ucp_cleanup(context_);
        context_ = NULL;
        cpu_latency_fd_.release();
    }
}

int UcpContext::init()
{
    ucp_params_t ucp_params;
    ucs_status_t status;
    int ret = 0;
    ucp_config_t *config;

    status = ucp_config_read(nullptr, nullptr, &config);
    if (status != UCS_OK) {
        LOG(ERROR) << "failed to ucp_config_read ("
                   << ucs_status_string(status) << ")";
        return -1;
    }
    std::unique_ptr<ucp_config_t, decltype(&ucp_config_release)>
        config_ref(config, ucp_config_release);

    /* UCP initialization */
    memset(&ucp_params, 0, sizeof(ucp_params));
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
                            UCP_PARAM_FIELD_MT_WORKERS_SHARED |
                            UCP_PARAM_FIELD_NAME;
    ucp_params.features = UCP_FEATURE_AM |
                          UCP_FEATURE_WAKEUP;
    ucp_params.mt_workers_shared = 1;
    ucp_params.name = "brpc";

    status = ucp_init(&ucp_params, config, &context_);
    if (status != UCS_OK) {
        LOG(ERROR) << "failed to ucp_init ("
                   << ucs_status_string(status) << ")";
        context_ = NULL;
        ret = -1;
    }

    if (ret == 0) {
        // Ignore error. Error setting latency is not a failure. 
        cpu_latency_fd_.reset(set_cpu_latency());
    }

    return ret;
}

int create_ucp_worker(ucp_context_h ucp_ctx, ucp_worker_h *ucp_worker,
    int events, const char *name, int *efd)
{
    ucp_worker_params_t worker_params;
    ucs_status_t stat;

    memset(&worker_params, 0, sizeof(worker_params));

    worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE |
                                UCP_WORKER_PARAM_FIELD_AM_ALIGNMENT|
                                UCP_WORKER_PARAM_FIELD_NAME ;
    worker_params.name = name;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
    // at least 4 bytes (dword for nvme)
    worker_params.am_alignment = 64;
    if (events) {
        worker_params.field_mask |= UCP_WORKER_PARAM_FIELD_EVENTS;
        worker_params.events = events;
    }

    stat = ucp_worker_create(ucp_ctx, &worker_params, ucp_worker);
    if (stat != UCS_OK) {
        LOG(ERROR) << "failed to ucp_worker_create ("
                   << ucs_status_string(stat) << ")";
        *ucp_worker = NULL;
        *efd = -1;
        return -1;
    }
    stat = ucp_worker_get_efd(*ucp_worker, efd);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucx failed to ucp_worker_get_efd ("
                   << ucs_status_string(stat) << ")";
        ucp_worker_destroy(*ucp_worker);
        *ucp_worker = NULL;
        *efd = -1;
        return -1;
    }

    return 0;
}

int create_ucp_ep(ucp_worker_h w, ucp_conn_request_h conn_request,
    ucp_err_handler_cb_t err_cb, void *err_arg, ucp_ep_h *ep)
{
    ucp_ep_params_t ep_params;
    ucs_status_t stat;

    ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                UCP_EP_PARAM_FIELD_CONN_REQUEST;
    ep_params.conn_request    = conn_request;
    ep_params.err_handler.cb  = err_cb;
    ep_params.err_handler.arg = err_arg;
    stat = ucp_ep_create(w, &ep_params, ep);
    if (stat != UCS_OK) {
        LOG(ERROR) << "failed to create an endpoint on the server: ("
                   << ucs_status_string(stat) << ")";
        return -1;
    }
    return 0;
}

int create_ucp_ep(ucp_worker_h w, const butil::EndPoint &endpoint,
    ucp_err_handler_cb_t err_cb, void *err_arg, ucp_ep_h *ep)
{
    ucp_ep_params_t ep_params;
    struct sockaddr_in saddr;
    ucs_status_t status;

    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_addr = endpoint.ip;
    saddr.sin_port = htons(endpoint.port);

    ep_params.field_mask       = UCP_EP_PARAM_FIELD_FLAGS       |
                                 UCP_EP_PARAM_FIELD_SOCK_ADDR   |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    ep_params.err_mode         = UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb   = err_cb;
    ep_params.err_handler.arg  = err_arg;
    ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr    = (struct sockaddr*)&saddr;
    ep_params.sockaddr.addrlen = sizeof(saddr);

    status = ucp_ep_create(w, &ep_params, ep);
    if (status != UCS_OK) {
        LOG(ERROR) << "failed to connect to " << endpoint << "("
                   << ucs_status_string(status) << ")";
        return -1;
    }

    return 0;
}

} // namespace brpc
