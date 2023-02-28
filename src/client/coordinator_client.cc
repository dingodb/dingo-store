// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>

#include "braft/raft.h"
#include "braft/route_table.h"
#include "braft/util.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "gflags/gflags.h"
#include "proto/coordinator.pb.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_string(coordinator_addr, "127.0.0.1:8201", "coordinator server addr");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_string(method, "Hello", "Request method");

bvar::LatencyRecorder g_latency_recorder("dingo-coordinator");

void SendHello(brpc::Controller& cntl,
               dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  std::string key = "Hello";
  // const char* op = nullptr;
  request.set_hello(0);
  stub.Hello(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " hello=" << request.hello()
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size()
              << " latency=" << cntl.latency_us();
  }
}

void SendHello1(brpc::Controller& cntl,
                dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  std::string key = "Hello";
  // const char* op = nullptr;
  request.set_hello(0);
  stub.Hello(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " hello=" << request.hello()
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size()
              << " latency=" << cntl.latency_us();
  }
}

void* Sender(void* /*arg*/) {
  while (!brpc::IsAskedToQuit()) {
    braft::PeerId leader(FLAGS_coordinator_addr);

    // rpc
    brpc::Channel channel;
    if (channel.Init(leader.addr, nullptr) != 0) {
      LOG(ERROR) << "Fail to init channel to " << leader;
      bthread_usleep(FLAGS_timeout_ms * 1000L);
      continue;
    }
    dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);

    if (FLAGS_method == "Hello") {
      SendHello(cntl, stub);
    }

    bthread_usleep(FLAGS_timeout_ms * 10000L);
  }
  return nullptr;
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    if (bthread_start_background(&tids[i], nullptr, Sender, nullptr) != 0) {
      LOG(ERROR) << "Fail to create bthread";
      return -1;
    }
  }

  while (!brpc::IsAskedToQuit()) {
    LOG_IF(INFO, !FLAGS_log_each_request)
        << "Sending Request"
        << " qps=" << g_latency_recorder.qps(1)
        << " latency=" << g_latency_recorder.latency(1);
  }

  LOG(INFO) << "Coordinator client is going to quit";
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }

  return 0;
}