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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "braft/raft.h"
#include "braft/route_table.h"
#include "braft/util.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "gflags/gflags.h"
#include "proto/store.pb.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_string(store_addr, "127.0.0.1:20001", "store server addr");

bvar::LatencyRecorder g_latency_recorder("dingo-store");

void sendKvGet(brpc::Controller& cntl,
               dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvGetRequest request;
  dingodb::pb::store::KvGetResponse response;

  std::string key = "Hello";
  const char* op = NULL;
  request.set_key(key.data(), key.size());
  stub.KvGet(&cntl, &request, &response, NULL);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " key=" << request.key() << " value=" << response.value()
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size()
              << " latency=" << cntl.latency_us();
  }
}

void sendAddRegion(brpc::Controller& cntl,
                   dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::AddRegionRequest request;
  dingodb::pb::store::AddRegionResponse response;

  dingodb::pb::common::Region* region = request.mutable_region();
  region->set_region_id(10000);
  region->set_region_epoch(1);
  region->set_table_id(10);
  // region->set_table_name("test-10");
  region->set_partition_id(1);
  // region->set_replica_num(1);
  dingodb::pb::common::Range* range = region->mutable_range();
  range->set_start_key("0000000");
  range->set_end_key("11111111");
  // region->add_electors("127.0.0.1:20101:0");
  // region->add_electors("127.0.0.1:20102:0");
  // region->add_electors("127.0.0.1:20103:0");

  stub.AddRegion(&cntl, &request, &response, NULL);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size()
              << " latency=" << cntl.latency_us();
  }
}

void* sender(void* arg) {
  for (int i = 0; i < FLAGS_req_num; ++i) {
    braft::PeerId leader(FLAGS_store_addr);

    // rpc
    brpc::Channel channel;
    if (channel.Init(leader.addr, NULL) != 0) {
      LOG(ERROR) << "Fail to init channel to " << leader;
      bthread_usleep(FLAGS_timeout_ms * 1000L);
      continue;
    }
    dingodb::pb::store::StoreService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);

    // sendKvGet(cntl, stub);
    sendAddRegion(cntl, stub);

    g_latency_recorder << cntl.latency_us();

    bthread_usleep(FLAGS_timeout_ms * 10000L);
  }

  return nullptr;
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    if (bthread_start_background(&tids[i], NULL, sender, NULL) != 0) {
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

  LOG(INFO) << "Store client is going to quit";
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    bthread_join(tids[i], NULL);
  }

  return 0;
}
