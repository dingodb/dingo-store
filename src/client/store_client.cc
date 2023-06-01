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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "client/store_client_function.h"
#include "fmt/core.h"
#include "glog/logging.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_int32(round_num, 1, "Round of requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_string(addrs, "127.0.0.1:10001,127.0.0.1:10002,127.0.0.1:10003", "server addrs");
DEFINE_string(raft_addrs, "127.0.0.1:10101:0,127.0.0.1:10102:0,127.0.0.1:10103:0", "raft addrs");
DEFINE_string(coor_addrs, "127.0.0.1:10101:0,127.0.0.1:10102:0,127.0.0.1:10103:0", "corr addrs");
DEFINE_string(method, "KvGet", "Request method");
DEFINE_string(key, "", "Request key");
DEFINE_string(value, "", "Request values");
DEFINE_string(prefix, "", "key prefix");
DEFINE_int32(region_id, 111111, "region id");
DEFINE_int32(region_count, 1, "region count");
DEFINE_int32(table_id, 0, "table id");
DEFINE_string(table_name, "", "table name");
DEFINE_string(raft_group, "store_default_test", "raft group");
DEFINE_int32(partition_num, 1, "table partition num");

bvar::LatencyRecorder g_latency_recorder("dingo-store");

const std::map<std::string, std::vector<std::string>> kParamConstraint = {
    {"RaftGroup", {"AddRegion", "ChangeRegion", "BatchAddRegion", "TestBatchPutGet"}},
    {"RaftAddrs", {"AddRegion", "ChangeRegion", "BatchAddRegion", "TestBatchPutGet"}},
    {"ThreadNum", {"BatchAddRegion", "TestBatchPutGet", "TestBatchPutGet"}},
    {"RegionCount", {"BatchAddRegion", "TestBatchPutGet"}},
    {"ReqNum", {"KvBatchGet", "TestBatchPutGet", "TestBatchPutGet", "AutoTest"}},
    {"TableName", {"AutoTest"}},
    {"PartitionNum", {"AutoTest"}},
};

void ValidateParam() {
  if (FLAGS_region_id == 0) {
    DINGO_LOG(FATAL) << "missing param region_id error";
  }

  if (FLAGS_raft_group.empty()) {
    auto methods = kParamConstraint.find("RaftGroup")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(FATAL) << "missing param raft_group error";
      }
    }
  }

  if (FLAGS_raft_addrs.empty()) {
    auto methods = kParamConstraint.find("RaftAddrs")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(FATAL) << "missing param raft_addrs error";
      }
    }
  }

  if (FLAGS_thread_num == 0) {
    auto methods = kParamConstraint.find("ThreadNum")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(FATAL) << "missing param thread_num error";
      }
    }
  }

  if (FLAGS_region_count == 0) {
    auto methods = kParamConstraint.find("RegionCount")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(FATAL) << "missing param region_count error";
      }
    }
  }

  if (FLAGS_req_num == 0) {
    auto methods = kParamConstraint.find("ReqNum")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(FATAL) << "missing param req_num error";
      }
    }
  }

  if (FLAGS_table_name.empty()) {
    auto methods = kParamConstraint.find("TableName")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(FATAL) << "missing param table_name error";
      }
    }
  }

  if (FLAGS_partition_num == 0) {
    auto methods = kParamConstraint.find("PartitionNum")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(FATAL) << "missing param partition_num error";
      }
    }
  }
}

void Sender(client::ServerInteractionPtr interaction, const std::string& method, int round_num) {
  ValidateParam();

  std::vector<std::string> raft_addrs;
  butil::SplitString(FLAGS_raft_addrs, ',', &raft_addrs);

  for (int i = 0; i < round_num; ++i) {
    DINGO_LOG(INFO) << fmt::format("round: {} / {}", i, round_num);
    // Region operation
    if (method == "AddRegion") {
      client::SendAddRegion(interaction, FLAGS_region_id, FLAGS_raft_group, raft_addrs);
    } else if (method == "ChangeRegion") {
      client::SendChangeRegion(interaction, FLAGS_region_id, FLAGS_raft_group, raft_addrs);
    } else if (method == "DestroyRegion") {
      client::SendDestroyRegion(interaction, FLAGS_region_id);
    } else if (method == "Snapshot") {
      client::SendSnapshot(interaction, FLAGS_region_id);
    } else if (method == "BatchAddRegion") {
      client::BatchSendAddRegion(interaction, FLAGS_region_id, FLAGS_region_count, FLAGS_thread_num, FLAGS_raft_group,
                                 raft_addrs);
    }

    // Kev/Value operation
    if (method == "KvGet") {
      std::string value;
      client::SendKvGet(interaction, FLAGS_region_id, FLAGS_key, value);
    } else if (method == "KvBatchGet") {
      client::SendKvBatchGet(interaction, FLAGS_region_id, FLAGS_prefix, FLAGS_req_num);
    } else if (method == "KvPut") {
      client::SendKvPut(interaction, FLAGS_region_id, FLAGS_key);
    } else if (method == "KvBatchPut") {
      client::SendKvBatchPut(interaction, FLAGS_region_id, FLAGS_prefix, 100);
    } else if (method == "KvPutIfAbsent") {
      client::SendKvPutIfAbsent(interaction, FLAGS_region_id, FLAGS_key);
    } else if (method == "KvBatchPutIfAbsent") {
      client::SendKvBatchPutIfAbsent(interaction, FLAGS_region_id, FLAGS_prefix, 100);
    } else if (method == "KvBatchDelete") {
      client::SendKvBatchDelete(interaction, FLAGS_region_id, FLAGS_key);
    } else if (method == "KvDeleteRange") {
      client::SendKvDeleteRange(interaction, FLAGS_region_id, FLAGS_prefix);
    } else if (method == "KvScan") {
      client::SendKvScan(interaction, FLAGS_region_id, FLAGS_prefix);
    } else if (method == "KvCompareAndSet") {
      client::SendKvCompareAndSet(interaction, FLAGS_region_id, FLAGS_key);
    } else if (method == "KvBatchCompareAndSet") {
      client::SendKvBatchCompareAndSet(interaction, FLAGS_region_id, FLAGS_prefix, 100);
    }

    // Test
    if (method == "TestBatchPut") {
      client::TestBatchPut(interaction, FLAGS_region_id, FLAGS_thread_num, FLAGS_req_num, FLAGS_prefix);
    } else if (method == "TestBatchPutGet") {
      client::TestBatchPutGet(interaction, FLAGS_region_id, FLAGS_thread_num, FLAGS_req_num, FLAGS_prefix);
    } else if (method == "TestRegionLifecycle") {
      client::TestRegionLifecycle(interaction, FLAGS_region_id, FLAGS_raft_group, raft_addrs, FLAGS_region_count,
                                  FLAGS_thread_num, FLAGS_req_num, FLAGS_prefix);
    }

    // Auto test
    if (method == "AutoTest") {
      auto ctx = std::make_shared<client::Context>();
      ctx->table_name = FLAGS_table_name;
      ctx->partition_num = FLAGS_partition_num;
      ctx->req_num = FLAGS_req_num;
      ctx->store_interaction = interaction;

      client::ServerInteractionPtr coor_interaction = std::make_shared<client::ServerInteraction>();
      coor_interaction->Init(FLAGS_coor_addrs);

      ctx->coordinator_interaction = coor_interaction;

      AutoTest(ctx);
    }
  }
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  google::InitGoogleLogging(argv[0]);

  google::ParseCommandLineFlags(&argc, &argv, true);

  client::ServerInteractionPtr interaction = std::make_shared<client::ServerInteraction>();
  interaction->Init(FLAGS_addrs);

  Sender(interaction, FLAGS_method, FLAGS_round_num);

  return 0;
}
