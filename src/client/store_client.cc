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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "client/client_helper.h"
#include "client/store_client_function.h"
#include "fmt/core.h"
#include "glog/logging.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_int32(round_num, 1, "Round of requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_string(store_addrs, "", "server addrs");
DEFINE_string(raft_addrs, "127.0.0.1:10101:0,127.0.0.1:10102:0,127.0.0.1:10103:0", "raft addrs");
DEFINE_string(coor_url, "", "coordinator url");
DEFINE_string(method, "KvGet", "Request method");
DEFINE_string(key, "", "Request key");
DEFINE_string(value, "", "Request values");
DEFINE_string(prefix, "", "key prefix");
DEFINE_int32(region_id, 0, "region id");
DEFINE_int32(region_count, 1, "region count");
DEFINE_int32(table_id, 0, "table id");
DEFINE_string(table_name, "", "table name");
DEFINE_string(raft_group, "store_default_test", "raft group");
DEFINE_int32(partition_num, 1, "table partition num");
DEFINE_int32(dimension, 16, "dimension");
DEFINE_int32(count, 50, "count");
DEFINE_int32(id, 0, "id");

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

// Get store addr from coordinator
std::vector<std::string> GetStoreAddrs(client::ServerInteractionPtr interaction, uint64_t region_id) {
  std::vector<std::string> addrs;
  auto region = client::SendQueryRegion(interaction, region_id);
  for (const auto& peer : region.definition().peers()) {
    const auto& location = peer.server_location();
    addrs.push_back(fmt::format("{}:{}", location.host(), location.port()));
  }

  return addrs;
}

void Sender(std::shared_ptr<client::Context> ctx, const std::string& method, int round_num) {
  ValidateParam();

  std::vector<std::string> raft_addrs;
  butil::SplitString(FLAGS_raft_addrs, ',', &raft_addrs);

  if (FLAGS_store_addrs.empty()) {
    if (method != "AutoDropTable" && method != "AutoTest") {
      // Get store addr from coordinator
      auto store_addrs = GetStoreAddrs(ctx->coordinator_interaction, FLAGS_region_id);
      ctx->store_interaction = std::make_shared<client::ServerInteraction>();
      if (!ctx->store_interaction->Init(store_addrs)) {
        DINGO_LOG(ERROR) << "Fail to init store_interaction";
        return;
      }
    }
  } else {
    ctx->store_interaction = std::make_shared<client::ServerInteraction>();
    if (!ctx->store_interaction->Init(FLAGS_store_addrs)) {
      DINGO_LOG(ERROR) << "Fail to init store_interaction";
      return;
    }
  }

  for (int i = 0; i < round_num; ++i) {
    DINGO_LOG(INFO) << fmt::format("round: {} / {}", i, round_num);
    // Region operation
    if (method == "AddRegion") {
      client::SendAddRegion(ctx->store_interaction, FLAGS_region_id, FLAGS_raft_group, raft_addrs);
    } else if (method == "ChangeRegion") {
      client::SendChangeRegion(ctx->store_interaction, FLAGS_region_id, FLAGS_raft_group, raft_addrs);
    } else if (method == "DestroyRegion") {
      client::SendDestroyRegion(ctx->store_interaction, FLAGS_region_id);
    } else if (method == "Snapshot") {
      client::SendSnapshot(ctx->store_interaction, FLAGS_region_id);
    } else if (method == "BatchAddRegion") {
      client::BatchSendAddRegion(ctx->store_interaction, FLAGS_region_id, FLAGS_region_count, FLAGS_thread_num,
                                 FLAGS_raft_group, raft_addrs);
    }

    // Kev/Value operation
    if (method == "KvGet") {
      std::string value;
      client::SendKvGet(ctx->store_interaction, FLAGS_region_id, FLAGS_key, value);
    } else if (method == "KvBatchGet") {
      client::SendKvBatchGet(ctx->store_interaction, FLAGS_region_id, FLAGS_prefix, FLAGS_req_num);
    } else if (method == "KvPut") {
      client::SendKvPut(ctx->store_interaction, FLAGS_region_id, FLAGS_key);
    } else if (method == "KvBatchPut") {
      client::SendKvBatchPut(ctx->store_interaction, FLAGS_region_id, FLAGS_prefix, 100);
    } else if (method == "KvPutIfAbsent") {
      client::SendKvPutIfAbsent(ctx->store_interaction, FLAGS_region_id, FLAGS_key);
    } else if (method == "KvBatchPutIfAbsent") {
      client::SendKvBatchPutIfAbsent(ctx->store_interaction, FLAGS_region_id, FLAGS_prefix, 100);
    } else if (method == "KvBatchDelete") {
      client::SendKvBatchDelete(ctx->store_interaction, FLAGS_region_id, FLAGS_key);
    } else if (method == "KvDeleteRange") {
      client::SendKvDeleteRange(ctx->store_interaction, FLAGS_region_id, FLAGS_prefix);
    } else if (method == "KvScan") {
      client::SendKvScan(ctx->store_interaction, FLAGS_region_id, FLAGS_prefix);
    } else if (method == "KvCompareAndSet") {
      client::SendKvCompareAndSet(ctx->store_interaction, FLAGS_region_id, FLAGS_key);
    } else if (method == "KvBatchCompareAndSet") {
      client::SendKvBatchCompareAndSet(ctx->store_interaction, FLAGS_region_id, FLAGS_prefix, 100);
    } else if (method == "VectorSearch") {
      client::SendVectorSearch(ctx->store_interaction, FLAGS_region_id, FLAGS_dimension, FLAGS_id);
    } else if (method == "VectorAdd") {
      client::SendVectorAdd(ctx->store_interaction, FLAGS_region_id, FLAGS_dimension, FLAGS_count);
    } else if (method == "VectorDelete") {
      client::SendVectorDelete(ctx->store_interaction, FLAGS_region_id, FLAGS_count);
    }

    // Test
    if (method == "TestBatchPut") {
      client::TestBatchPut(ctx->store_interaction, FLAGS_region_id, FLAGS_thread_num, FLAGS_req_num, FLAGS_prefix);
    } else if (method == "TestBatchPutGet") {
      client::TestBatchPutGet(ctx->store_interaction, FLAGS_region_id, FLAGS_thread_num, FLAGS_req_num, FLAGS_prefix);
    } else if (method == "TestRegionLifecycle") {
      client::TestRegionLifecycle(ctx->store_interaction, FLAGS_region_id, FLAGS_raft_group, raft_addrs,
                                  FLAGS_region_count, FLAGS_thread_num, FLAGS_req_num, FLAGS_prefix);
    } else if (method == "TestDeleteRangeWhenTransferLeader") {
      client::TestDeleteRangeWhenTransferLeader(ctx, FLAGS_region_id, FLAGS_req_num, FLAGS_prefix);
    }

    // Auto test
    if (method == "AutoTest") {
      ctx->table_name = FLAGS_table_name;
      ctx->partition_num = FLAGS_partition_num;
      ctx->req_num = FLAGS_req_num;

      AutoTest(ctx);
    }

    // Table operation
    if (method == "AutoDropTable") {
      ctx->req_num = FLAGS_req_num;
      client::AutoDropTable(ctx);
    }

    if (i + 1 < round_num) {
      bthread_usleep(1000 * 1000L);
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

  if (FLAGS_coor_url.empty()) {
    FLAGS_coor_url = "file://./coor_list";
  }

  auto ctx = std::make_shared<client::Context>();
  if (!FLAGS_coor_url.empty()) {
    std::string path = FLAGS_coor_url;
    path = path.replace(path.find("file://"), 7, "");
    auto addrs = client::Helper::GetAddrsFromFile(path);
    if (addrs.empty()) {
      DINGO_LOG(ERROR) << "url not find addr, path=" << path;
      return -1;
    }

    auto coordinator_interaction = std::make_shared<client::ServerInteraction>();
    if (!coordinator_interaction->Init(addrs)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << FLAGS_coor_url;
      return -1;
    }
    ctx->coordinator_interaction = coordinator_interaction;
  }

  Sender(ctx, FLAGS_method, FLAGS_round_num);

  return 0;
}
