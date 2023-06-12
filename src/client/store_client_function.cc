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

#include "client/store_client_function.h"

#include <sys/stat.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "client/client_helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"

const int kBatchSize = 1000;

namespace client {

void SendVectorSearch(ServerInteractionPtr interaction, uint64_t region_id, uint32_t dimension, uint64_t id) {
  dingodb::pb::index::VectorSearchRequest request;
  dingodb::pb::index::VectorSearchResponse response;

  request.set_region_id(region_id);
  auto* vector = request.mutable_vector()->mutable_vector();

  for (int i = 0; i < dimension; i++) {
    vector->add_values(1.0 * i);
  }

  request.mutable_parameter()->set_top_n(10);

  if (id > 0) {
    request.mutable_vector()->set_id(id);
  }

  interaction->SendRequest("IndexService", "VectorSearch", request, response);

  DINGO_LOG(INFO) << "VectorSearch response: " << response.DebugString();
}

void SendVectorAdd(ServerInteractionPtr interaction, uint64_t region_id, uint32_t dimension, uint32_t count) {
  dingodb::pb::index::VectorAddRequest request;
  dingodb::pb::index::VectorAddResponse response;

  request.set_region_id(region_id);
  for (int i = 1; i <= count; ++i) {
    auto* vector_with_id = request.add_vectors();
    vector_with_id->set_id(i);
    for (int j = 0; j < dimension; j++) {
      vector_with_id->mutable_vector()->add_values(1.0 * dingodb::Helper::GenerateRandomInteger(0, 100) / 10);
    }
  }

  interaction->SendRequest("IndexService", "VectorAdd", request, response);

  DINGO_LOG(INFO) << "VectorAdd response: " << response.DebugString();
}

void SendVectorDelete(ServerInteractionPtr interaction, uint64_t region_id, uint32_t count) {
  dingodb::pb::index::VectorDeleteRequest request;
  dingodb::pb::index::VectorDeleteResponse response;

  request.set_region_id(region_id);
  for (int i = 1; i <= count; ++i) {
    request.add_ids(i);
  }

  interaction->SendRequest("IndexService", "VectorDelete", request, response);

  DINGO_LOG(INFO) << "VectorDelete response: " << response.DebugString();
}

void SendKvGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key, std::string& value) {
  dingodb::pb::store::KvGetRequest request;
  dingodb::pb::store::KvGetResponse response;

  request.set_region_id(region_id);
  request.set_key(key);

  interaction->SendRequest("StoreService", "KvGet", request, response);

  value = response.value();
}

void SendKvBatchGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count) {
  dingodb::pb::store::KvBatchGetRequest request;
  dingodb::pb::store::KvBatchGetResponse response;

  request.set_region_id(region_id);
  for (int i = 0; i < count; ++i) {
    std::string key = prefix + Helper::GenRandomString(30);
    request.add_keys(key);
  }

  interaction->SendRequest("StoreService", "KvBatchGet", request, response);
}

void SendKvPut(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key, std::string value) {
  dingodb::pb::store::KvPutRequest request;
  dingodb::pb::store::KvPutResponse response;

  request.set_region_id(region_id);
  auto* kv = request.mutable_kv();
  kv->set_key(key);
  kv->set_value(value.empty() ? Helper::GenRandomString(64) : value);

  interaction->SendRequest("StoreService", "KvPut", request, response);
}

void SendKvBatchPut(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count) {
  dingodb::pb::store::KvBatchPutRequest request;
  dingodb::pb::store::KvBatchPutResponse response;

  request.set_region_id(region_id);
  for (int i = 0; i < count; ++i) {
    std::string key = prefix + Helper::GenRandomString(30);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(Helper::GenRandomString(64));
  }

  interaction->SendRequest("StoreService", "KvBatchPut", request, response);
}

void SendKvPutIfAbsent(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key) {
  dingodb::pb::store::KvPutIfAbsentRequest request;
  dingodb::pb::store::KvPutIfAbsentResponse response;

  request.set_region_id(region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(key);
  kv->set_value(Helper::GenRandomString(64));

  interaction->SendRequest("StoreService", "KvPutIfAbsent", request, response);
}

void SendKvBatchPutIfAbsent(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix,
                            int count) {
  dingodb::pb::store::KvBatchPutIfAbsentRequest request;
  dingodb::pb::store::KvBatchPutIfAbsentResponse response;

  request.set_region_id(region_id);
  for (int i = 0; i < count; ++i) {
    std::string key = prefix + Helper::GenRandomString(30);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(Helper::GenRandomString(64));
  }

  interaction->SendRequest("StoreService", "KvBatchPutIfAbsent", request, response);
}

void SendKvBatchDelete(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key) {
  dingodb::pb::store::KvBatchDeleteRequest request;
  dingodb::pb::store::KvBatchDeleteResponse response;

  request.set_region_id(region_id);
  request.add_keys(key);

  interaction->SendRequest("StoreService", "KvBatchDelete", request, response);
}

void SendKvDeleteRange(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix) {
  dingodb::pb::store::KvDeleteRangeRequest request;
  dingodb::pb::store::KvDeleteRangeResponse response;

  request.set_region_id(region_id);
  request.mutable_range()->mutable_range()->set_start_key(prefix);
  request.mutable_range()->mutable_range()->set_end_key(dingodb::Helper::PrefixNext(prefix));
  request.mutable_range()->set_with_start(true);
  request.mutable_range()->set_with_end(false);

  interaction->SendRequest("StoreService", "KvDeleteRange", request, response);
  DINGO_LOG(INFO) << "delete count: " << response.delete_count();
}

void SendKvScan(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix) {
  dingodb::pb::store::KvScanBeginRequest request;
  dingodb::pb::store::KvScanBeginResponse response;

  request.set_region_id(region_id);
  request.mutable_range()->mutable_range()->set_start_key(prefix);
  request.mutable_range()->mutable_range()->set_end_key(dingodb::Helper::PrefixNext(prefix));
  request.mutable_range()->set_with_start(true);
  request.mutable_range()->set_with_end(false);

  interaction->SendRequest("StoreService", "KvScanBegin", request, response);
  if (response.error().errcode() != 0) {
    return;
  }

  dingodb::pb::store::KvScanContinueRequest continue_request;
  dingodb::pb::store::KvScanContinueResponse continue_response;
  continue_request.set_region_id(region_id);
  continue_request.set_scan_id(response.scan_id());
  int batch_size = 1000;
  continue_request.set_max_fetch_cnt(batch_size);

  int count = 0;
  for (;;) {
    interaction->SendRequest("StoreService", "KvScanContinue", continue_request, continue_response);
    if (continue_response.error().errcode() != 0) {
      return;
    }

    count += continue_response.kvs().size();
    if (continue_response.kvs().size() < batch_size) {
      break;
    }
  }

  DINGO_LOG(INFO) << "scan count: " << count;

  dingodb::pb::store::KvScanReleaseRequest release_request;
  dingodb::pb::store::KvScanReleaseResponse release_response;

  release_request.set_region_id(region_id);
  release_request.set_scan_id(response.scan_id());

  interaction->SendRequest("StoreService", "KvScanRelease", release_request, release_response);
}

void SendKvCompareAndSet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key) {
  dingodb::pb::store::KvCompareAndSetRequest request;
  dingodb::pb::store::KvCompareAndSetResponse response;

  request.set_region_id(region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(key);
  kv->set_value(Helper::GenRandomString(64));
  request.set_expect_value("");

  interaction->SendRequest("StoreService", "KvCompareAndSet", request, response);
}

void SendKvBatchCompareAndSet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix,
                              int count) {
  dingodb::pb::store::KvBatchCompareAndSetRequest request;
  dingodb::pb::store::KvBatchCompareAndSetResponse response;

  request.set_region_id(region_id);
  for (int i = 0; i < count; ++i) {
    std::string key = prefix + Helper::GenRandomString(30);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(Helper::GenRandomString(64));
  }

  for (int i = 0; i < count; i++) {
    request.add_expect_values("");
  }

  request.set_is_atomic(false);

  interaction->SendRequest("StoreService", "KvBatchCompareAndSet", request, response);
}

dingodb::pb::common::RegionDefinition BuildRegionDefinition(uint64_t region_id, const std::string& raft_group,
                                                            std::vector<std::string>& raft_addrs,
                                                            const std::string& start_key, const std::string& end_key) {
  dingodb::pb::common::RegionDefinition region_definition;
  region_definition.set_id(region_id);
  region_definition.set_epoch(1);
  region_definition.set_name(raft_group);
  dingodb::pb::common::Range* range = region_definition.mutable_range();
  range->set_start_key(start_key);
  range->set_end_key(end_key);

  int count = 0;
  for (auto& addr : raft_addrs) {
    std::vector<std::string> host_port_idx;
    butil::SplitString(addr, ':', &host_port_idx);

    auto* peer = region_definition.add_peers();
    peer->set_store_id(1000 + (++count));
    auto* raft_loc = peer->mutable_raft_location();
    raft_loc->set_host(host_port_idx[0]);
    raft_loc->set_port(std::stoi(host_port_idx[1]));
    if (host_port_idx.size() > 2) {
      raft_loc->set_port(std::stoi(host_port_idx[2]));
    }
  }

  return region_definition;
}

void SendAddRegion(ServerInteractionPtr interaction, uint64_t region_id, const std::string& raft_group,
                   std::vector<std::string> raft_addrs) {
  dingodb::pb::store::AddRegionRequest request;
  request.mutable_region()->CopyFrom(BuildRegionDefinition(region_id, raft_group, raft_addrs, "a", "z"));
  dingodb::pb::store::AddRegionResponse response;

  interaction->SendRequest("StoreService", "AddRegion", request, response);
}

void SendChangeRegion(ServerInteractionPtr interaction, uint64_t region_id, const std::string& raft_group,
                      std::vector<std::string> raft_addrs) {
  dingodb::pb::store::ChangeRegionRequest request;
  dingodb::pb::store::ChangeRegionResponse response;

  request.mutable_region()->CopyFrom(BuildRegionDefinition(region_id, raft_group, raft_addrs, "a", "z"));
  dingodb::pb::common::RegionDefinition* region = request.mutable_region();

  interaction->SendRequest("StoreService", "ChangeRegion", request, response);
}

void SendDestroyRegion(ServerInteractionPtr interaction, uint64_t region_id) {
  dingodb::pb::store::DestroyRegionRequest request;
  dingodb::pb::store::DestroyRegionResponse response;

  request.set_region_id(region_id);

  interaction->SendRequest("StoreService", "DestroyRegion", request, response);
}

void SendSnapshot(ServerInteractionPtr interaction, uint64_t region_id) {
  dingodb::pb::store::SnapshotRequest request;
  dingodb::pb::store::SnapshotResponse response;

  request.set_region_id(region_id);

  interaction->SendRequest("StoreService", "Snapshot", request, response);
}

void SendTransferLeader(ServerInteractionPtr interaction, uint64_t region_id, const dingodb::pb::common::Peer& peer) {
  dingodb::pb::store::TransferLeaderRequest request;
  dingodb::pb::store::TransferLeaderResponse response;

  request.set_region_id(region_id);
  request.mutable_peer()->CopyFrom(peer);

  interaction->SendRequest("StoreService", "TransferLeader", request, response);
}

void SendTransferLeaderByCoordinator(ServerInteractionPtr interaction, uint64_t region_id, uint64_t leader_store_id) {
  dingodb::pb::coordinator::TransferLeaderRegionRequest request;
  dingodb::pb::coordinator::TransferLeaderRegionResponse response;

  request.set_region_id(region_id);
  request.set_leader_store_id(leader_store_id);

  interaction->SendRequest("CoordinatorService", "TransferLeaderRegion", request, response);
}

std::vector<uint64_t> SendGetTables(ServerInteractionPtr interaction) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  interaction->SendRequest("MetaService", "GetTables", request, response);

  std::vector<uint64_t> table_ids;
  for (const auto& id : response.table_definition_with_ids()) {
    table_ids.push_back(id.table_id().entity_id());
  }

  return table_ids;
}

struct BatchPutGetParam {
  uint64_t region_id;
  int32_t req_num;
  int32_t thread_no;
  std::string prefix;
  ServerInteractionPtr interaction;

  std::map<std::string, std::shared_ptr<dingodb::pb::store::StoreService_Stub>> stubs;
};

void BatchPut(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int req_num) {
  std::vector<uint64_t> latencys;
  for (int i = 0; i < req_num; ++i) {
    std::string key = prefix + Helper::GenRandomStringV2(32);
    std::string value = Helper::GenRandomString(256);
    SendKvPut(interaction, region_id, key, value);
    latencys.push_back(interaction->GetLatency());
  }

  int64_t sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<uint64_t>(0));
  DINGO_LOG(INFO) << "Put average latency: " << sum / latencys.size() << " us";
}

void TestBatchPut(ServerInteractionPtr interaction, uint64_t region_id, int thread_num, int req_num,
                  const std::string& prefix) {
  std::vector<bthread_t> tids;
  tids.resize(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    BatchPutGetParam* param = new BatchPutGetParam;
    param->req_num = req_num;
    param->region_id = region_id;
    param->thread_no = i;
    param->interaction = interaction;
    param->prefix = prefix;

    if (bthread_start_background(
            &tids[i], nullptr,
            [](void* arg) -> void* {
              std::unique_ptr<BatchPutGetParam> param(static_cast<BatchPutGetParam*>(arg));

              LOG(INFO) << "========thread: " << param->thread_no;
              BatchPut(param->interaction, param->region_id, param->prefix, param->req_num);
              return nullptr;
            },
            param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void BatchPutGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int req_num) {
  auto dataset = Helper::GenDataset(prefix, req_num);

  std::vector<uint64_t> latencys;
  latencys.reserve(dataset.size());
  for (auto& [key, value] : dataset) {
    SendKvPut(interaction, region_id, key, value);

    latencys.push_back(interaction->GetLatency());
  }

  int64_t sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<uint64_t>(0));
  DINGO_LOG(INFO) << "Put average latency: " << sum / latencys.size() << " us";

  latencys.clear();
  for (auto& [key, expect_value] : dataset) {
    std::string value;
    SendKvGet(interaction, region_id, key, value);
    if (value != expect_value) {
      DINGO_LOG(INFO) << "Not match: " << key << " = " << value << " expected=" << expect_value;
    }
    latencys.push_back(interaction->GetLatency());
  }

  sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<uint64_t>(0));
  DINGO_LOG(INFO) << "Get average latency: " << sum / latencys.size() << " us";
}

void TestBatchPutGet(ServerInteractionPtr interaction, uint64_t region_id, int thread_num, int req_num,
                     const std::string& prefix) {
  std::vector<bthread_t> tids;
  tids.resize(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    BatchPutGetParam* param = new BatchPutGetParam;
    param->req_num = req_num;
    param->region_id = region_id;
    param->thread_no = i;
    param->interaction = interaction;
    param->prefix = prefix;

    if (bthread_start_background(
            &tids[i], nullptr,
            [](void* arg) -> void* {
              std::unique_ptr<BatchPutGetParam> param(static_cast<BatchPutGetParam*>(arg));

              LOG(INFO) << "========thread: " << param->thread_no;
              BatchPutGet(param->interaction, param->region_id, param->prefix, param->req_num);

              return nullptr;
            },
            param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

struct AddRegionParam {
  uint64_t start_region_id;
  int32_t region_count;
  std::string raft_group;
  int req_num;
  std::string prefix;

  std::vector<std::string> raft_addrs;
  ServerInteractionPtr interaction;
};

void* AdddRegionRoutine(void* arg) {
  std::unique_ptr<AddRegionParam> param(static_cast<AddRegionParam*>(arg));
  auto interaction = param->interaction;
  for (int i = 0; i < param->region_count; ++i) {
    dingodb::pb::store::AddRegionRequest request;
    request.mutable_region()->CopyFrom(
        BuildRegionDefinition(param->start_region_id + i, param->raft_group, param->raft_addrs, "a", "z"));
    dingodb::pb::store::AddRegionResponse response;

    interaction->AllSendRequest("StoreService", "AddRegion", request, response);

    bthread_usleep(3 * 1000 * 1000L);
  }

  return nullptr;
}

void BatchSendAddRegion(ServerInteractionPtr interaction, int start_region_id, int region_count, int thread_num,
                        const std::string& raft_group, std::vector<std::string>& raft_addrs) {
  int32_t step = region_count / thread_num;
  std::vector<bthread_t> tids;
  tids.resize(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    AddRegionParam* param = new AddRegionParam;
    param->start_region_id = start_region_id + i * step;
    param->region_count = (i + 1 == thread_num) ? region_count - i * step : step;
    param->raft_group = raft_group;
    param->interaction = interaction;
    param->raft_addrs = raft_addrs;

    if (bthread_start_background(&tids[i], nullptr, AdddRegionRoutine, param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void* OperationRegionRoutine(void* arg) {
  std::unique_ptr<AddRegionParam> param(static_cast<AddRegionParam*>(arg));
  auto interaction = param->interaction;

  bthread_usleep((Helper::GetRandInt() % 1000) * 1000L);
  for (int i = 0; i < param->region_count; ++i) {
    uint64_t region_id = param->start_region_id + i;

    // Create region
    {
      DINGO_LOG(INFO) << "======Create region " << region_id;
      dingodb::pb::store::AddRegionRequest request;
      request.mutable_region()->CopyFrom(
          BuildRegionDefinition(param->start_region_id + i, param->raft_group, param->raft_addrs, "a", "z"));
      dingodb::pb::store::AddRegionResponse response;

      interaction->AllSendRequest("StoreService", "AddRegion", request, response);
    }

    // Put/Get
    {
      bthread_usleep(3 * 1000 * 1000L);
      DINGO_LOG(INFO) << "======Put region " << region_id;
      BatchPutGet(interaction, region_id, param->prefix, param->req_num);
    }

    // Destroy region
    {
      bthread_usleep(3 * 1000 * 1000L);
      DINGO_LOG(INFO) << "======Delete region " << region_id;
      dingodb::pb::store::DestroyRegionRequest request;
      dingodb::pb::store::DestroyRegionResponse response;

      request.set_region_id(region_id);

      interaction->AllSendRequest("StoreService", "DestroyRegion", request, response);
    }

    bthread_usleep(1 * 1000 * 1000L);
  }

  return nullptr;
}

void TestRegionLifecycle(ServerInteractionPtr interaction, uint64_t region_id, const std::string& raft_group,
                         std::vector<std::string>& raft_addrs, int region_count, int thread_num, int req_num,
                         const std::string& prefix) {
  int32_t step = region_count / thread_num;
  std::vector<bthread_t> tids;
  tids.resize(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    AddRegionParam* param = new AddRegionParam;
    param->start_region_id = region_id + i * step;
    param->region_count = (i + 1 == thread_num) ? region_count - i * step : step;
    param->raft_addrs = raft_addrs;
    param->interaction = interaction;
    param->raft_group = raft_group;
    param->req_num = req_num;
    param->prefix = prefix;

    if (bthread_start_background(&tids[i], nullptr, OperationRegionRoutine, param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void TestDeleteRangeWhenTransferLeader(std::shared_ptr<Context> ctx, uint64_t region_id, int req_num,
                                       const std::string& prefix) {
  // put data
  DINGO_LOG(INFO) << "batch put...";
  BatchPut(ctx->store_interaction, region_id, prefix, req_num);

  // transfer leader
  dingodb::pb::common::Peer new_leader_peer;
  auto region = SendQueryRegion(ctx->coordinator_interaction, region_id);
  for (const auto& peer : region.definition().peers()) {
    if (region.leader_store_id() != peer.store_id()) {
      new_leader_peer = peer;
    }
  }

  DINGO_LOG(INFO) << fmt::format("transfer leader {}:{}", new_leader_peer.raft_location().host(),
                                 new_leader_peer.raft_location().port());
  SendTransferLeader(ctx->store_interaction, region_id, new_leader_peer);

  // delete range
  DINGO_LOG(INFO) << "delete range...";
  SendKvDeleteRange(ctx->store_interaction, region_id, prefix);

  // scan data
  DINGO_LOG(INFO) << "scan...";
  SendKvScan(ctx->store_interaction, region_id, prefix);
}

dingodb::pb::meta::CreateTableRequest BuildCreateTableRequest(const std::string& table_name, int partition_num) {
  dingodb::pb::meta::CreateTableRequest request;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  // string name = 1;
  auto* table_definition = request.mutable_table_definition();
  table_definition->set_name(table_name);

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type(::dingodb::pb::meta::SqlType::SQL_TYPE_INTEGER);
    column->set_element_type(::dingodb::pb::meta::ElementType::ELEM_TYPE_BYTES);
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");
  }

  table_definition->set_version(1);
  table_definition->set_ttl(0);
  table_definition->set_engine(::dingodb::pb::common::Engine::ENG_ROCKSDB);
  auto* prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  auto* partition_rule = table_definition->mutable_table_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  auto* range_partition = partition_rule->mutable_range_partition();

  for (int i = 0; i < partition_num; i++) {
    auto* part_range = range_partition->add_ranges();
    auto* part_range_start = part_range->mutable_start_key();
    part_range_start->assign(std::to_string(i * 100));
    auto* part_range_end = part_range->mutable_end_key();
    part_range_end->assign(std::to_string((i + 1) * 100));
  }

  return request;
}

uint64_t SendCreateTable(ServerInteractionPtr interaction, const std::string& table_name, int partition_num) {
  auto request = BuildCreateTableRequest(table_name, partition_num);

  dingodb::pb::meta::CreateTableResponse response;
  interaction->SendRequest("MetaService", "CreateTable", request, response);

  DINGO_LOG(INFO) << "response=" << response.DebugString();
  return response.table_id().entity_id();
}

dingodb::pb::meta::TableRange SendGetTableRange(ServerInteractionPtr interaction, uint64_t table_id) {
  dingodb::pb::meta::GetTableRangeRequest request;
  dingodb::pb::meta::GetTableRangeResponse response;

  auto* mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  interaction->SendRequest("MetaService", "GetTableRange", request, response);

  return response.table_range();
}

void SendDropTable(ServerInteractionPtr interaction, uint64_t table_id) {
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  auto* mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  interaction->SendRequest("MetaService", "DropTable", request, response);
}

// Create table / Put data / Get data / Destroy table
void* CreateAndPutAndGetAndDestroyTableRoutine(void* arg) {
  std::shared_ptr<Context> ctx(static_cast<Context*>(arg));

  DINGO_LOG(INFO) << "======= Create table " << ctx->table_name;
  uint64_t table_id = SendCreateTable(ctx->coordinator_interaction, ctx->table_name, ctx->partition_num);

  DINGO_LOG(INFO) << "======= Put/Get table " << ctx->table_name;
  int batch_count = ctx->req_num / kBatchSize + 1;
  for (int i = 0; i < batch_count; ++i) {
    auto table_range = SendGetTableRange(ctx->coordinator_interaction, table_id);

    for (const auto& range_dist : table_range.range_distribution()) {
      if (range_dist.leader().host().empty()) {
        bthread_usleep(1 * 1000 * 1000);
        continue;
      }
      uint64_t region_id = range_dist.id().entity_id();
      std::string prefix = range_dist.range().start_key();

      BatchPutGet(ctx->store_interaction, region_id, prefix, kBatchSize);
    }
  }

  DINGO_LOG(INFO) << "======= Drop table " << ctx->table_name;
  SendDropTable(ctx->coordinator_interaction, table_id);

  return nullptr;
}

uint64_t SendGetTableByName(ServerInteractionPtr interaction, const std::string& table_name) {
  dingodb::pb::meta::GetTableByNameRequest request;
  dingodb::pb::meta::GetTableByNameResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_table_name(table_name);

  interaction->SendRequest("MetaService", "GetTableByName", request, response);

  return response.table_definition_with_id().table_id().entity_id();
}

dingodb::pb::common::StoreMap SendGetStoreMap(ServerInteractionPtr interaction) {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(1);

  interaction->SendRequest("CoordinatorService", "GetStoreMap", request, response);

  return response.storemap();
}

dingodb::pb::common::Region SendQueryRegion(ServerInteractionPtr interaction, uint64_t region_id) {
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  request.set_region_id(region_id);

  interaction->SendRequest("CoordinatorService", "QueryRegion", request, response);

  return response.region();
}

void SendChangePeer(ServerInteractionPtr interaction, const dingodb::pb::common::RegionDefinition& region_definition) {
  dingodb::pb::coordinator::ChangePeerRegionRequest request;
  dingodb::pb::coordinator::ChangePeerRegionResponse response;

  auto* mut_definition = request.mutable_change_peer_request()->mutable_region_definition();
  mut_definition->CopyFrom(region_definition);

  interaction->SendRequest("CoordinatorService", "ChangePeerRegion", request, response);
}

void SendSplitRegion(ServerInteractionPtr interaction, const dingodb::pb::common::RegionDefinition& region_definition) {
  dingodb::pb::coordinator::SplitRegionRequest request;
  dingodb::pb::coordinator::SplitRegionResponse response;

  request.mutable_split_request()->set_split_from_region_id(region_definition.id());

  // calc the mid value between start_vec and end_vec
  const auto& start_key = region_definition.range().start_key();
  const auto& end_key = region_definition.range().end_key();

  auto diff = dingodb::Helper::StringSubtract(start_key, end_key);
  auto half_diff = dingodb::Helper::StringDivideByTwo(diff);
  auto mid = dingodb::Helper::StringAdd(start_key, half_diff);
  auto real_mid = mid.substr(1, mid.size() - 1);

  DINGO_LOG(INFO) << fmt::format("split range: [{}, {}) diff: {} half_diff: {} mid: {} real_mid: {}",
                                 dingodb::Helper::StringToHex(start_key), dingodb::Helper::StringToHex(end_key),
                                 dingodb::Helper::StringToHex(diff), dingodb::Helper::StringToHex(half_diff),
                                 dingodb::Helper::StringToHex(mid), dingodb::Helper::StringToHex(real_mid));

  request.mutable_split_request()->set_split_watershed_key(real_mid);

  interaction->SendRequest("CoordinatorService", "SplitRegion", request, response);
}

std::string FormatPeers(dingodb::pb::common::RegionDefinition definition) {
  std::string str;
  for (const auto& peer : definition.peers()) {
    str +=
        fmt::format("{}:{}:{}", peer.raft_location().host(), peer.raft_location().port(), peer.raft_location().index());
    str += ",";
  }

  return str;
}

// Expand/Shrink/Split region
void* AutoExpandAndShrinkAndSplitRegion(void* arg) {
  std::shared_ptr<Context> ctx(static_cast<Context*>(arg));

  for (;;) {
    uint64_t table_id = SendGetTableByName(ctx->coordinator_interaction, ctx->table_name);
    if (table_id == 0) {
      DINGO_LOG(INFO) << fmt::format("table: {} table_id: {}", ctx->table_name, table_id);
      bthread_usleep(1 * 1000 * 1000);
      continue;
    }

    auto store_map = SendGetStoreMap(ctx->coordinator_interaction);
    auto table_range = SendGetTableRange(ctx->coordinator_interaction, table_id);

    // Traverse region
    for (const auto& range_dist : table_range.range_distribution()) {
      uint64_t region_id = range_dist.id().entity_id();
      if (region_id == 0) {
        DINGO_LOG(INFO) << fmt::format("Get table range failed, table: {} region_id: {}", ctx->table_name, region_id);
        continue;
      }

      auto region = SendQueryRegion(ctx->coordinator_interaction, region_id);
      if (region.id() == 0) {
        DINGO_LOG(INFO) << fmt::format("Get region failed, table: {} region_id: {}", ctx->table_name, region_id);
        continue;
      }

      DINGO_LOG(INFO) << fmt::format("region {} state {} row_count {} min_key {} max_key {} region_size {}", region_id,
                                     static_cast<int>(region.state()), region.metrics().row_count(),
                                     dingodb::Helper::StringToHex(region.metrics().min_key()),
                                     dingodb::Helper::StringToHex(region.metrics().max_key()),
                                     region.metrics().region_size());
      if (region.state() != dingodb::pb::common::RegionState::REGION_NORMAL) {
        continue;
      }

      // Expand region
      if (Helper::RandomChoice()) {
        // Traverse store, get add new peer.
        dingodb::pb::common::Peer expand_peer;
        for (const auto& store : store_map.stores()) {
          bool is_exist = false;
          for (const auto& peer : region.definition().peers()) {
            if (store.id() == peer.store_id()) {
              is_exist = true;
            }
          }

          // Store not exist at the raft group, may add peer.
          if (!is_exist && Helper::RandomChoice()) {
            expand_peer.set_store_id(store.id());
            expand_peer.set_role(dingodb::pb::common::PeerRole::VOTER);
            expand_peer.mutable_server_location()->CopyFrom(store.server_location());
            expand_peer.mutable_raft_location()->CopyFrom(store.raft_location());
            break;
          }
        }

        // Add new peer.
        if (expand_peer.store_id() != 0) {
          dingodb::pb::common::RegionDefinition region_definition;
          region_definition.CopyFrom(region.definition());
          *region_definition.add_peers() = expand_peer;
          DINGO_LOG(INFO) << fmt::format("======= Expand region {}/{} region {} peers {}", ctx->table_name, table_id,
                                         region.id(), FormatPeers(region_definition));

          // SendChangePeer(ctx->coordinator_interaction, region_definition);
        }
      } else {  // Shrink region
        if (region.definition().peers().size() <= 3) {
          continue;
        }

        dingodb::pb::common::Peer shrink_peer;
        for (const auto& peer : region.definition().peers()) {
          if (peer.store_id() != region.leader_store_id() && Helper::RandomChoice()) {
            shrink_peer = peer;
            break;
          }
        }

        if (shrink_peer.store_id() != 0) {
          dingodb::pb::common::RegionDefinition region_definition;
          region_definition.CopyFrom(region.definition());
          region_definition.mutable_peers()->Clear();
          for (const auto& peer : region.definition().peers()) {
            if (peer.store_id() != shrink_peer.store_id()) {
              *region_definition.add_peers() = peer;
            }
          }

          DINGO_LOG(INFO) << fmt::format("======= Shrink region {}/{} region {} peers {}", ctx->table_name, table_id,
                                         region.id(), FormatPeers(region_definition));

          // SendChangePeer(ctx->coordinator_interaction, region.definition());
        }
      }

      // Split region, when row count greater than 1 million.
      if (region.metrics().row_count() > 1 * 1000 * 1000) {
        SendSplitRegion(ctx->coordinator_interaction, region.definition());
      }
    }

    bthread_usleep(1 * 1000 * 1000);
  }

  return nullptr;
}

void AutoTest(std::shared_ptr<Context> ctx) {
  std::vector<std::function<void*(void*)>> funcs = {CreateAndPutAndGetAndDestroyTableRoutine,
                                                    AutoExpandAndShrinkAndSplitRegion};
  std::vector<bthread_t> tids;
  tids.resize(funcs.size());

  // Thread: Create table / Put data / Get data / Destroy table
  // Thread: Expand/Shrink/Split region
  // Thread: Random kill/launch Node
  for (int i = 0; i < funcs.size(); ++i) {
    int ret = bthread_start_background(&tids[i], nullptr, *funcs[i].target<void* (*)(void*)>(), ctx->Clone().release());
    if (ret != 0) {
      DINGO_LOG(ERROR) << "Create bthread failed, ret: " << ret;
      return;
    }
  }

  for (auto& tid : tids) {
    bthread_join(tid, nullptr);
  }
}

void AutoDropTable(std::shared_ptr<Context> ctx) {
  // Get all table
  auto table_ids = SendGetTables(ctx->coordinator_interaction);
  DINGO_LOG(INFO) << "table nums: " << table_ids.size();

  // Drop table
  std::sort(table_ids.begin(), table_ids.end());
  for (int i = 0; i < ctx->req_num && i < table_ids.size(); ++i) {
    DINGO_LOG(INFO) << "Delete table: " << table_ids[i];
    SendDropTable(ctx->coordinator_interaction, table_ids[i]);
  }
}

}  // namespace client