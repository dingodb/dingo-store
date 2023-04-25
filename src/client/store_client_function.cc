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

#include <cstdint>
#include <string>

namespace client {

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

struct BatchPutGetParam {
  uint64_t region_id;
  int32_t req_num;
  int32_t thread_no;
  std::string prefix;
  ServerInteractionPtr interaction;

  std::map<std::string, std::shared_ptr<dingodb::pb::store::StoreService_Stub>> stubs;
};

void* BatchPutGetRoutine(void* arg) {
  std::unique_ptr<BatchPutGetParam> param(static_cast<BatchPutGetParam*>(arg));

  DINGO_LOG(INFO) << "========thread: " << param->thread_no;
  BatchPutGet(param->interaction, param->region_id, param->prefix, param->req_num);

  return nullptr;
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

    if (bthread_start_background(&tids[i], nullptr, BatchPutGetRoutine, param) != 0) {
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

}  // namespace client