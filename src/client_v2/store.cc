
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

#include "store.h"

#include <cstdint>
#include <iostream>
#include <string>

#include "bthread/bthread.h"
#include "client_v2/dump.h"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "client_v2/router.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/serial_helper.h"
#include "common/version.h"
#include "document/codec.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/document.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/store.pb.h"
#include "proto/version.pb.h"
#include "serial/buf.h"
#include "serial/schema/long_schema.h"
#include "vector/codec.h"

const int kBatchSize = 1000;

namespace client_v2 {
void SetUpStoreSubCommands(CLI::App& app) {
  SetUpKvGet(app);
  SetUpKvPut(app);
  SetUpTxnDump(app);
  SetUpSnapshot(app);
  SetUpRegionMetrics(app);
}
dingodb::pb::common::RegionDefinition BuildRegionDefinitionV2(int64_t region_id, const std::string& raft_group,
                                                              std::vector<std::string>& raft_addrs,
                                                              const std::string& start_key,
                                                              const std::string& end_key) {
  dingodb::pb::common::RegionDefinition region_definition;
  region_definition.set_id(region_id);
  region_definition.mutable_epoch()->set_conf_version(1);
  region_definition.mutable_epoch()->set_version(1);
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

void SetUpAddRegion(CLI::App& app) {
  auto opt = std::make_shared<AddRegionOptions>();
  auto coor = app.add_subcommand("AddRegion", "Add Region")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--raft_addrs", opt->raft_addrs,
                   "example --raft_addr:127.0.0.1:10101:0,127.0.0.1:10102:0,127.0.0.1:10103:0")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--raft_group", opt->raft_group, "Request parameter raft_group")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter raft_group")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunAddRegion(*opt); });
}

void RunAddRegion(AddRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  std::string value;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);
  dingodb::pb::debug::AddRegionRequest request;
  *(request.mutable_region()) = BuildRegionDefinitionV2(opt.region_id, opt.raft_group, raft_addrs, "a", "z");
  dingodb::pb::debug::AddRegionResponse response;

  InteractionManager::GetInstance().AllSendRequestWithoutContext("DebugService", "AddRegion", request, response);
}

void SetUpKvGet(CLI::App& app) {
  auto opt = std::make_shared<KvGetOptions>();
  auto coor = app.add_subcommand("KvGet", "Kv get")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvGet(*opt); });
}

void RunKvGet(KvGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  std::string value;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvGet(opt, value);
}

void SetUpKvPut(CLI::App& app) {
  auto opt = std::make_shared<KvPutOptions>();
  auto coor = app.add_subcommand("KvPut", "Kv put")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--value", opt->value, "Request parameter value")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvPut(*opt); });
}

void RunKvPut(KvPutOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  std::string value = opt.value.empty() ? client_v2::Helper::GenRandomString(256) : opt.value;
  client_v2::SendKvPut(opt, value);
}

void SetUpChangeRegion(CLI::App& app) {
  auto opt = std::make_shared<ChangeRegionOptions>();
  auto coor = app.add_subcommand("ChangeRegion", "Change region")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--raft_group", opt->raft_group, "Request parameter raft_group")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--raft_addrs", opt->raft_addrs, "Request parameter value")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunChangeRegion(*opt); });
}

void RunChangeRegion(ChangeRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendChangeRegion(opt);
}

void SetUpMergeRegionAtStore(CLI::App& app) {
  auto opt = std::make_shared<MergeRegionAtStoreOptions>();
  auto coor = app.add_subcommand("MergeRegionAtStore", "Merge region at store ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--source_id", opt->source_id, "Request parameter source region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--target_id", opt->target_id, "Request parameter target region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunMergeRegionAtStore(*opt); });
}

void RunMergeRegionAtStore(MergeRegionAtStoreOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, 0, opt.source_id)) {
    exit(-1);
  }
  client_v2::SendMergeRegion(opt);
}

void SetUpDestroyRegion(CLI::App& app) {
  auto opt = std::make_shared<DestroyRegionOptions>();
  auto coor = app.add_subcommand("DestroyRegion", "Destroy region")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDestroyRegion(*opt); });
}

void RunDestroyRegion(DestroyRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDestroyRegion(opt);
}

void SetUpSnapshot(CLI::App& app) {
  auto opt = std::make_shared<SnapshotOptions>();
  auto coor = app.add_subcommand("Snapshot", "Snapshot")->group("Region Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  coor->callback([opt]() { RunSnapshot(*opt); });
}

void RunSnapshot(SnapshotOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendSnapshot(opt);
}

void SetUpBatchAddRegion(CLI::App& app) {
  auto opt = std::make_shared<BatchAddRegionOptions>();
  auto coor = app.add_subcommand("BatchAddRegion", "Batch add region")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");

  coor->add_option("--region_count", opt->region_count, "Request parameter region id")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->add_option("--thread_num", opt->thread_num, "Request parameter region id")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->add_option("--raft_group", opt->raft_group, "Request parameter raft_group")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--raft_addrs", opt->raft_addrs, "Request parameter value")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunBatchAddRegion(*opt); });
}

void RunBatchAddRegion(BatchAddRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::BatchSendAddRegion(opt);
}

void SetUpSnapshotVectorIndex(CLI::App& app) {
  auto opt = std::make_shared<SnapshotVectorIndexOptions>();
  auto coor = app.add_subcommand("SnapshotVectorIndex", "Snapshot vector index")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSnapshotVectorIndex(*opt); });
}

void RunSnapshotVectorIndex(SnapshotVectorIndexOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendSnapshotVectorIndex(opt);
}

void SetUpCompact(CLI::App& app) {
  auto opt = std::make_shared<CompactOptions>();
  auto coor = app.add_subcommand("Compact", "Compact ")->group("Store Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunCompact(*opt); });
}

void RunCompact(CompactOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    exit(-1);
  }
  client_v2::SendCompact("");
}

void SetUpGetMemoryStats(CLI::App& app) {
  auto opt = std::make_shared<GetMemoryStatsOptions>();
  auto coor = app.add_subcommand("GetMemoryStats", "GetMemory stats ")->group("Store Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunGetMemoryStats(*opt); });
}

void RunGetMemoryStats(GetMemoryStatsOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    exit(-1);
  }
  client_v2::GetMemoryStats();
}

void SetUpReleaseFreeMemory(CLI::App& app) {
  auto opt = std::make_shared<ReleaseFreeMemoryOptions>();
  auto coor = app.add_subcommand("ReleaseFreeMemory", "Release free memory ")->group("Store Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--rate", opt->rate, "server addrs")->default_val(0.0)->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunReleaseFreeMemory(*opt); });
}

void RunReleaseFreeMemory(ReleaseFreeMemoryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    exit(-1);
  }
  client_v2::ReleaseFreeMemory(opt);
}

void SetUpKvBatchGet(CLI::App& app) {
  auto opt = std::make_shared<KvBatchGetOptions>();
  auto coor = app.add_subcommand("KvBatchGet", "Kv batch get")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--prefix", opt->prefix, "Request parameter prefix")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--req_num", opt->req_num, "Request parameter region id")
      ->default_val(1)
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { RunKvBatchGet(*opt); });
}

void RunKvBatchGet(KvBatchGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvBatchGet(opt);
}

void SetUpKvBatchPut(CLI::App& app) {
  auto opt = std::make_shared<KvBatchPutOptions>();
  auto coor = app.add_subcommand("KvBatchPut", "Kv batch put")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--prefix", opt->prefix, "Request parameter prefix")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--count", opt->count, "Request parameter region id")
      ->default_val(50)
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { RunKvBatchPut(*opt); });
}

void RunKvBatchPut(KvBatchPutOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvBatchPut(opt);
}

void SetUpKvPutIfAbsent(CLI::App& app) {
  auto opt = std::make_shared<KvPutIfAbsentOptions>();
  auto coor = app.add_subcommand("KvPutIfAbsent", "Kv put if absent")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter prefix")->required()->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvPutIfAbsent(*opt); });
}

void RunKvPutIfAbsent(KvPutIfAbsentOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvPutIfAbsent(opt);
}

void SetUpKvBatchPutIfAbsent(CLI::App& app) {
  auto opt = std::make_shared<KvBatchPutIfAbsentOptions>();
  auto coor = app.add_subcommand("KvBatchPutIfAbsent", "Kv batch put if absent")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--prefix", opt->prefix, "Request parameter prefix")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--count", opt->count, "Request parameter region id")
      ->default_val(50)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvBatchPutIfAbsent(*opt); });
}

void RunKvBatchPutIfAbsent(KvBatchPutIfAbsentOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvBatchPutIfAbsent(opt);
}

void SetUpKvBatchDelete(CLI::App& app) {
  auto opt = std::make_shared<KvBatchDeleteOptions>();
  auto coor = app.add_subcommand("KvBatchDelete", "Kv batch delete")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvBatchDelete(*opt); });
}

void RunKvBatchDelete(KvBatchDeleteOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvBatchDelete(opt);
}

void SetUpKvDeleteRange(CLI::App& app) {
  auto opt = std::make_shared<KvDeleteRangeOptions>();
  auto coor = app.add_subcommand("KvDeleteRange", "Kv delete range")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--prefix", opt->prefix, "Request parameter prefix")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvDeleteRange(*opt); });
}

void RunKvDeleteRange(KvDeleteRangeOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvDeleteRange(opt);
}

void SetUpKvScan(CLI::App& app) {
  auto opt = std::make_shared<KvScanOptions>();
  auto coor = app.add_subcommand("KvScan", "Kv scan")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--prefix", opt->prefix, "Request parameter prefix")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvScan(*opt); });
}

void RunKvScan(KvScanOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvScan(opt);
}

void SetUpKvCompareAndSet(CLI::App& app) {
  auto opt = std::make_shared<KvCompareAndSetOptions>();
  auto coor = app.add_subcommand("KvCompareAndSet", "Kv compare and set")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvCompareAndSet(*opt); });
}

void RunKvCompareAndSet(KvCompareAndSetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvCompareAndSet(opt);
}

void SetUpKvBatchCompareAndSet(CLI::App& app) {
  auto opt = std::make_shared<KvBatchCompareAndSetOptions>();
  auto coor = app.add_subcommand("KvBatchCompareAndSet", "Kv batch compare and set")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--prefix", opt->prefix, "Request parameter prefix")
      ->required()
      ->group("Coordinator Manager Commands");
  ;
  coor->add_option("--count", opt->count, "Request parameter count")
      ->default_val(100)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvBatchCompareAndSet(*opt); });
}

void RunKvBatchCompareAndSet(KvBatchCompareAndSetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendKvBatchCompareAndSet(opt);
}

void SetUpKvScanBeginV2(CLI::App& app) {
  auto opt = std::make_shared<KvScanBeginV2Options>();
  auto coor = app.add_subcommand("KvScanBeginV2", "Kv scan beginV2")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");

  coor->add_option("--scan_id", opt->scan_id, "Request parameter scan id")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvScanBeginV2(*opt); });
}

void RunKvScanBeginV2(KvScanBeginV2Options const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvScanBeginV2(opt);
}

void SetUpKvScanContinueV2(CLI::App& app) {
  auto opt = std::make_shared<KvScanContinueV2Options>();
  auto coor = app.add_subcommand("KvScanContinueV2", "Kv scan continueV2 ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");

  coor->add_option("--scan_id", opt->scan_id, "Request parameter scan id")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvScanContinueV2(*opt); });
}

void RunKvScanContinueV2(KvScanContinueV2Options const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvScanContinueV2(opt);
}

void SetUpKvScanReleaseV2(CLI::App& app) {
  auto opt = std::make_shared<KvScanReleaseV2Options>();
  auto coor = app.add_subcommand("KvScanReleaseV2", "Kv scan releaseV2 ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");

  coor->add_option("--scan_id", opt->scan_id, "Request parameter scan id")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunKvScanReleaseV2(*opt); });
}

void RunKvScanReleaseV2(KvScanReleaseV2Options const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendKvScanReleaseV2(opt);
}

void SetUpTxnGet(CLI::App& app) {
  auto opt = std::make_shared<TxnGetOptions>();
  auto coor = app.add_subcommand("TxnGet", "Txn get ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "key is hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--resolve_locks", opt->resolve_locks, "Request parameter resolve_locks")
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnGet(*opt); });
}

void RunTxnGet(TxnGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnGet(opt);
}

void SetUpTxnScan(CLI::App& app) {
  auto opt = std::make_shared<TxnScanOptions>();
  auto coor = app.add_subcommand("TxnGet", "Txn scan")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--start_key", opt->start_key, "Request parameter start_key")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--end_key", opt->end_key, "Request parameter start_key")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--limit", opt->limit, "Request parameter limit")
      ->default_val(50)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--is_reverse", opt->is_reverse, "Request parameter is_reverse ")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--key_only", opt->key_only, "Request parameter key_only")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--resolve_locks", opt->resolve_locks, "Request parameter resolve_locks")
      ->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_start", opt->with_start, "Request parameter with_start")
      ->default_val(true)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_end", opt->with_end, "Request parameter with_end")
      ->default_val(true)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnScan(*opt); });
}

void RunTxnScan(TxnScanOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnScan(opt);
}

void SetUpTxnPessimisticLock(CLI::App& app) {
  auto opt = std::make_shared<TxnPessimisticLockOptions>();
  auto coor = app.add_subcommand("TxnPessimisticLock", "Txn pessimistic lock")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--primary_lock", opt->primary_lock, "Request parameter primary_lock")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--lock_ttl", opt->lock_ttl, "Request parameter lock_ttl")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--for_update_ts", opt->for_update_ts, "Request parameter for_update_ts ")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--mutation_op", opt->mutation_op, "Request parameter mutation_op must be one of [lock]")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--value", opt->value, "Request parameter with_start")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--value_is_hex", opt->value_is_hex, "Request parameter value_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnPessimisticLock(*opt); });
}

void RunTxnPessimisticLock(TxnPessimisticLockOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnPessimisticLock(opt);
}

void SetUpTxnPessimisticRollback(CLI::App& app) {
  auto opt = std::make_shared<TxnPessimisticRollbackOptions>();
  auto coor = app.add_subcommand("TxnPessimisticLock", "Txn pessimistic lock")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--for_update_ts", opt->for_update_ts, "Request parameter for_update_ts ")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnPessimisticRollback(*opt); });
}

void RunTxnPessimisticRollback(TxnPessimisticRollbackOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnPessimisticRollback(opt);
}

void SetUpTxnPrewrite(CLI::App& app) {
  auto opt = std::make_shared<TxnPrewriteOptions>();
  auto coor = app.add_subcommand("TxnPrewrite", "Txn prewrite")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--primary_lock", opt->primary_lock, "Request parameter primary_lock")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--lock_ttl", opt->lock_ttl, "Request parameter lock_ttl")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--txn_size", opt->txn_size, "Request parameter txn_size")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--try_one_pc", opt->try_one_pc, "Request parameter try_one_pc")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--max_commit_ts", opt->max_commit_ts, "Request parameter max_commit_ts ")
      ->default_val(0)
      ->group("Coordinator Manager Commands");
  coor->add_option("--mutation_op", opt->mutation_op,
                   "Request parameter mutation_op must be one of [put, delete, insert]")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--key2", opt->key2, "Request parameter key2")->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--value", opt->value, "Request parameter value2")->group("Coordinator Manager Commands");
  coor->add_option("--value2", opt->value2, "Request parameter value2")->group("Coordinator Manager Commands");
  coor->add_flag("--value_is_hex", opt->value_is_hex, "Request parameter value_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--extra_data", opt->extra_data, "Request parameter extra_data ")
      ->group("Coordinator Manager Commands");
  coor->add_option("--for_update_ts", opt->for_update_ts, "Request parameter for_update_ts ")
      ->group("Coordinator Manager Commands");
  coor->add_option("--vector_id", opt->vector_id, "Request parameter vector_id ")
      ->group("Coordinator Manager Commands");
  coor->add_option("--document_id", opt->document_id, "Request parameter document_id ")
      ->group("Coordinator Manager Commands");
  coor->add_option("--document_text1", opt->document_text1, "Request parameter document_text1 ")
      ->group("Coordinator Manager Commands");
  coor->add_option("--document_text2", opt->document_text2, "Request parameter document_text2 ")
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { RunTxnPrewrite(*opt); });
}

void RunTxnPrewrite(TxnPrewriteOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnPrewrite(opt);
}

void SetUpTxnCommit(CLI::App& app) {
  auto opt = std::make_shared<TxnCommitOptions>();
  auto coor = app.add_subcommand("TxnCommit", "Txn commit")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--commit_ts", opt->commit_ts, "Request parameter commit_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--key2", opt->key2, "Request parameter key2")->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnCommit(*opt); });
}

void RunTxnCommit(TxnCommitOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnCommit(opt);
}

void SetUpTxnCheckTxnStatus(CLI::App& app) {
  auto opt = std::make_shared<TxnCheckTxnStatusOptions>();
  auto coor = app.add_subcommand("TxnCheckTxnStatus", "Txn check txn status")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--primary_key", opt->primary_key, "Request parameter primary_key")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--lock_ts", opt->lock_ts, "Request parameter lock_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--caller_start_ts", opt->caller_start_ts, "Request parameter caller_start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--current_ts", opt->current_ts, "Request parameter current_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnCheckTxnStatus(*opt); });
}

void RunTxnCheckTxnStatus(TxnCheckTxnStatusOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnCheckTxnStatus(opt);
}

void SetUpTxnResolveLock(CLI::App& app) {
  auto opt = std::make_shared<TxnResolveLockOptions>();
  auto coor = app.add_subcommand("TxnResolveLock", "Txn resolve lock ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--commit_ts", opt->commit_ts, "Request parameter commit_ts, if commmit=0, will do rollback")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key,
                   "Request parameter key, if key is empty, will do resolve lock for all keys of this transaction")
      ->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnResolveLock(*opt); });
}

void RunTxnResolveLock(TxnResolveLockOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnResolveLock(opt);
}

void SetUpTxnBatchGet(CLI::App& app) {
  auto opt = std::make_shared<TxnBatchGetOptions>();
  auto coor = app.add_subcommand("TxnBatchGet", "Txn batch get ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--resolve_locks", opt->resolve_locks, "Request parameter resolve_locks")
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--key2", opt->key2, "Request parameter key2")->required()->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { RunTxnBatchGet(*opt); });
}

void RunTxnBatchGet(TxnBatchGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnBatchGet(opt);
}

void SetUpTxnBatchRollback(CLI::App& app) {
  auto opt = std::make_shared<TxnBatchRollbackOptions>();
  auto coor = app.add_subcommand("TxnBatchRollback", "Txn batch rollback ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--key2", opt->key2, "Request parameter key2")->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnBatchRollback(*opt); });
}

void RunTxnBatchRollback(TxnBatchRollbackOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnBatchRollback(opt);
}

void SetUpTxnScanLock(CLI::App& app) {
  auto opt = std::make_shared<TxnScanLockOptions>();
  auto coor = app.add_subcommand("TxnScanLock", "Txn scan lock ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--max_ts", opt->max_ts, "Request parameter max_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_key", opt->start_key, "Request parameter start_key")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--end_key", opt->end_key, "Request parameter end_key")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--limit", opt->limit, "Request parameter limit")
      ->default_val(50)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnScanLock(*opt); });
}

void RunTxnScanLock(TxnScanLockOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnScanLock(opt);
}

void SetUpTxnHeartBeat(CLI::App& app) {
  auto opt = std::make_shared<TxnHeartBeatOptions>();
  auto coor = app.add_subcommand("TxnHeartBeat", "Txn heart beat ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--primary_lock", opt->primary_lock, "Request parameter primary_lock")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--advise_lock_ttl", opt->advise_lock_ttl, "Request parameter advise_lock_ttl")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnHeartBeat(*opt); });
}

void RunTxnHeartBeat(TxnHeartBeatOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnHeartBeat(opt);
}

void SetUpTxnGC(CLI::App& app) {
  auto opt = std::make_shared<TxnGCOptions>();
  auto coor = app.add_subcommand("TxnGC", "Txn gc ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--safe_point_ts", opt->safe_point_ts, "Request parameter safe_point_ts")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnGC(*opt); });
}

void RunTxnGC(TxnGCOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnGc(opt);
}

void SetUpTxnDeleteRange(CLI::App& app) {
  auto opt = std::make_shared<TxnDeleteRangeOptions>();
  auto coor = app.add_subcommand("TxnDeleteRange", "Txn delete range ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_option("--start_key", opt->start_key, "Request parameter start_key")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--end_key", opt->end_key, "Request parameter end_key")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTxnDeleteRange(*opt); });
}

void RunTxnDeleteRange(TxnDeleteRangeOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnDeleteRange(opt);
}

void SetUpTxnDump(CLI::App& app) {
  auto opt = std::make_shared<TxnDumpOptions>();
  auto coor = app.add_subcommand("TxnDump", "Txn dump")->group("Region Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  coor->add_option("--start_key", opt->start_key, "Request parameter start_key")->required();
  coor->add_option("--end_key", opt->end_key, "Request parameter end_key")->required();
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")->default_val(false);
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  coor->add_option("--end_ts", opt->end_ts, "Request parameter end_ts")->required();
  coor->callback([opt]() { RunTxnDump(*opt); });
}

void RunTxnDump(TxnDumpOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendTxnDump(opt);
}

void SetUpDocumentDelete(CLI::App& app) {
  auto opt = std::make_shared<DocumentDeleteOptions>();
  auto coor = app.add_subcommand("DocumentDelete", "Document delete ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_id", opt->start_id, "Request parameter start id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--count", opt->count, "Request parameter start id")
      ->default_val(50)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDocumentDelete(*opt); });
}

void RunDocumentDelete(DocumentDeleteOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDocumentDelete(opt);
}

void SetUpDocumentAdd(CLI::App& app) {
  auto opt = std::make_shared<DocumentAddOptions>();
  auto coor = app.add_subcommand("DocumentAdd", "Document add ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--document_id", opt->document_id, "Request parameter document_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--document_text1", opt->document_text1, "Request parameter document_text1")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--document_text2", opt->document_text2, "Request parameter document_text2")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--is_update", opt->is_update, "Request parameter is_update")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDocumentAdd(*opt); });
}

void RunDocumentAdd(DocumentAddOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDocumentAdd(opt);
}

void SetUpDocumentSearch(CLI::App& app) {
  auto opt = std::make_shared<DocumentSearchOptions>();
  auto coor = app.add_subcommand("DocumentSearch", "Document search ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--query_string", opt->query_string, "Request parameter query_string")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--topn", opt->topn, "Request parameter topn")->required()->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDocumentSearch(*opt); });
}

void RunDocumentSearch(DocumentSearchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDocumentSearch(opt);
}

void SetUpDocumentBatchQuery(CLI::App& app) {
  auto opt = std::make_shared<DocumentBatchQueryOptions>();
  auto coor = app.add_subcommand("DocumentBatchQuery", "Document batch query ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--document_id", opt->document_id, "Request parameter document id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDocumentBatchQuery(*opt); });
}

void RunDocumentBatchQuery(DocumentBatchQueryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDocumentBatchQuery(opt);
}

void SetUpDocumentScanQuery(CLI::App& app) {
  auto opt = std::make_shared<DocumentScanQueryOptions>();
  auto coor = app.add_subcommand("DocumentScanQuery", "Document scan query ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_id", opt->start_id, "Request parameter start id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--end_id", opt->end_id, "Request parameter end id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--limit", opt->limit, "Request parameter limit")
      ->default_val(50)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--is_reverse", opt->is_reverse, "Request parameter is_reverse")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDocumentScanQuery(*opt); });
}

void RunDocumentScanQuery(DocumentScanQueryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDocumentScanQuery(opt);
}

void SetUpDocumentGetMaxId(CLI::App& app) {
  auto opt = std::make_shared<DocumentGetMaxIdOptions>();
  auto coor = app.add_subcommand("DocumentGetMaxId", "Document get max id ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDocumentGetMaxId(*opt); });
}

void RunDocumentGetMaxId(DocumentGetMaxIdOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDocumentGetMaxId(opt);
}

void SetUpDocumentGetMinId(CLI::App& app) {
  auto opt = std::make_shared<DocumentGetMinIdOptions>();
  auto coor = app.add_subcommand("DocumentGetMinId", "Document get min id ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDocumentGetMinId(*opt); });
}

void RunDocumentGetMinId(DocumentGetMinIdOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDocumentGetMinId(opt);
}

void SetUpDocumentCount(CLI::App& app) {
  auto opt = std::make_shared<DocumentCountOptions>();
  auto coor = app.add_subcommand("DocumentCount", "Document count ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_id", opt->start_id, "Request parameter start id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--end_id", opt->end_id, "Request parameter end id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDocumentCount(*opt); });
}

void RunDocumentCount(DocumentCountOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDocumentCount(opt);
}

void SetUpDocumentGetRegionMetrics(CLI::App& app) {
  auto opt = std::make_shared<DocumentGetRegionMetricsOptions>();
  auto coor =
      app.add_subcommand("DocumentGetRegionMetrics", "Document get region metrics ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDocumentGetRegionMetrics(*opt); });
}

void RunDocumentGetRegionMetrics(DocumentGetRegionMetricsOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendDocumentGetRegionMetrics(opt);
}

void SetUpVectorSearch(CLI::App& app) {
  auto opt = std::make_shared<VectorSearchOptions>();
  auto coor = app.add_subcommand("VectorSearch", "Vector search ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--dimension", opt->dimension, "Request parameter dimension")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--topn", opt->topn, "Request parameter topn")->required()->group("Coordinator Manager Commands");
  coor->add_option("--vector_data", opt->vector_data, "Request parameter vector data")
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->group("Coordinator Manager Commands");
  coor->add_flag("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_table_pre_filter", opt->with_table_pre_filter, "Search vector with table data pre filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_key", opt->scalar_filter_key, "Request scalar_filter_key")
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_value", opt->scalar_filter_value, "Request parameter scalar_filter_value")
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_key2", opt->scalar_filter_key2, "Request parameter scalar_filter_key2")
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_value2", opt->scalar_filter_value2, "Request parameter scalar_filter_value2")
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_scalar_post_filter", opt->with_scalar_post_filter,
                 "Search vector with scalar data post filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--bruteforce", opt->bruteforce, "Use bruteforce search")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--print_vector_search_delay", opt->print_vector_search_delay, "print vector search delay")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--ef_search", opt->ef_search, "hnsw index search ef")
      ->default_val(0)
      ->group("Coordinator Manager Commands");
  coor->add_option("--csv_output", opt->csv_output, "csv output")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorSearch(*opt); });
}

void RunVectorSearch(VectorSearchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendVectorSearch(opt);
}

void SetUpVectorSearchDebug(CLI::App& app) {
  auto opt = std::make_shared<VectorSearchDebugOptions>();
  auto coor = app.add_subcommand("VectorSearchDebug", "Vector search debug")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--dimension", opt->dimension, "Request parameter dimension")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--topn", opt->topn, "Request parameter topn")->required()->group("Coordinator Manager Commands");
  coor->add_option("--start_vector_id", opt->start_vector_id, "Request parameter start_vector_id")
      ->group("Coordinator Manager Commands");
  coor->add_option("--batch_count", opt->batch_count, "Request parameter batch count")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->group("Coordinator Manager Commands");
  coor->add_option("--value", opt->value, "Request parameter value")->group("Coordinator Manager Commands");
  coor->add_flag("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--vector_ids_count", opt->vector_ids_count, "Request parameter vector_ids_count")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");

  coor->add_flag("--with_scalar_post_filter", opt->with_scalar_post_filter,
                 "Search vector with scalar data post filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { RunVectorSearchDebug(*opt); });
}

void RunVectorSearchDebug(VectorSearchDebugOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendVectorSearchDebug(opt);
}

void SetUpVectorRangeSearch(CLI::App& app) {
  auto opt = std::make_shared<VectorRangeSearchOptions>();
  auto coor = app.add_subcommand("VectorRangeSearch", "Vector range search ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--dimension", opt->dimension, "Request parameter dimension")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--radius", opt->radius, "Request parameter radius")
      ->default_val(10.1)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->group("Coordinator Manager Commands");
  coor->add_flag("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_scalar_post_filter", opt->with_scalar_post_filter,
                 "Search vector with scalar data post filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--print_vector_search_delay", opt->print_vector_search_delay, "print vector search delay")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorRangeSearch(*opt); });
}

void RunVectorRangeSearch(VectorRangeSearchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendVectorRangeSearch(opt);
}

void SetUpVectorRangeSearchDebug(CLI::App& app) {
  auto opt = std::make_shared<VectorRangeSearchDebugOptions>();
  auto coor =
      app.add_subcommand("VectorRangeSearchDebug", "Vector range search debug")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--dimension", opt->dimension, "Request parameter dimension")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_vector_id", opt->start_vector_id, "Request parameter start_vector_id")
      ->group("Coordinator Manager Commands");
  coor->add_option("--batch_count", opt->batch_count, "Request parameter batch_count")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--radius", opt->radius, "Request parameter radius")
      ->default_val(10.1)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->group("Coordinator Manager Commands");
  coor->add_option("--value", opt->value, "Request parameter value")->group("Coordinator Manager Commands");
  coor->add_flag("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--vector_ids_count", opt->vector_ids_count, "Search vector with vector ids count")
      ->default_val(100)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_scalar_post_filter", opt->with_scalar_post_filter,
                 "Search vector with scalar data post filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--print_vector_search_delay", opt->print_vector_search_delay, "print vector search delay")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorRangeSearchDebug(*opt); });
}

void RunVectorRangeSearchDebug(VectorRangeSearchDebugOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendVectorRangeSearchDebug(opt);
}

void SetUpVectorBatchSearch(CLI::App& app) {
  auto opt = std::make_shared<VectorBatchSearchOptions>();
  auto coor = app.add_subcommand("VectorBatchSearch", "Vector batch search")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--dimension", opt->dimension, "Request parameter dimension")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--topn", opt->topn, "Request parameter topn")->required()->group("Coordinator Manager Commands");
  coor->add_option("--batch_count", opt->batch_count, "Request parameter batch_count")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->group("Coordinator Manager Commands");
  coor->add_flag("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--with_scalar_post_filter", opt->with_scalar_post_filter,
                 "Search vector with scalar data post filter")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--print_vector_search_delay", opt->print_vector_search_delay, "print vector search delay")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorBatchSearch(*opt); });
}

void RunVectorBatchSearch(VectorBatchSearchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendVectorBatchSearch(opt);
}

void SetUpVectorBatchQuery(CLI::App& app) {
  auto opt = std::make_shared<VectorBatchQueryOptions>();
  auto coor = app.add_subcommand("VectorBatchQuery", "Vector batch query")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--vector_ids", opt->vector_ids, "Request parameter vector_ids")
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->group("Coordinator Manager Commands");
  coor->add_flag("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorBatchQuery(*opt); });
}

void RunVectorBatchQuery(VectorBatchQueryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendVectorBatchQuery(opt);
}

void SetUpVectorScanQuery(CLI::App& app) {
  auto opt = std::make_shared<VectorScanQueryOptions>();
  auto coor = app.add_subcommand("VectorScanQuery", "Vector scan query ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");

  coor->add_option("--start_id", opt->start_id, "Request parameter start_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--end_id", opt->end_id, "Request parameter end_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--limit", opt->limit, "Request parameter limit")
      ->default_val(50)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--is_reverse", opt->is_reverse, "Request parameter is_reverse")
      ->default_val(false)
      ->group("Coordinator Manager Commands");

  coor->add_flag("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->group("Coordinator Manager Commands");

  coor->add_option("--scalar_filter_key", opt->scalar_filter_key, "Request scalar_filter_key")
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_value", opt->scalar_filter_value, "Request parameter scalar_filter_value")
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_key2", opt->scalar_filter_key2, "Request parameter scalar_filter_key2")
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_value2", opt->scalar_filter_value2, "Request parameter scalar_filter_value2")
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { RunVectorScanQuery(*opt); });
}

void RunVectorScanQuery(VectorScanQueryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendVectorScanQuery(opt);
}

void SetUpVectorScanDump(CLI::App& app) {
  auto opt = std::make_shared<VectorScanDumpOptions>();
  auto coor = app.add_subcommand("VectorScanQuery", "Vector scan query ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_id", opt->start_id, "Request parameter start_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--end_id", opt->end_id, "Request parameter end_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--limit", opt->limit, "Request parameter limit")
      ->default_val(50)
      ->group("Coordinator Manager Commands");
  coor->add_option("--is_reverse", opt->is_reverse, "Request parameter is_reverse")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--csv_output", opt->csv_output, "Request parameter is_reverse")
      ->required()
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { RunVectorScanDump(*opt); });
}

void RunVectorScanDump(VectorScanDumpOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendVectorScanDump(opt);
}

void SetUpVectorGetRegionMetrics(CLI::App& app) {
  auto opt = std::make_shared<VectorGetRegionMetricsOptions>();
  auto coor =
      app.add_subcommand("VectorGetRegionMetrics", "Vector get region metrics")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorGetRegionMetricsd(*opt); });
}

void RunVectorGetRegionMetricsd(VectorGetRegionMetricsOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendVectorGetRegionMetrics(opt);
}

void SetUpVectorAdd(CLI::App& app) {
  auto opt = std::make_shared<VectorAddOptions>();
  auto coor = app.add_subcommand("VectorAdd", "Vector add ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Request parameter table_id")->group("Coordinator Manager Commands");
  coor->add_option("--dimension", opt->dimension, "Request parameter dimension")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_id", opt->start_id, "Request parameter start_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--count", opt->count, "Request parameter count")->group("Coordinator Manager Commands");
  coor->add_option("--step_count", opt->step_count, "Request parameter step_count")
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_table", opt->without_table, "Request parameter without_scalar")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--csv_data", opt->csv_data, "Request parameter csv_data")->group("Coordinator Manager Commands");
  coor->add_option("--json_data", opt->json_data, "Request parameter json_data")->group("Coordinator Manager Commands");

  coor->add_option("--scalar_filter_key", opt->scalar_filter_key, "Request parameter scalar_filter_key")
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_value", opt->scalar_filter_value, "Request parameter scalar_filter_value")
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_key2", opt->scalar_filter_key2, "Request parameter scalar_filter_key2")
      ->group("Coordinator Manager Commands");
  coor->add_option("--scalar_filter_value2", opt->scalar_filter_value2, "Request parameter scalar_filter_value2")
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { RunVectorAdd(*opt); });
}

void RunVectorAdd(VectorAddOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  if (opt.table_id > 0) {
    client_v2::SendVectorAddRetry(opt);
  } else {
    client_v2::SendVectorAdd(opt);
  }
  // client_v2::SendVectorGetRegionMetrics(opt.region_id);
}

void SetUpVectorDelete(CLI::App& app) {
  auto opt = std::make_shared<VectorDeleteOptions>();
  auto coor = app.add_subcommand("VectorDelete", "Vector delete")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_id", opt->start_id, "Request parameter start_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--count", opt->count, "Request parameter count")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorDelete(*opt); });
}

void RunVectorDelete(VectorDeleteOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendVectorDelete(opt);
}

void SetUpVectorGetMaxId(CLI::App& app) {
  auto opt = std::make_shared<VectorGetMaxIdOptions>();
  auto coor = app.add_subcommand("VectorGetMaxId", "Vector get max id")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorGetMaxId(*opt); });
}

void RunVectorGetMaxId(VectorGetMaxIdOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendVectorGetMaxId(opt);
}

void SetUpVectorGetMinId(CLI::App& app) {
  auto opt = std::make_shared<VectorGetMinIdOptions>();
  auto coor = app.add_subcommand("VectorGetMinId", "Vector get min id")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorGetMinId(*opt); });
}

void RunVectorGetMinId(VectorGetMinIdOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendVectorGetMinId(opt);
}

void SetUpVectorAddBatch(CLI::App& app) {
  auto opt = std::make_shared<VectorAddBatchOptions>();
  auto coor = app.add_subcommand("VectorAddBatch", "Vector add batch")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--dimension", opt->dimension, "Request parameter dimension")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_id", opt->start_id, "Request parameter start_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--count", opt->count, "Request parameter count")->group("Coordinator Manager Commands");
  coor->add_option("--step_count", opt->step_count, "Request parameter step_count")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--vector_index_add_cost_file", opt->vector_index_add_cost_file, "exec batch vector add. cost time")
      ->default_val("./cost.txt")
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { RunVectorAddBatch(*opt); });
}

void RunVectorAddBatch(VectorAddBatchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendVectorAddBatch(opt);
}

void SetUpVectorAddBatchDebug(CLI::App& app) {
  auto opt = std::make_shared<VectorAddBatchDebugOptions>();
  auto coor = app.add_subcommand("VectorAddBatchDebug", "Vector add batch debug")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--dimension", opt->dimension, "Request parameter dimension")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_id", opt->start_id, "Request parameter start_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--count", opt->count, "Request parameter count")->group("Coordinator Manager Commands");
  coor->add_option("--step_count", opt->step_count, "Request parameter step_count")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--vector_index_add_cost_file", opt->vector_index_add_cost_file, "exec batch vector add. cost time")
      ->default_val("./cost.txt")
      ->group("Coordinator Manager Commands");
  coor->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorAddBatchDebug(*opt); });
}

void RunVectorAddBatchDebug(VectorAddBatchDebugOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::SendVectorAddBatchDebug(opt);
}

void SetUpVectorCalcDistance(CLI::App& app) {
  auto opt = std::make_shared<VectorCalcDistanceOptions>();
  auto coor = app.add_subcommand("VectorCalcDistance", "Vector add batch debug")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--dimension", opt->dimension, "Request parameter dimension")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--alg_type", opt->alg_type, "use alg type. such as faiss or hnsw")
      ->default_val("faiss")
      ->group("Coordinator Manager Commands");
  coor->add_option("--metric_type", opt->metric_type, "metric type. such as L2 or IP or cosine")
      ->default_val("L2")
      ->group("Coordinator Manager Commands");
  coor->add_option("--left_vector_size", opt->left_vector_size, "left vector size. <= 0 error")
      ->default_val(2)
      ->group("Coordinator Manager Commands");
  coor->add_option("--right_vector_size", opt->right_vector_size, "right vector size. <= 0 error")
      ->default_val(3)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--is_return_normlize", opt->is_return_normlize, "is return normlize")
      ->default_val(true)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorCalcDistance(*opt); });
}

void RunVectorCalcDistance(VectorCalcDistanceOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendVectorCalcDistance(opt);
}

void SetUpCalcDistance(CLI::App& app) {
  auto opt = std::make_shared<CalcDistanceOptions>();
  auto coor = app.add_subcommand("CalcDistance", "Calc distance")->group("Store Manager Commands");
  coor->add_option("--vector_data1", opt->vector_data1, "Request parameter vector_data1")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--vector_data2", opt->vector_data2, "Request parameter vector_data2")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunCalcDistance(*opt); });
}

void RunCalcDistance(CalcDistanceOptions const& opt) { client_v2::SendCalcDistance(opt); }

void SetUpVectorCount(CLI::App& app) {
  auto opt = std::make_shared<VectorCountOptions>();
  auto coor = app.add_subcommand("VectorCount", "Vector count")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--start_id", opt->start_id, "Request parameter start_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--end_id", opt->end_id, "Request parameter end_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunVectorCount(*opt); });
}

void RunVectorCount(VectorCountOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::SendVectorCount(opt);
}

void SetUpCountVectorTable(CLI::App& app) {
  auto opt = std::make_shared<CountVectorTableOptions>();
  auto coor = app.add_subcommand("CountVectorTable", "Count vector table")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Request parameter table_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunCountVectorTable(*opt); });
}

void RunCountVectorTable(CountVectorTableOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {opt.store_addrs}, 0, 0)) {
    exit(-1);
  }

  client_v2::CountVectorTable(opt);
}

// test operation
void SetUpTestBatchPutGet(CLI::App& app) {
  auto opt = std::make_shared<TestBatchPutGetOptions>();
  auto coor = app.add_subcommand("TestBatchPutGet", "Test batch put and get")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Request parameter table_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--thread_num", opt->thread_num, "Number of threads sending requests")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->add_option("--req_num", opt->req_num, "Number of requests")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->add_option("--prefix", opt->prefix, "key prefix")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTestBatchPutGet(*opt); });
}

void RunTestBatchPutGet(TestBatchPutGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }

  client_v2::TestBatchPutGet(opt);
}

void SetUpTestRegionLifecycle(CLI::App& app) {
  auto opt = std::make_shared<TestRegionLifecycleOptions>();
  auto coor = app.add_subcommand("TestRegionLifecycle", "Test region lifecycle")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--raft_group", opt->raft_group, "Request parameter raft group")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--raft_addrs", opt->raft_addrs,
                   "example --raft_addr:127.0.0.1:10101:0,127.0.0.1:10102:0,127.0.0.1:10103:0")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--thread_num", opt->thread_num, "Number of threads sending requests")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->add_option("--req_num", opt->req_num, "Number of requests")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->add_option("--prefix", opt->prefix, "key prefix")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTestRegionLifecycle(*opt); });
}

void RunTestRegionLifecycle(TestRegionLifecycleOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);
  client_v2::TestRegionLifecycle(opt);
}

void SetUpTestDeleteRangeWhenTransferLeader(CLI::App& app) {
  auto opt = std::make_shared<TestDeleteRangeWhenTransferLeaderOptions>();
  auto coor =
      app.add_subcommand("TestDeleteRangeWhenTransferLeaderOptions", "Test delete range when transfer leader options")
          ->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--req_num", opt->req_num, "Number of requests")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->add_option("--prefix", opt->prefix, "key prefix")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunTestDeleteRangeWhenTransferLeader(*opt); });
}

void RunTestDeleteRangeWhenTransferLeader(TestDeleteRangeWhenTransferLeaderOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    exit(-1);
  }
  client_v2::TestDeleteRangeWhenTransferLeader(opt);
}

void SetUpAutoMergeRegion(CLI::App& app) {
  auto opt = std::make_shared<AutoMergeRegionOptions>();
  auto coor = app.add_subcommand("AutoMergeRegion", "Auto merge region")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Request parameter table_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--index_id", opt->index_id, "Request parameter index_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunAutoMergeRegion(*opt); });
}

void RunAutoMergeRegion(AutoMergeRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    exit(-1);
  }

  AutoMergeRegion(opt);
}

void SetUpAutoDropTable(CLI::App& app) {
  auto opt = std::make_shared<AutoDropTableOptions>();
  auto coor = app.add_subcommand("AutoDropTable", "Auto drop table")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--req_num", opt->req_num, "Number of requests")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunAutoDropTable(*opt); });
}

void RunAutoDropTable(AutoDropTableOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  client_v2::AutoDropTable(opt);
}

void SetUpCheckTableDistribution(CLI::App& app) {
  auto opt = std::make_shared<CheckTableDistributionOptions>();
  auto coor = app.add_subcommand("CheckTableDistribution", "Check table distribution")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Number of table_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Number of key")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunCheckTableDistribution(*opt); });
}

void RunCheckTableDistribution(CheckTableDistributionOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  client_v2::CheckTableDistribution(opt);
}

void SetUpCheckIndexDistribution(CLI::App& app) {
  auto opt = std::make_shared<CheckIndexDistributionOptions>();
  auto coor = app.add_subcommand("CheckIndexDistribution", "Check index distribution")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Number of table_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunCheckIndexDistribution(*opt); });
}

void RunCheckIndexDistribution(CheckIndexDistributionOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  client_v2::CheckIndexDistribution(opt);
}

void SetUpDumpDb(CLI::App& app) {
  auto opt = std::make_shared<DumpDbOptions>();
  auto coor = app.add_subcommand("DumpDb", "Dump rocksdb")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Number of table_id")->group("Coordinator Manager Commands");
  coor->add_option("--index_id", opt->index_id, "Number of index_id")->group("Coordinator Manager Commands");
  coor->add_option("--db_path", opt->db_path, "rocksdb path")->group("Coordinator Manager Commands");
  coor->add_option("--offset", opt->offset, "Number of offset, must greatern than 0")
      ->group("Coordinator Manager Commands");
  coor->add_option("--limit", opt->limit, "Number of limit")->default_val(50)->group("Coordinator Manager Commands");
  coor->add_flag("--show_vector", opt->show_vector, "show vector data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");

  coor->add_flag("--show_lock", opt->show_lock, "show lock")->default_val(false)->group("Coordinator Manager Commands");
  coor->add_flag("--show_write", opt->show_write, "show write")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--show_last_data", opt->show_last_data, "show last data")
      ->default_val(true)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--show_all_data", opt->show_all_data, "show all data")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--show_pretty", opt->show_pretty, "show  pretty")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--print_column_width", opt->print_column_width, "print column width")
      ->default_val(24)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunDumpDb(*opt); });
}

void RunDumpDb(DumpDbOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  client_v2::DumpDb(opt);
}

void SetUpWhichRegion(CLI::App& app) {
  auto opt = std::make_shared<WhichRegionOptions>();
  auto coor = app.add_subcommand("DumpDb", "Dump rocksdb")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Number of table_id")->group("Coordinator Manager Commands");
  coor->add_option("--index_id", opt->index_id, "Number of index_id")->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunWhichRegion(*opt); });
}

void RunWhichRegion(WhichRegionOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  if (opt.table_id == 0 && opt.index_id == 0) {
    DINGO_LOG(ERROR) << "Param table_id|index_id is error.";
    return;
  }
  if (opt.key.empty()) {
    DINGO_LOG(ERROR) << "Param key is error.";
    return;
  }
  WhichRegion(opt);
}

void SetUpRegionMetrics(CLI::App& app) {
  auto opt = std::make_shared<RegionMetricsOptions>();
  auto coor = app.add_subcommand("GetRegionMetrics", "Get region metrics ")->group("Region Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  coor->add_option("--region_ids", opt->region_ids, "Request parameter, empty means query all regions");
  coor->add_option("--type", opt->type,
                   "Request parameter type, only support 6 or 8, 6 means store region metrics;8 means store region "
                   "actual metrics.")
      ->required();
  coor->callback([opt]() { RunRegionMetrics(*opt); });
}

void RunRegionMetrics(RegionMetricsOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    exit(-1);
  }
  dingodb::pb::debug::DebugRequest request;
  dingodb::pb::debug::DebugResponse response;
  if (opt.type != 6 && opt.type != 8) {
    std::cout << "type only support 6 or 8";
    return;
  }
  request.set_type(::dingodb::pb::debug::DebugType(opt.type));
  // request.set_type(opt.type);
  for (const int64_t& region_id : opt.region_ids) {
    request.add_region_ids(region_id);
  }

  auto status = InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "Debug", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Get region metrics  failed, error:"
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  if (opt.type == 6) {
    for (const auto& it : response.region_metrics().region_metricses()) {
      std::cout << it.DebugString() << std::endl;
    }
  } else if (opt.type == 8) {
    for (const auto& it : response.region_actual_metrics().region_metricses()) {
      std::cout << it.DebugString() << std::endl;
    }
  }
}

// store function
int GetCreateTableId(int64_t& table_id) {
  dingodb::pb::meta::CreateTableIdRequest request;
  dingodb::pb::meta::CreateTableIdResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  auto status =
      InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "CreateTableId", request, response);
  DINGO_LOG(INFO) << "SendRequestWithoutContext status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.has_table_id()) {
    table_id = response.table_id().entity_id();
    return 0;
  } else {
    return -1;
  }
}

dingodb::pb::meta::CreateTableRequest BuildCreateTableRequest(const std::string& table_name, int partition_num) {
  dingodb::pb::meta::CreateTableRequest request;

  int64_t new_table_id = 0;
  int ret = GetCreateTableId(new_table_id);
  if (ret != 0) {
    DINGO_LOG(WARNING) << "GetCreateTableId failed";
    return request;
  }

  uint32_t part_count = partition_num;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = 0;
    int ret = GetCreateTableId(new_part_id);
    if (ret != 0) {
      DINGO_LOG(WARNING) << "GetCreateTableId failed";
      return request;
    }
    part_ids.push_back(new_part_id);
  }

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  // setup table_id
  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(schema_id->entity_id());
  table_id->set_entity_id(new_table_id);

  // string name = 1;
  auto* table_definition = request.mutable_table_definition();
  table_definition->set_name(table_name);

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type("INT");
    column->set_element_type("INT");
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

  for (int i = 0; i < partition_num; i++) {
    auto* part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_table_id);
    part->mutable_range()->set_start_key(client_v2::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client_v2::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  return request;
}

int64_t SendCreateTable(const std::string& table_name, int partition_num) {
  auto request = BuildCreateTableRequest(table_name, partition_num);
  if (request.table_id().entity_id() == 0) {
    DINGO_LOG(WARNING) << "BuildCreateTableRequest failed";
    return 0;
  }

  dingodb::pb::meta::CreateTableResponse response;
  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "CreateTable", request, response);

  DINGO_LOG(INFO) << "response=" << response.DebugString();
  return response.table_id().entity_id();
}

void SendDropTable(int64_t table_id) {
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  auto* mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "DropTable", request, response);
}

int64_t SendGetTableByName(const std::string& table_name) {
  dingodb::pb::meta::GetTableByNameRequest request;
  dingodb::pb::meta::GetTableByNameResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_table_name(table_name);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTableByName", request, response);

  return response.table_definition_with_id().table_id().entity_id();
}

bool QueryRegionIdByVectorId(dingodb::pb::meta::IndexRange& index_range, int64_t vector_id,  // NOLINT
                             int64_t& region_id) {                                           // NOLINT
  for (const auto& item : index_range.range_distribution()) {
    const auto& range = item.range();
    int64_t min_vector_id = dingodb::VectorCodec::UnPackageVectorId(range.start_key());
    int64_t max_vector_id = dingodb::VectorCodec::UnPackageVectorId(range.end_key());
    max_vector_id = max_vector_id == 0 ? INT64_MAX : max_vector_id;
    if (vector_id >= min_vector_id && vector_id < max_vector_id) {
      region_id = item.id().entity_id();
      return true;
    }
  }

  DINGO_LOG(ERROR) << fmt::format("query region id by key failed, vector_id {}", vector_id);
  return false;
}

void PrintIndexRange(dingodb::pb::meta::IndexRange& index_range) {  // NOLINT
  DINGO_LOG(INFO) << "refresh route...";
  for (const auto& item : index_range.range_distribution()) {
    DINGO_LOG(INFO) << fmt::format("region {} range [{}-{})", item.id().entity_id(),
                                   dingodb::Helper::StringToHex(item.range().start_key()),
                                   dingodb::Helper::StringToHex(item.range().end_key()));
  }
}

std::vector<int64_t> SendGetTablesBySchema() {
  dingodb::pb::meta::GetTablesBySchemaRequest request;
  dingodb::pb::meta::GetTablesBySchemaResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTablesBySchema", request, response);

  std::vector<int64_t> table_ids;
  for (const auto& id : response.table_definition_with_ids()) {
    table_ids.push_back(id.table_id().entity_id());
  }

  return table_ids;
}

// ============================== meta service ===========================

dingodb::pb::meta::TableDefinition SendGetIndex(int64_t index_id) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto* mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(index_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTables", request, response);

  return response.table_definition_with_ids()[0].table_definition();
}

dingodb::pb::meta::TableDefinition SendGetTable(int64_t table_id) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto* mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTables", request, response);

  if (response.table_definition_with_ids().empty()) {
    return dingodb::pb::meta::TableDefinition();
  }
  return response.table_definition_with_ids()[0].table_definition();
}

dingodb::pb::meta::TableRange SendGetTableRange(int64_t table_id) {
  dingodb::pb::meta::GetTableRangeRequest request;
  dingodb::pb::meta::GetTableRangeResponse response;

  auto* mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTableRange", request, response);

  return response.table_range();
}

dingodb::pb::meta::IndexRange SendGetIndexRange(int64_t table_id) {
  dingodb::pb::meta::GetIndexRangeRequest request;
  dingodb::pb::meta::GetIndexRangeResponse response;

  auto* mut_index_id = request.mutable_index_id();
  mut_index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_index_id->set_entity_id(table_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetIndexRange", request, response);

  return response.index_range();
}

dingodb::pb::common::Region SendQueryRegion(int64_t region_id) {
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  request.set_region_id(region_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "QueryRegion", request, response);

  return response.region();
}

void SendDocumentAdd(DocumentAddOptions const& opt) {
  dingodb::pb::document::DocumentAddRequest request;
  dingodb::pb::document::DocumentAddResponse response;

  if (opt.document_id <= 0) {
    DINGO_LOG(ERROR) << "document_id is invalid";
    return;
  }

  if (opt.document_text1.empty()) {
    DINGO_LOG(ERROR) << "document_text1 is empty";
    return;
  }

  if (opt.document_text2.empty()) {
    DINGO_LOG(ERROR) << "document_text2 is empty";
    return;
  }

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  auto* document = request.add_documents();
  document->set_id(opt.document_id);
  auto* document_data = document->mutable_document()->mutable_document_data();

  // col1 text
  {
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data(opt.document_text1);
    (*document_data)["col1"] = document_value1;
  }

  // col2 int64
  {
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
    document_value1.mutable_field_value()->set_long_data(opt.document_id);
    (*document_data)["col2"] = document_value1;
  }

  // col3 double
  {
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
    document_value1.mutable_field_value()->set_double_data(opt.document_id * 1.0);
    (*document_data)["col3"] = document_value1;
  }

  // col4 text
  {
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data(opt.document_text2);
    (*document_data)["col4"] = document_value1;
  }

  if (opt.is_update) {
    request.set_is_update(true);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentAdd", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendDocumentDelete(DocumentDeleteOptions const& opt) {
  dingodb::pb::document::DocumentDeleteRequest request;
  dingodb::pb::document::DocumentDeleteResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (int i = 0; i < opt.count; ++i) {
    request.add_ids(i + opt.start_id);
  }

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentDelete", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendDocumentSearch(DocumentSearchOptions const& opt) {
  dingodb::pb::document::DocumentSearchRequest request;
  dingodb::pb::document::DocumentSearchResponse response;

  if (opt.query_string.empty()) {
    DINGO_LOG(ERROR) << "query_string is empty";
    return;
  }

  if (opt.topn == 0) {
    DINGO_LOG(ERROR) << "topn is 0";
    return;
  }

  auto* parameter = request.mutable_parameter();
  parameter->set_top_n(opt.topn);
  parameter->set_query_string(opt.query_string);
  parameter->set_without_scalar_data(opt.without_scalar);

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentSearch", request, response);

  DINGO_LOG(INFO) << "DocumentSearch response: " << response.DebugString();
}

void SendDocumentBatchQuery(DocumentBatchQueryOptions const& opt) {
  dingodb::pb::document::DocumentBatchQueryRequest request;
  dingodb::pb::document::DocumentBatchQueryResponse response;
  auto document_ids = {static_cast<int64_t>(opt.document_id)};

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (auto document_id : document_ids) {
    request.add_document_ids(document_id);
  }

  if (opt.without_scalar) {
    request.set_without_scalar_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentBatchQuery", request, response);

  DINGO_LOG(INFO) << "DocumentBatchQuery response: " << response.DebugString();
}

void SendDocumentGetMaxId(DocumentGetMaxIdOptions const& opt) {  // NOLINT
  dingodb::pb::document::DocumentGetBorderIdRequest request;
  dingodb::pb::document::DocumentGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentGetBorderId", request, response);

  DINGO_LOG(INFO) << "DocumentGetBorderId response: " << response.DebugString();
}

void SendDocumentGetMinId(DocumentGetMinIdOptions const& opt) {  // NOLINT
  dingodb::pb::document::DocumentGetBorderIdRequest request;
  dingodb::pb::document::DocumentGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_get_min(true);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentGetBorderId", request, response);

  DINGO_LOG(INFO) << "DocumentGetBorderId response: " << response.DebugString();
}

void SendDocumentScanQuery(DocumentScanQueryOptions const& opt) {
  dingodb::pb::document::DocumentScanQueryRequest request;
  dingodb::pb::document::DocumentScanQueryResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_document_id_start(opt.start_id);
  request.set_document_id_end(opt.end_id);

  if (opt.limit > 0) {
    request.set_max_scan_count(opt.limit);
  } else {
    request.set_max_scan_count(10);
  }

  request.set_is_reverse_scan(opt.is_reverse);

  request.set_without_scalar_data(opt.without_scalar);
  if (!opt.key.empty()) {
    auto* keys = request.mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentScanQuery", request, response);

  DINGO_LOG(INFO) << "DocumentScanQuery response: " << response.DebugString()
                  << " documents count: " << response.documents_size();
}

int64_t SendDocumentCount(DocumentCountOptions const& opt) {
  dingodb::pb::document::DocumentCountRequest request;
  dingodb::pb::document::DocumentCountResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  if (opt.start_id > 0) {
    request.set_document_id_start(opt.start_id);
  }
  if (opt.end_id > 0) {
    request.set_document_id_end(opt.end_id);
  }

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentCount", request, response);

  return response.error().errcode() != 0 ? 0 : response.count();
}

void SendDocumentGetRegionMetrics(DocumentGetRegionMetricsOptions const& opt) {
  dingodb::pb::document::DocumentGetRegionMetricsRequest request;
  dingodb::pb::document::DocumentGetRegionMetricsResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentGetRegionMetrics", request,
                                                           response);

  DINGO_LOG(INFO) << "DocumentGetRegionMetrics response: " << response.DebugString();
}

int SendBatchVectorAdd(int64_t region_id, uint32_t dimension, std::vector<int64_t> vector_ids,
                       std::vector<std::vector<float>> vector_datas, uint32_t vector_datas_offset, bool with_scalar,
                       bool with_table, std::string scalar_filter_key, std::string scalar_filter_value,
                       std::string scalar_filter_key2, std::string scalar_filter_value2) {
  dingodb::pb::index::VectorAddRequest request;
  dingodb::pb::index::VectorAddResponse response;

  uint32_t max_size = 0;
  if (vector_datas.empty()) {
    max_size = vector_ids.size();
  } else {
    if (vector_datas.size() > vector_datas_offset + vector_ids.size()) {
      max_size = vector_ids.size();
    } else {
      max_size = vector_datas.size() > vector_datas_offset ? vector_datas.size() - vector_datas_offset : 0;
    }
  }

  if (max_size == 0) {
    DINGO_LOG(INFO) << "vector_datas.size() - vector_datas_offset <= vector_ids.size(), max_size: " << max_size
                    << ", vector_datas.size: " << vector_datas.size()
                    << ", vector_datas_offset: " << vector_datas_offset << ", vector_ids.size: " << vector_ids.size();
    return 0;
  } else {
    DINGO_LOG(DEBUG) << "vector_datas.size(): " << vector_datas.size()
                     << " vector_datas_offset: " << vector_datas_offset << " vector_ids.size(): " << vector_ids.size()
                     << " max_size: " << max_size;
  }

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(region_id);

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  for (int i = 0; i < max_size; ++i) {
    const auto& vector_id = vector_ids[i];

    auto* vector_with_id = request.add_vectors();
    vector_with_id->set_id(vector_id);
    vector_with_id->mutable_vector()->set_dimension(dimension);
    vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);

    if (vector_datas.empty()) {
      for (int j = 0; j < dimension; j++) {
        vector_with_id->mutable_vector()->add_float_values(distrib(rng));
      }

    } else {
      const auto& vector_data = vector_datas[i + vector_datas_offset];
      CHECK(vector_data.size() == dimension);

      for (int j = 0; j < dimension; j++) {
        vector_with_id->mutable_vector()->add_float_values(vector_data[j]);
      }
    }

    if (with_scalar) {
      if (scalar_filter_key.empty() || scalar_filter_value.empty()) {
        for (int k = 0; k < 2; ++k) {
          auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
          dingodb::pb::common::ScalarValue scalar_value;
          scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
          scalar_value.add_fields()->set_string_data(fmt::format("scalar_value{}", k));
          (*scalar_data)[fmt::format("scalar_key{}", k)] = scalar_value;
        }
        for (int k = 2; k < 4; ++k) {
          auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
          dingodb::pb::common::ScalarValue scalar_value;
          scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
          scalar_value.add_fields()->set_long_data(k);
          (*scalar_data)[fmt::format("scalar_key{}", k)] = scalar_value;
        }
      } else {
        if (scalar_filter_key == "enable_scalar_schema" || scalar_filter_key2 == "enable_scalar_schema") {
          auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
          // bool speedup key
          {
            std::string key = "speedup_key_bool";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            bool value = i % 2;
            field->set_bool_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // int speedup key
          {
            std::string key = "speedup_key_int";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            int value = i;
            field->set_int_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // long speedup key
          {
            std::string key = "speedup_key_long";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            int64_t value = i + 1000;
            field->set_long_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // float speedup key
          {
            std::string key = "speedup_key_float";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            float value = 0.23 + i;
            field->set_float_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // double speedup key
          {
            std::string key = "speedup_key_double";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            double value = 0.23 + i + 1000;
            field->set_double_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // string speedup key
          {
            std::string key = "speedup_key_string";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            std::string value(1024 * 2, 's');
            value = std::to_string(i) + value;
            field->set_string_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // bytes speedup key
          {
            std::string key = "speedup_key_bytes";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            std::string value(1024 * 2, 'b');
            value = std::to_string(i) + value;
            field->set_bytes_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          //////////////////////////////////no speedup
          /// key/////////////////////////////////////////////////////////////////////////
          // bool key
          {
            std::string key = "key_bool";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            bool value = i % 2;
            field->set_bool_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // int key
          {
            std::string key = "key_int";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            int value = i;
            field->set_int_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // long key
          {
            std::string key = "key_long";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            int64_t value = i + 1000;
            field->set_long_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // float key
          {
            std::string key = "key_float";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            float value = 0.23 + i;
            field->set_float_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // double key
          {
            std::string key = "key_double";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            double value = 0.23 + i + 1000;
            field->set_double_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // string  key
          {
            std::string key = "key_string";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            std::string value(1024 * 2, 's');
            value = std::to_string(i) + value;
            field->set_string_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // bytes key
          {
            std::string key = "key_bytes";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            std::string value(1024 * 2, 'b');
            value = std::to_string(i) + value;
            field->set_bytes_data(value);

            (*scalar_data)[key] = scalar_value;
          }

        } else {
          if (!scalar_filter_key.empty()) {
            auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            scalar_value.add_fields()->set_string_data(scalar_filter_value);
            (*scalar_data)[scalar_filter_key] = scalar_value;
          }

          if (!scalar_filter_key2.empty()) {
            auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            scalar_value.add_fields()->set_string_data(scalar_filter_value2);
            (*scalar_data)[scalar_filter_key2] = scalar_value;
          }
        }
      }
    }

    if (with_table) {
      auto* table_data = vector_with_id->mutable_table_data();
      table_data->set_table_key(fmt::format("table_key{}", vector_id));
      table_data->set_table_value(fmt::format("table_value{}", vector_id));
    }

    DINGO_LOG(DEBUG) << "vector_with_id : " << vector_with_id->ShortDebugString();
  }

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorAdd", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "VectorAdd repsonse error: " << response.error().DebugString();
  }

  DINGO_LOG(INFO) << fmt::format("VectorAdd response success region: {} count: {} fail count: {} vector count: {}",
                                 region_id, success_count, response.key_states().size() - success_count,
                                 request.vectors().size());

  return response.error().errcode();
}

// We can use a csv file to import vector_data
// The csv file format is:
// 1.0, 2.0, 3.0, 4.0
// 1.1, 2.1, 3.1, 4.1
// the line count must equal to the vector count, no new line at the end of the file
void SendVectorAddRetry(VectorAddOptions const& opt) {  // NOLINT
  auto index_range = SendGetIndexRange(opt.table_id);
  if (index_range.range_distribution().empty()) {
    DINGO_LOG(INFO) << fmt::format("Not found range of table {}", opt.table_id);
    return;
  }
  PrintIndexRange(index_range);

  std::vector<std::vector<float>> vector_datas;

  if (!opt.csv_data.empty()) {
    if (!dingodb::Helper::IsExistPath(opt.csv_data)) {
      DINGO_LOG(ERROR) << fmt::format("csv data file {} not exist", opt.csv_data);
      return;
    }

    std::ifstream file(opt.csv_data);

    if (file.is_open()) {
      std::string line;
      while (std::getline(file, line)) {
        std::vector<float> row;
        std::stringstream ss(line);
        std::string value;
        while (std::getline(ss, value, ',')) {
          row.push_back(std::stof(value));
        }
        vector_datas.push_back(row);
      }
      file.close();
    }

    DINGO_LOG(INFO) << fmt::format("csv data size: {}", vector_datas.size());
  }

  uint32_t total_count = 0;

  int64_t end_id = 0;
  if (vector_datas.empty()) {
    end_id = opt.start_id + opt.count;
  } else {
    end_id = opt.start_id + vector_datas.size();
  }
  DINGO_LOG(INFO) << "start_id: " << opt.start_id << " end_id: " << end_id << ", step_count: " << opt.step_count;

  std::vector<int64_t> vector_ids;
  vector_ids.reserve(opt.step_count);
  for (int64_t i = opt.start_id; i < end_id; i += opt.step_count) {
    for (int64_t j = i; j < i + opt.step_count; ++j) {
      vector_ids.push_back(j);
    }

    int64_t region_id = 0;
    if (!QueryRegionIdByVectorId(index_range, i, region_id)) {
      DINGO_LOG(ERROR) << fmt::format("query region id by vector id failed, vector id {}", i);
      return;
    }

    int ret = SendBatchVectorAdd(region_id, opt.dimension, vector_ids, vector_datas, total_count, !opt.without_scalar,
                                 !opt.without_table, opt.scalar_filter_key, opt.scalar_filter_value,
                                 opt.scalar_filter_key2, opt.scalar_filter_value2);
    if (ret == dingodb::pb::error::EKEY_OUT_OF_RANGE || ret == dingodb::pb::error::EREGION_REDIRECT) {
      bthread_usleep(1000 * 500);  // 500ms
      index_range = SendGetIndexRange(opt.table_id);
      PrintIndexRange(index_range);
    }

    DINGO_LOG(INFO) << fmt::format("schedule: {}/{}", i, end_id);

    total_count += vector_ids.size();

    vector_ids.clear();
  }
}

// We can use a csv file to import vector_data
// The csv file format is:
// 1.0, 2.0, 3.0, 4.0
// 1.1, 2.1, 3.1, 4.1
// the line count must equal to the vector count, no new line at the end of the file
void SendVectorAdd(VectorAddOptions const& opt) {
  std::vector<int64_t> vector_ids;
  vector_ids.reserve(opt.step_count);

  std::vector<std::vector<float>> vector_datas;

  if (!opt.csv_data.empty()) {
    if (!dingodb::Helper::IsExistPath(opt.csv_data)) {
      DINGO_LOG(ERROR) << fmt::format("csv data file {} not exist", opt.csv_data);
      return;
    }

    std::ifstream file(opt.csv_data);

    if (file.is_open()) {
      std::string line;
      while (std::getline(file, line)) {
        std::vector<float> row;
        std::stringstream ss(line);
        std::string value;
        while (std::getline(ss, value, ',')) {
          row.push_back(std::stof(value));
        }
        vector_datas.push_back(row);
      }
      file.close();
    }

    DINGO_LOG(INFO) << fmt::format("csv data size: {}", vector_datas.size());
  }

  uint32_t total_count = 0;

  int64_t end_id = 0;
  if (vector_datas.empty()) {
    end_id = opt.start_id + opt.count;
  } else {
    end_id = opt.start_id + vector_datas.size();
  }
  DINGO_LOG(INFO) << "start_id: " << opt.start_id << " end_id: " << end_id << ", step_count: " << opt.step_count;

  for (int i = opt.start_id; i < end_id; i += opt.step_count) {
    for (int j = i; j < i + opt.step_count; ++j) {
      vector_ids.push_back(j);
    }

    SendBatchVectorAdd(opt.region_id, opt.dimension, vector_ids, vector_datas, total_count, !opt.without_scalar,
                       !opt.without_table, opt.scalar_filter_key, opt.scalar_filter_value, opt.scalar_filter_key2,
                       opt.scalar_filter_value2);

    total_count += vector_ids.size();

    vector_ids.clear();
  }
}

void SendVectorDelete(VectorDeleteOptions const& opt) {  // NOLINT
  dingodb::pb::index::VectorDeleteRequest request;
  dingodb::pb::index::VectorDeleteResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (int i = 0; i < opt.count; ++i) {
    request.add_ids(i + opt.start_id);
  }

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorDelete", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }

  DINGO_LOG(INFO) << "VectorDelete repsonse error: " << response.error().DebugString();
  DINGO_LOG(INFO) << fmt::format("VectorDelete response success count: {} fail count: {}", success_count,
                                 response.key_states().size() - success_count);
}

void SendVectorGetMaxId(VectorGetMaxIdOptions const& opt) {  // NOLINT
  dingodb::pb::index::VectorGetBorderIdRequest request;
  dingodb::pb::index::VectorGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorGetBorderId", request, response);

  DINGO_LOG(INFO) << "VectorGetBorderId response: " << response.DebugString();
}

void SendVectorGetMinId(VectorGetMinIdOptions const& opt) {  // NOLINT
  dingodb::pb::index::VectorGetBorderIdRequest request;
  dingodb::pb::index::VectorGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_get_min(true);

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorGetBorderId", request, response);

  DINGO_LOG(INFO) << "VectorGetBorderId response: " << response.DebugString();
}

void SendVectorAddBatch(VectorAddBatchOptions const& opt) {
  if (opt.step_count == 0) {
    DINGO_LOG(ERROR) << "step_count must be greater than 0";
    return;
  }
  if (opt.region_id == 0) {
    DINGO_LOG(ERROR) << "region_id must be greater than 0";
    return;
  }
  if (opt.dimension == 0) {
    DINGO_LOG(ERROR) << "dimension must be greater than 0";
    return;
  }
  if (opt.count == 0) {
    DINGO_LOG(ERROR) << "count must be greater than 0";
    return;
  }
  if (opt.start_id < 0) {
    DINGO_LOG(ERROR) << "start_id must be greater than 0";
    return;
  }
  if (opt.vector_index_add_cost_file.empty()) {
    DINGO_LOG(ERROR) << "vector_index_add_cost_file must not be empty";
    return;
  }

  std::filesystem::path url(opt.vector_index_add_cost_file);
  std::fstream out;
  if (!std::filesystem::exists(url)) {
    // not exist
    out.open(opt.vector_index_add_cost_file, std::ios::out | std::ios::binary);
  } else {
    out.open(opt.vector_index_add_cost_file, std::ios::out | std::ios::binary | std::ios::trunc);
  }

  if (!out.is_open()) {
    DINGO_LOG(ERROR) << fmt::format("{} open failed", opt.vector_index_add_cost_file);
    out.close();
    return;
  }

  out << "index,cost(us)\n";
  int64_t total = 0;

  if (opt.count % opt.step_count != 0) {
    DINGO_LOG(ERROR) << fmt::format("count {} must be divisible by step_count {}", opt.count, opt.step_count);
    return;
  }

  uint32_t cnt = opt.count / opt.step_count;

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  std::vector<float> random_seeds;
  random_seeds.resize(opt.count * opt.dimension);
  for (uint32_t i = 0; i < opt.count; ++i) {
    for (uint32_t j = 0; j < opt.dimension; ++j) {
      random_seeds[i * opt.dimension + j] = distrib(rng);
    }

    if (i % 10000 == 0) {
      DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", i, opt.count);
    }
  }

  // Add data to index
  // uint32_t num_threads = std::thread::hardware_concurrency();
  // try {
  //   ParallelFor(0, count, num_threads, [&](size_t row, size_t /*thread_id*/) {
  //     std::mt19937 rng;
  //     std::uniform_real_distribution<> distrib(0.0, 10.0);
  //     for (uint32_t i = 0; i < dimension; ++i) {
  //       random_seeds[row * dimension + i] = distrib(rng);
  //     }
  //   });
  // } catch (std::runtime_error& e) {
  //   DINGO_LOG(ERROR) << "generate random data failed, error=" << e.what();
  //   return;
  // }

  DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", opt.count, opt.count);

  for (uint32_t x = 0; x < cnt; x++) {
    auto start = std::chrono::steady_clock::now();
    {
      dingodb::pb::index::VectorAddRequest request;
      dingodb::pb::index::VectorAddResponse response;

      *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

      int64_t real_start_id = opt.start_id + x * opt.step_count;
      for (int i = real_start_id; i < real_start_id + opt.step_count; ++i) {
        auto* vector_with_id = request.add_vectors();
        vector_with_id->set_id(i);
        vector_with_id->mutable_vector()->set_dimension(opt.dimension);
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        for (int j = 0; j < opt.dimension; j++) {
          vector_with_id->mutable_vector()->add_float_values(random_seeds[(i - opt.start_id) * opt.dimension + j]);
        }

        if (!opt.without_scalar) {
          for (int k = 0; k < 3; k++) {
            ::dingodb::pb::common::ScalarValue scalar_value;
            int index = k + (i < 30 ? 0 : 1);
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            field->set_string_data("value" + std::to_string(index));

            vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert(
                {"key" + std::to_string(index), scalar_value});
          }
        }
      }

      butil::Status status =
          InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorAdd", request, response);
      int success_count = 0;
      for (auto key_state : response.key_states()) {
        if (key_state) {
          ++success_count;
        }
      }
    }

    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    out << x << " , " << diff << "\n";

    DINGO_LOG(INFO) << "index : " << x << " : " << diff
                    << " us, avg : " << static_cast<long double>(diff) / opt.step_count << " us";

    total += diff;
  }

  DINGO_LOG(INFO) << fmt::format("total : {} cost : {} (us) avg : {} us", opt.count, total,
                                 static_cast<long double>(total) / opt.count);

  out.close();
}

void SendVectorAddBatchDebug(VectorAddBatchDebugOptions const& opt) {
  if (opt.step_count == 0) {
    DINGO_LOG(ERROR) << "step_count must be greater than 0";
    return;
  }
  if (opt.region_id == 0) {
    DINGO_LOG(ERROR) << "region_id must be greater than 0";
    return;
  }
  if (opt.dimension == 0) {
    DINGO_LOG(ERROR) << "dimension must be greater than 0";
    return;
  }
  if (opt.count == 0) {
    DINGO_LOG(ERROR) << "count must be greater than 0";
    return;
  }
  if (opt.start_id < 0) {
    DINGO_LOG(ERROR) << "start_id must be greater than 0";
    return;
  }
  if (opt.vector_index_add_cost_file.empty()) {
    DINGO_LOG(ERROR) << "vector_index_add_cost_file must not be empty";
    return;
  }

  std::filesystem::path url(opt.vector_index_add_cost_file);
  std::fstream out;
  if (!std::filesystem::exists(url)) {
    // not exist
    out.open(opt.vector_index_add_cost_file, std::ios::out | std::ios::binary);
  } else {
    out.open(opt.vector_index_add_cost_file, std::ios::out | std::ios::binary | std::ios::trunc);
  }

  if (!out.is_open()) {
    DINGO_LOG(ERROR) << fmt::format("{} open failed", opt.vector_index_add_cost_file);
    out.close();
    return;
  }

  out << "index,cost(us)\n";
  int64_t total = 0;

  if (opt.count % opt.step_count != 0) {
    DINGO_LOG(ERROR) << fmt::format("count {} must be divisible by step_count {}", opt.count, opt.step_count);
    return;
  }

  uint32_t cnt = opt.count / opt.step_count;

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  std::vector<float> random_seeds;
  random_seeds.resize(opt.count * opt.dimension);
  for (uint32_t i = 0; i < opt.count; ++i) {
    for (uint32_t j = 0; j < opt.dimension; ++j) {
      random_seeds[i * opt.dimension + j] = distrib(rng);
    }

    if (i % 10000 == 0) {
      DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", i, opt.count);
    }
  }

  // Add data to index
  // uint32_t num_threads = std::thread::hardware_concurrency();
  // try {
  //   ParallelFor(0, count, num_threads, [&](size_t row, size_t /*thread_id*/) {
  //     std::mt19937 rng;
  //     std::uniform_real_distribution<> distrib(0.0, 10.0);
  //     for (uint32_t i = 0; i < dimension; ++i) {
  //       random_seeds[row * dimension + i] = distrib(rng);
  //     }
  //   });
  // } catch (std::runtime_error& e) {
  //   DINGO_LOG(ERROR) << "generate random data failed, error=" << e.what();
  //   return;
  // }

  DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", opt.count, opt.count);

  for (uint32_t x = 0; x < cnt; x++) {
    auto start = std::chrono::steady_clock::now();
    {
      dingodb::pb::index::VectorAddRequest request;
      dingodb::pb::index::VectorAddResponse response;

      *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

      int64_t real_start_id = opt.start_id + x * opt.step_count;
      for (int i = real_start_id; i < real_start_id + opt.step_count; ++i) {
        auto* vector_with_id = request.add_vectors();
        vector_with_id->set_id(i);
        vector_with_id->mutable_vector()->set_dimension(opt.dimension);
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        for (int j = 0; j < opt.dimension; j++) {
          vector_with_id->mutable_vector()->add_float_values(random_seeds[(i - opt.start_id) * opt.dimension + j]);
        }

        if (!opt.without_scalar) {
          auto index = (i - opt.start_id);
          auto left = index % 200;

          auto lambda_insert_scalar_data_function = [&vector_with_id](int tag) {
            ::dingodb::pb::common::ScalarValue scalar_value;

            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            field->set_string_data("tag" + std::to_string(tag));

            vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert(
                {"tag" + std::to_string(tag), scalar_value});
          };

          if (left >= 0 && left < 99) {  // tag1 [0, 99] total 100
            lambda_insert_scalar_data_function(1);
          } else if (left >= 100 && left <= 139) {  // tag2 [100, 139]  total 40
            lambda_insert_scalar_data_function(2);
          } else if (left >= 140 && left <= 149) {  // tag3 [140, 149]  total 10
            lambda_insert_scalar_data_function(3);
          } else if (left >= 150 && left <= 154) {  // tag4 [150, 154]  total 5
            lambda_insert_scalar_data_function(4);
          } else if (left >= 155 && left <= 156) {  // tag5 [155, 156]  total 2
            lambda_insert_scalar_data_function(5);
          } else if (left >= 157 && left <= 157) {  // tag6 [157, 157]  total 1
            lambda_insert_scalar_data_function(6);
          } else {
            // do nothing
          }
        }
      }

      butil::Status status =
          InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorAdd", request, response);
      int success_count = 0;
      for (auto key_state : response.key_states()) {
        if (key_state) {
          ++success_count;
        }
      }
    }

    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    out << x << " , " << diff << "\n";

    DINGO_LOG(INFO) << "index : " << x << " : " << diff
                    << " us, avg : " << static_cast<long double>(diff) / opt.step_count << " us";

    total += diff;
  }

  DINGO_LOG(INFO) << fmt::format("total : {} cost : {} (us) avg : {} us", opt.count, total,
                                 static_cast<long double>(total) / opt.count);

  out.close();
}

// vector_data1: "1.0, 2.0, 3.0, 4.0"
// vector_data2: "1.1, 2.0, 3.0, 4.1"
void SendCalcDistance(CalcDistanceOptions const& opt) {
  if (opt.vector_data1.empty() || opt.vector_data2.empty()) {
    DINGO_LOG(ERROR) << "vector_data1 or vector_data2 is empty";
    return;
  }

  std::vector<float> x_i = dingodb::Helper::StringToVector(opt.vector_data1);
  std::vector<float> y_j = dingodb::Helper::StringToVector(opt.vector_data2);

  if (x_i.size() != y_j.size() || x_i.empty() || y_j.empty()) {
    DINGO_LOG(ERROR) << "vector_data1 size must be equal to vector_data2 size";
    return;
  }

  auto dimension = x_i.size();

  auto faiss_l2 = dingodb::Helper::DingoFaissL2sqr(x_i.data(), y_j.data(), dimension);
  auto faiss_ip = dingodb::Helper::DingoFaissInnerProduct(x_i.data(), y_j.data(), dimension);
  auto hnsw_l2 = dingodb::Helper::DingoHnswL2Sqr(x_i.data(), y_j.data(), dimension);
  auto hnsw_ip = dingodb::Helper::DingoHnswInnerProduct(x_i.data(), y_j.data(), dimension);
  auto hnsw_ip_dist = dingodb::Helper::DingoHnswInnerProductDistance(x_i.data(), y_j.data(), dimension);

  DINGO_LOG(INFO) << "vector_data1: " << opt.vector_data1;
  DINGO_LOG(INFO) << " vector_data2: " << opt.vector_data2;

  DINGO_LOG(INFO) << "[faiss_l2: " << faiss_l2 << ", faiss_ip: " << faiss_ip << ", hnsw_l2: " << hnsw_l2
                  << ", hnsw_ip: " << hnsw_ip << ", hnsw_ip_dist: " << hnsw_ip_dist << "]";
}

void SendVectorCalcDistance(VectorCalcDistanceOptions const& opt) {
  ::dingodb::pb::index::VectorCalcDistanceRequest request;
  ::dingodb::pb::index::VectorCalcDistanceResponse response;

  if (opt.dimension == 0) {
    DINGO_LOG(ERROR) << "step_count must be greater than 0";
    return;
  }

  std::string real_alg_type = opt.alg_type;
  std::string real_metric_type = opt.metric_type;

  std::transform(real_alg_type.begin(), real_alg_type.end(), real_alg_type.begin(), ::tolower);
  std::transform(real_metric_type.begin(), real_metric_type.end(), real_metric_type.begin(), ::tolower);

  bool is_faiss = ("faiss" == real_alg_type);
  bool is_hnsw = ("hnsw" == real_alg_type);

  // if (!is_faiss && !is_hnsw) {
  //   DINGO_LOG(ERROR) << "invalid alg_type :  use faiss or hnsw!!!";
  //   return;
  // }

  bool is_l2 = ("l2" == real_metric_type);
  bool is_ip = ("ip" == real_metric_type);
  bool is_cosine = ("cosine" == real_metric_type);
  // if (!is_l2 && !is_ip && !is_cosine) {
  //   DINGO_LOG(ERROR) << "invalid metric_type :  use L2 or IP or cosine !!!";
  //   return;
  // }

  // if (left_vector_size <= 0) {
  //   DINGO_LOG(ERROR) << "left_vector_size <=0 : " << left_vector_size;
  //   return;
  // }

  // if (right_vector_size <= 0) {
  //   DINGO_LOG(ERROR) << "right_vector_size <=0 : " << left_vector_size;
  //   return;
  // }

  dingodb::pb::index::AlgorithmType algorithm_type = dingodb::pb::index::AlgorithmType::ALGORITHM_NONE;
  if (is_faiss) {
    algorithm_type = dingodb::pb::index::AlgorithmType::ALGORITHM_FAISS;
  }
  if (is_hnsw) {
    algorithm_type = dingodb::pb::index::AlgorithmType::ALGORITHM_HNSWLIB;
  }

  dingodb::pb::common::MetricType my_metric_type = dingodb::pb::common::MetricType::METRIC_TYPE_NONE;
  if (is_l2) {
    my_metric_type = dingodb::pb::common::MetricType::METRIC_TYPE_L2;
  }
  if (is_ip) {
    my_metric_type = dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
  }
  if (is_cosine) {
    my_metric_type = dingodb::pb::common::MetricType::METRIC_TYPE_COSINE;
  }
  google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
  google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib;

  // op left assignment
  for (size_t i = 0; i < opt.left_vector_size; i++) {
    ::dingodb::pb::common::Vector op_left_vector;
    for (uint32_t i = 0; i < opt.dimension; i++) {
      op_left_vector.add_float_values(distrib(rng));
    }
    op_left_vectors.Add(std::move(op_left_vector));
  }

  // op right assignment
  for (size_t i = 0; i < opt.right_vector_size; i++) {
    ::dingodb::pb::common::Vector op_right_vector;
    for (uint32_t i = 0; i < opt.dimension; i++) {
      op_right_vector.add_float_values(distrib(rng));
    }
    op_right_vectors.Add(std::move(op_right_vector));  // NOLINT
  }

  request.set_algorithm_type(algorithm_type);
  request.set_metric_type(my_metric_type);
  request.set_is_return_normlize(opt.is_return_normlize);
  request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
  request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

  DINGO_LOG(INFO) << "SendVectorCalcDistance request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithoutContext("UtilService", "VectorCalcDistance", request, response);

  for (const auto& distance : response.distances()) {
    DINGO_LOG(INFO) << "distance: " << distance.ShortDebugString();
  }
  DINGO_LOG(INFO) << "SendVectorCalcDistance error: " << response.error().ShortDebugString();
  DINGO_LOG(INFO) << "distance size: " << response.distances_size();
}

int64_t SendVectorCount(VectorCountOptions const& opt) {
  ::dingodb::pb::index::VectorCountRequest request;
  ::dingodb::pb::index::VectorCountResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  if (opt.start_id > 0) {
    request.set_vector_id_start(opt.start_id);
  }
  if (opt.end_id > 0) {
    request.set_vector_id_end(opt.end_id);
  }

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorCount", request, response);

  return response.error().errcode() != 0 ? 0 : response.count();
}

void CountVectorTable(CountVectorTableOptions const& opt) {
  auto index_range = SendGetIndexRange(opt.table_id);

  int64_t total_count = 0;
  std::map<std::string, dingodb::pb::meta::RangeDistribution> region_map;
  for (const auto& region_range : index_range.range_distribution()) {
    if (region_range.range().start_key() >= region_range.range().end_key()) {
      DINGO_LOG(ERROR) << fmt::format("Invalid region {} range [{}-{})", region_range.id().entity_id(),
                                      dingodb::Helper::StringToHex(region_range.range().start_key()),
                                      dingodb::Helper::StringToHex(region_range.range().end_key()));
      continue;
    }
    VectorCountOptions opt;
    opt.region_id = region_range.id().entity_id();
    opt.end_id = 0;
    opt.start_id = 0;
    int64_t count = SendVectorCount(opt);
    total_count += count;
    DINGO_LOG(INFO) << fmt::format("partition_id({}) region({}) count({}) total_count({})",
                                   region_range.id().parent_entity_id(), region_range.id().entity_id(), count,
                                   total_count);
  }
}

// We cant use FLAGS_vector_data to define the vector we want to search, the format is:
// 1.0, 2.0, 3.0, 4.0
// only one vector data, no new line at the end of the file, and only float value and , is allowed

void SendVectorSearch(VectorSearchOptions const& opt) {
  dingodb::pb::index::VectorSearchRequest request;
  dingodb::pb::index::VectorSearchResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  auto* vector = request.add_vector_with_ids();

  if (opt.region_id == 0) {
    DINGO_LOG(ERROR) << "region_id is 0";
    return;
  }

  if (opt.dimension == 0) {
    DINGO_LOG(ERROR) << "dimension is 0";
    return;
  }

  if (opt.topn == 0) {
    DINGO_LOG(ERROR) << "topn is 0";
    return;
  }

  if (opt.vector_data.empty()) {
    for (int i = 0; i < opt.dimension; i++) {
      vector->mutable_vector()->add_float_values(1.0 * i);
    }
  } else {
    std::vector<float> row = dingodb::Helper::StringToVector(opt.vector_data);

    CHECK(opt.dimension == row.size()) << "dimension not match";

    for (auto v : row) {
      vector->mutable_vector()->add_float_values(v);
    }
  }

  request.mutable_parameter()->set_top_n(opt.topn);

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < 20; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 100000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    if (opt.scalar_filter_key.empty() || opt.scalar_filter_value.empty()) {
      DINGO_LOG(ERROR) << "scalar_filter_key or scalar_filter_value is empty";
      return;
    }

    auto lambda_cmd_set_key_value_function = [](dingodb::pb::common::VectorScalardata& scalar_data,
                                                const std::string& cmd_key, const std::string& cmd_value) {
      // bool
      if ("speedup_key_bool" == cmd_key || "key_bool" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        bool value = (cmd_value == "true");
        field->set_bool_data(value);
        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // int
      if ("speedup_key_int" == cmd_key || "key_int" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        int value = std::stoi(cmd_value);
        field->set_int_data(value);
        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // long
      if ("speedup_key_long" == cmd_key || "key_long" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        int64_t value = std::stoll(cmd_value);
        field->set_long_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // float
      if ("speedup_key_float" == cmd_key || "key_float" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        float value = std::stof(cmd_value);
        field->set_float_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // double
      if ("speedup_key_double" == cmd_key || "key_double" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        double value = std::stod(cmd_value);
        field->set_double_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // string
      if ("speedup_key_string" == cmd_key || "key_string" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        std::string value = cmd_value;
        field->set_string_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // bytes
      if ("speedup_key_bytes" == cmd_key || "key_bytes" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        std::string value = cmd_value;
        field->set_bytes_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }
    };

    if (!opt.scalar_filter_key.empty()) {
      if (std::string::size_type idx = opt.scalar_filter_key.find("key"); idx != std::string::npos) {
        lambda_cmd_set_key_value_function(*vector->mutable_scalar_data(), opt.scalar_filter_key,
                                          opt.scalar_filter_value);

      } else {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        field->set_string_data(opt.scalar_filter_value);

        vector->mutable_scalar_data()->mutable_scalar_data()->insert({opt.scalar_filter_key, scalar_value});

        DINGO_LOG(INFO) << "scalar_filter_key: " << opt.scalar_filter_key
                        << " scalar_filter_value: " << opt.scalar_filter_value;
      }
    }

    if (!opt.scalar_filter_key2.empty()) {
      if (std::string::size_type idx = opt.scalar_filter_key2.find("key"); idx != std::string::npos) {
        lambda_cmd_set_key_value_function(*vector->mutable_scalar_data(), opt.scalar_filter_key2,
                                          opt.scalar_filter_value2);
      } else {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        field->set_string_data(opt.scalar_filter_value2);

        vector->mutable_scalar_data()->mutable_scalar_data()->insert({opt.scalar_filter_key2, scalar_value});

        DINGO_LOG(INFO) << "scalar_filter_key2: " << opt.scalar_filter_key2
                        << " scalar_filter_value2: " << opt.scalar_filter_value2;
      }
    }
  }

  if (opt.with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);
    if (opt.scalar_filter_key.empty() || opt.scalar_filter_value.empty()) {
      DINGO_LOG(ERROR) << "scalar_filter_key or scalar_filter_value is empty";
      return;
    }

    if (!opt.scalar_filter_key.empty()) {
      dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.scalar_filter_value);

      vector->mutable_scalar_data()->mutable_scalar_data()->insert({opt.scalar_filter_key, scalar_value});

      DINGO_LOG(INFO) << "scalar_filter_key: " << opt.scalar_filter_key
                      << " scalar_filter_value: " << opt.scalar_filter_value;
    }

    if (!opt.scalar_filter_key2.empty()) {
      dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.scalar_filter_value2);

      vector->mutable_scalar_data()->mutable_scalar_data()->insert({opt.scalar_filter_key2, scalar_value});

      DINGO_LOG(INFO) << "scalar_filter_key2: " << opt.scalar_filter_key2
                      << " scalar_filter_value2: " << opt.scalar_filter_value2;
    }
  }

  if (opt.with_table_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::TABLE_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);
  }

  if (opt.ef_search > 0) {
    DINGO_LOG(INFO) << "ef_search=" << opt.ef_search;
    request.mutable_parameter()->mutable_hnsw()->set_efsearch(opt.ef_search);
  }

  if (opt.bruteforce) {
    DINGO_LOG(INFO) << "bruteforce=" << opt.bruteforce;
    request.mutable_parameter()->set_use_brute_force(opt.bruteforce);
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearch  span: {} (us)", diff);

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
  }

  DINGO_LOG(INFO) << "VectorSearch response: " << response.DebugString();

  std::ofstream file;
  if (!opt.csv_output.empty()) {
    file = std::ofstream(opt.csv_output, std::ios::out | std::ios::app);
    if (!file.is_open()) {
      DINGO_LOG(ERROR) << "open file failed";
      return;
    }
  }

  for (const auto& batch_result : response.batch_results()) {
    DINGO_LOG(INFO) << "VectorSearch response, batch_result_dist_size: " << batch_result.vector_with_distances_size();

    for (const auto& vector_with_distance : batch_result.vector_with_distances()) {
      std::string vector_string = dingodb::Helper::VectorToString(
          dingodb::Helper::PbRepeatedToVector(vector_with_distance.vector_with_id().vector().float_values()));

      DINGO_LOG(INFO) << "vector_id: " << vector_with_distance.vector_with_id().id() << ", vector: [" << vector_string
                      << "]";

      if (!opt.csv_output.empty()) {
        file << vector_string << '\n';
      }
    }

    if (!opt.without_vector) {
      for (const auto& vector_with_distance : batch_result.vector_with_distances()) {
        std::vector<float> x_i = dingodb::Helper::PbRepeatedToVector(vector->vector().float_values());
        std::vector<float> y_j =
            dingodb::Helper::PbRepeatedToVector(vector_with_distance.vector_with_id().vector().float_values());

        auto faiss_l2 = dingodb::Helper::DingoFaissL2sqr(x_i.data(), y_j.data(), opt.dimension);
        auto faiss_ip = dingodb::Helper::DingoFaissInnerProduct(x_i.data(), y_j.data(), opt.dimension);
        auto hnsw_l2 = dingodb::Helper::DingoHnswL2Sqr(x_i.data(), y_j.data(), opt.dimension);
        auto hnsw_ip = dingodb::Helper::DingoHnswInnerProduct(x_i.data(), y_j.data(), opt.dimension);
        auto hnsw_ip_dist = dingodb::Helper::DingoHnswInnerProductDistance(x_i.data(), y_j.data(), opt.dimension);

        DINGO_LOG(INFO) << "vector_id: " << vector_with_distance.vector_with_id().id()
                        << ", distance: " << vector_with_distance.distance() << ", [faiss_l2: " << faiss_l2
                        << ", faiss_ip: " << faiss_ip << ", hnsw_l2: " << hnsw_l2 << ", hnsw_ip: " << hnsw_ip
                        << ", hnsw_ip_dist: " << hnsw_ip_dist << "]";
      }
    }
  }

  if (file.is_open()) {
    file.close();
  }

  // match compare
  if (opt.with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';

    std::vector<int64_t> result_vt_ids;
    for (const auto& vector_with_distance_result : response.batch_results()) {
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }
    }

    if (result_vt_ids.empty()) {
      std::cout << "result_vt_ids : empty" << '\n';
    } else {
      std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
      for (auto id : result_vt_ids) {
        std::cout << id << " ";
      }
      std::cout << "]";
      std::cout << '\n';
    }

    for (auto id : result_vt_ids) {
      auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
      if (iter == vt_ids.end()) {
        std::cout << "result_vector_ids not all in vector_ids" << '\n';
        return;
      }
    }
    std::cout << "result_vector_ids  all in vector_ids" << '\n';
  }

  if (opt.with_scalar_pre_filter || opt.with_scalar_post_filter) {
    std::vector<int64_t> result_vt_ids;
    for (const auto& vector_with_distance_result : response.batch_results()) {
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }
    }

    auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
      std::cout << name << ": " << result_vt_ids.size() << " [ ";
      for (auto id : result_vt_ids) {
        std::cout << id << " ";
      }
      std::cout << "]";
      std::cout << '\n';
    };

    lambda_print_result_vector_function("before sort result_vt_ids");

    std::sort(result_vt_ids.begin(), result_vt_ids.end());

    lambda_print_result_vector_function("after  sort result_vt_ids");
  }
}

void SendVectorSearchDebug(VectorSearchDebugOptions const& opt) {
  dingodb::pb::index::VectorSearchDebugRequest request;
  dingodb::pb::index::VectorSearchDebugResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  if (opt.region_id == 0) {
    DINGO_LOG(ERROR) << "region_id is 0";
    return;
  }

  if (opt.dimension == 0) {
    DINGO_LOG(ERROR) << "dimension is 0";
    return;
  }

  if (opt.topn == 0) {
    DINGO_LOG(ERROR) << "topn is 0";
    return;
  }

  if (opt.batch_count == 0) {
    DINGO_LOG(ERROR) << "batch_count is 0";
    return;
  }

  if (opt.start_vector_id > 0) {
    for (int count = 0; count < opt.batch_count; count++) {
      auto* add_vector_with_id = request.add_vector_with_ids();
      add_vector_with_id->set_id(opt.start_vector_id + count);
    }
  } else {
    std::random_device seed;
    std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(0, 100);

    for (int count = 0; count < opt.batch_count; count++) {
      auto* vector = request.add_vector_with_ids()->mutable_vector();
      for (int i = 0; i < opt.dimension; i++) {
        auto random = static_cast<double>(distrib(engine)) / 10.123;
        vector->add_float_values(random);
      }
    }

    request.mutable_parameter()->set_top_n(opt.topn);
  }

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < opt.vector_ids_count; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 1000000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({opt.key, scalar_value});
    }
  }

  if (opt.with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({opt.key, scalar_value});
    }
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearchDebug", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearchDebug  span: {} (us)", diff);

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearchDebug", request, response);
  }

  DINGO_LOG(INFO) << "VectorSearchDebug response: " << response.DebugString();

  DINGO_LOG(INFO) << "VectorSearchDebug response, batch_result_size: " << response.batch_results_size();
  for (const auto& batch_result : response.batch_results()) {
    DINGO_LOG(INFO) << "VectorSearchDebug response, batch_result_dist_size: "
                    << batch_result.vector_with_distances_size();
  }

#if 0  // NOLINT
  // match compare
  if (FLAGS_with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << std::endl;

    std::cout << "response.batch_results() size : " << response.batch_results().size() << std::endl;

    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      if (result_vt_ids.empty()) {
        std::cout << "result_vt_ids : empty" << std::endl;
      } else {
        std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << std::endl;
      }

      for (auto id : result_vt_ids) {
        auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
        if (iter == vt_ids.end()) {
          std::cout << "result_vector_ids not all in vector_ids" << std::endl;
          return;
        }
      }
      std::cout << "result_vector_ids  all in vector_ids" << std::endl;
    }
  }

  if (FLAGS_with_scalar_pre_filter) {
    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
        std::cout << name << ": " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << std::endl;
      };

      lambda_print_result_vector_function("before sort result_vt_ids");

      std::sort(result_vt_ids.begin(), result_vt_ids.end());

      lambda_print_result_vector_function("after  sort result_vt_ids");

      std::cout << std::endl;
    }
  }
#endif
}

void SendVectorRangeSearch(VectorRangeSearchOptions const& opt) {
  dingodb::pb::index::VectorSearchRequest request;
  dingodb::pb::index::VectorSearchResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  auto* vector = request.add_vector_with_ids();

  if (opt.region_id == 0) {
    DINGO_LOG(ERROR) << "region_id is 0";
    return;
  }

  if (opt.dimension == 0) {
    DINGO_LOG(ERROR) << "dimension is 0";
    return;
  }

  for (int i = 0; i < opt.dimension; i++) {
    vector->mutable_vector()->add_float_values(1.0 * i);
  }

  request.mutable_parameter()->set_top_n(0);
  request.mutable_parameter()->set_enable_range_search(true);
  request.mutable_parameter()->set_radius(opt.radius);

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < 20; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 100000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int k = 0; k < 2; k++) {
      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data("value" + std::to_string(k));

      vector->mutable_scalar_data()->mutable_scalar_data()->insert({"key" + std::to_string(k), scalar_value});
    }
  }

  if (opt.with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);

    for (int k = 0; k < 2; k++) {
      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data("value" + std::to_string(k));

      vector->mutable_scalar_data()->mutable_scalar_data()->insert({"key" + std::to_string(k), scalar_value});
    }
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearch  span: {} (us)", diff);

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
  }

  DINGO_LOG(INFO) << "VectorSearch response: " << response.DebugString();

  // match compare
  if (opt.with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';

    std::vector<int64_t> result_vt_ids;
    for (const auto& vector_with_distance_result : response.batch_results()) {
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }
    }

    if (result_vt_ids.empty()) {
      std::cout << "result_vt_ids : empty" << '\n';
    } else {
      std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
      for (auto id : result_vt_ids) {
        std::cout << id << " ";
      }
      std::cout << "]";
      std::cout << '\n';
    }

    for (auto id : result_vt_ids) {
      auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
      if (iter == vt_ids.end()) {
        std::cout << "result_vector_ids not all in vector_ids" << '\n';
        return;
      }
    }
    std::cout << "result_vector_ids  all in vector_ids" << '\n';
  }

  if (opt.with_scalar_pre_filter || opt.with_scalar_post_filter) {
    std::vector<int64_t> result_vt_ids;
    for (const auto& vector_with_distance_result : response.batch_results()) {
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }
    }

    auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
      std::cout << name << ": " << result_vt_ids.size() << " [ ";
      for (auto id : result_vt_ids) {
        std::cout << id << " ";
      }
      std::cout << "]";
      std::cout << '\n';
    };

    lambda_print_result_vector_function("before sort result_vt_ids");

    std::sort(result_vt_ids.begin(), result_vt_ids.end());

    lambda_print_result_vector_function("after  sort result_vt_ids");
  }
}
void SendVectorRangeSearchDebug(VectorRangeSearchDebugOptions const& opt) {
  dingodb::pb::index::VectorSearchDebugRequest request;
  dingodb::pb::index::VectorSearchDebugResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  if (opt.region_id == 0) {
    DINGO_LOG(ERROR) << "region_id is 0";
    return;
  }

  if (opt.dimension == 0) {
    DINGO_LOG(ERROR) << "dimension is 0";
    return;
  }

  if (opt.batch_count == 0) {
    DINGO_LOG(ERROR) << "batch_count is 0";
    return;
  }

  if (opt.start_vector_id > 0) {
    for (int count = 0; count < opt.batch_count; count++) {
      auto* add_vector_with_id = request.add_vector_with_ids();
      add_vector_with_id->set_id(opt.start_vector_id + count);
    }
  } else {
    std::random_device seed;
    std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(0, 100);

    for (int count = 0; count < opt.batch_count; count++) {
      auto* vector = request.add_vector_with_ids()->mutable_vector();
      for (int i = 0; i < opt.dimension; i++) {
        auto random = static_cast<double>(distrib(engine)) / 10.123;
        vector->add_float_values(random);
      }
    }

    request.mutable_parameter()->set_top_n(0);
  }

  request.mutable_parameter()->set_enable_range_search(true);
  request.mutable_parameter()->set_radius(opt.radius);

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < opt.vector_ids_count; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 1000000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({opt.key, scalar_value});
    }
  }

  if (opt.with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({opt.key, scalar_value});
    }
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearchDebug", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearchDebug  span: {} (us)", diff);

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearchDebug", request, response);
  }

  DINGO_LOG(INFO) << "VectorSearchDebug response: " << response.DebugString();

  DINGO_LOG(INFO) << "VectorSearchDebug response, batch_result_size: " << response.batch_results_size();
  for (const auto& batch_result : response.batch_results()) {
    DINGO_LOG(INFO) << "VectorSearchDebug response, batch_result_dist_size: "
                    << batch_result.vector_with_distances_size();
  }

#if 0  // NOLINT
  // match compare
  if (FLAGS_with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << std::endl;

    std::cout << "response.batch_results() size : " << response.batch_results().size() << std::endl;

    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      if (result_vt_ids.empty()) {
        std::cout << "result_vt_ids : empty" << std::endl;
      } else {
        std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << std::endl;
      }

      for (auto id : result_vt_ids) {
        auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
        if (iter == vt_ids.end()) {
          std::cout << "result_vector_ids not all in vector_ids" << std::endl;
          return;
        }
      }
      std::cout << "result_vector_ids  all in vector_ids" << std::endl;
    }
  }

  if (FLAGS_with_scalar_pre_filter) {
    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
        std::cout << name << ": " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << std::endl;
      };

      lambda_print_result_vector_function("before sort result_vt_ids");

      std::sort(result_vt_ids.begin(), result_vt_ids.end());

      lambda_print_result_vector_function("after  sort result_vt_ids");

      std::cout << std::endl;
    }
  }
#endif
}

void SendVectorBatchSearch(VectorBatchSearchOptions const& opt) {
  dingodb::pb::index::VectorSearchRequest request;
  dingodb::pb::index::VectorSearchResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  if (opt.region_id == 0) {
    DINGO_LOG(ERROR) << "region_id is 0";
    return;
  }

  if (opt.dimension == 0) {
    DINGO_LOG(ERROR) << "dimension is 0";
    return;
  }

  if (opt.topn == 0) {
    DINGO_LOG(ERROR) << "topn is 0";
    return;
  }

  if (opt.batch_count == 0) {
    DINGO_LOG(ERROR) << "batch_count is 0";
    return;
  }

  std::random_device seed;
  std::ranlux48 engine(seed());
  std::uniform_int_distribution<> distrib(0, 100);

  for (int count = 0; count < opt.batch_count; count++) {
    auto* vector = request.add_vector_with_ids()->mutable_vector();
    for (int i = 0; i < opt.dimension; i++) {
      auto random = static_cast<double>(distrib(engine)) / 10.123;
      vector->add_float_values(random);
    }
  }

  request.mutable_parameter()->set_top_n(opt.topn);

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < 200; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 10000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      for (int k = 0; k < 2; k++) {
        ::dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
        ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        field->set_string_data("value" + std::to_string(k));

        vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({"key" + std::to_string(k), scalar_value});
      }
    }
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearch  span: {} (us)", diff);

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
  }

  DINGO_LOG(INFO) << "VectorSearch response: " << response.DebugString();

  DINGO_LOG(INFO) << "VectorSearch response, batch_result_size: " << response.batch_results_size();
  for (const auto& batch_result : response.batch_results()) {
    DINGO_LOG(INFO) << "VectorSearch response, batch_result_dist_size: " << batch_result.vector_with_distances_size();
  }

  // match compare
  if (opt.with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';

    std::cout << "response.batch_results() size : " << response.batch_results().size() << '\n';

    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      if (result_vt_ids.empty()) {
        std::cout << "result_vt_ids : empty" << '\n';
      } else {
        std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << '\n';
      }

      for (auto id : result_vt_ids) {
        auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
        if (iter == vt_ids.end()) {
          std::cout << "result_vector_ids not all in vector_ids" << '\n';
          return;
        }
      }
      std::cout << "result_vector_ids  all in vector_ids" << '\n';
    }
  }

  if (opt.with_scalar_pre_filter) {
    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
        std::cout << name << ": " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << '\n';
      };

      lambda_print_result_vector_function("before sort result_vt_ids");

      std::sort(result_vt_ids.begin(), result_vt_ids.end());

      lambda_print_result_vector_function("after  sort result_vt_ids");

      std::cout << '\n';
    }
  }
}

void SendVectorBatchQuery(VectorBatchQueryOptions const& opt) {
  dingodb::pb::index::VectorBatchQueryRequest request;
  dingodb::pb::index::VectorBatchQueryResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (auto vector_id : opt.vector_ids) {
    request.add_vector_ids(vector_id);
  }

  if (opt.without_vector) {
    request.set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorBatchQuery", request, response);

  DINGO_LOG(INFO) << "VectorBatchQuery response: " << response.DebugString();
}

void SendVectorScanQuery(VectorScanQueryOptions const& opt) {
  dingodb::pb::index::VectorScanQueryRequest request;
  dingodb::pb::index::VectorScanQueryResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_vector_id_start(opt.start_id);
  request.set_vector_id_end(opt.end_id);

  if (opt.limit > 0) {
    request.set_max_scan_count(opt.limit);
  } else {
    request.set_max_scan_count(10);
  }

  request.set_is_reverse_scan(opt.is_reverse);

  if (opt.without_vector) {
    request.set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  if (!opt.scalar_filter_key.empty()) {
    auto* scalar_data = request.mutable_scalar_for_filter()->mutable_scalar_data();
    dingodb::pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    scalar_value.add_fields()->set_string_data(opt.scalar_filter_value);
    (*scalar_data)[opt.scalar_filter_key] = scalar_value;

    request.set_use_scalar_filter(true);

    DINGO_LOG(INFO) << "scalar_filter_key: " << opt.scalar_filter_key
                    << " scalar_filter_value: " << opt.scalar_filter_value;
  }

  if (!opt.scalar_filter_key2.empty()) {
    auto* scalar_data = request.mutable_scalar_for_filter()->mutable_scalar_data();
    dingodb::pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    scalar_value.add_fields()->set_string_data(opt.scalar_filter_value2);
    (*scalar_data)[opt.scalar_filter_key2] = scalar_value;

    request.set_use_scalar_filter(true);

    DINGO_LOG(INFO) << "scalar_filter_key2: " << opt.scalar_filter_key2
                    << " scalar_filter_value2: " << opt.scalar_filter_value2;
  }

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorScanQuery", request, response);

  DINGO_LOG(INFO) << "VectorScanQuery response: " << response.DebugString()
                  << " vector count: " << response.vectors().size();
}

butil::Status ScanVectorData(int64_t region_id, int64_t start_id, int64_t end_id, int64_t limit, bool is_reverse,
                             std::vector<std::vector<float>>& vector_datas, int64_t& last_vector_id) {
  dingodb::pb::index::VectorScanQueryRequest request;
  dingodb::pb::index::VectorScanQueryResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(region_id);
  request.set_vector_id_start(start_id);
  request.set_vector_id_end(end_id);
  request.set_max_scan_count(limit);
  request.set_is_reverse_scan(is_reverse);
  request.set_without_vector_data(false);
  request.set_without_scalar_data(true);
  request.set_without_table_data(true);

  auto ret =
      InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorScanQuery", request, response);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "VectorScanQuery failed: " << ret.error_str();
    return ret;
  }

  if (response.error().errcode() != 0) {
    DINGO_LOG(ERROR) << "VectorScanQuery failed: " << response.error().errcode() << " " << response.error().errmsg();
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG(DEBUG) << "VectorScanQuery response: " << response.DebugString()
                   << " vector count: " << response.vectors().size();

  if (response.vectors_size() > 0) {
    for (const auto& vector : response.vectors()) {
      std::vector<float> vector_data;
      vector_data.reserve(vector.vector().float_values_size());
      for (int i = 0; i < vector.vector().float_values_size(); i++) {
        vector_data.push_back(vector.vector().float_values(i));
        if (vector.id() > last_vector_id) {
          last_vector_id = vector.id();
        }
      }
      vector_datas.push_back(vector_data);
    }
  }

  return butil::Status::OK();
}

void SendVectorScanDump(VectorScanDumpOptions const& opt) {
  if (opt.csv_output.empty()) {
    DINGO_LOG(ERROR) << "csv_output is empty";
    return;
  }

  std::ofstream file(opt.csv_output, std::ios::out);

  if (!file.is_open()) {
    DINGO_LOG(ERROR) << "open file failed";
    return;
  }

  int64_t batch_count = opt.limit > 1000 || opt.limit == 0 ? 1000 : opt.limit;

  int64_t new_start_id = opt.start_id - 1;

  for (;;) {
    std::vector<std::vector<float>> vector_datas;
    int64_t last_vector_id = 0;

    if (new_start_id >= opt.end_id) {
      DINGO_LOG(INFO) << "new_start_id: " << new_start_id << " end_id: " << opt.end_id << ", will break";
      break;
    }

    auto ret = ScanVectorData(opt.region_id, new_start_id + 1, opt.end_id, batch_count, opt.is_reverse, vector_datas,
                              new_start_id);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "ScanVectorData failed: " << ret.error_str();
      return;
    }

    if (vector_datas.empty()) {
      DINGO_LOG(INFO) << "vector_datas is empty, finish";
      break;
    }

    for (const auto& vector_data : vector_datas) {
      std::string vector_string = dingodb::Helper::VectorToString(vector_data);
      file << vector_string << '\n';
    }

    DINGO_LOG(INFO) << "new_start_id: " << new_start_id << " end_id: " << opt.end_id
                    << " vector_datas.size(): " << vector_datas.size();
  }
}

void SendVectorGetRegionMetrics(VectorGetRegionMetricsOptions const& opt) {
  dingodb::pb::index::VectorGetRegionMetricsRequest request;
  dingodb::pb::index::VectorGetRegionMetricsResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorGetRegionMetrics", request, response);

  DINGO_LOG(INFO) << "VectorGetRegionMetrics response: " << response.DebugString();
}

void SendKvGet(KvGetOptions const& opt, std::string& value) {
  dingodb::pb::store::KvGetRequest request;
  dingodb::pb::store::KvGetResponse response;
  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_key(opt.key);

  auto status = InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvGet", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "kv get failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  value = response.value();
  std::cout << "value: " << value << std::endl;
}

void SendKvBatchGet(KvBatchGetOptions const& opt) {
  dingodb::pb::store::KvBatchGetRequest request;
  dingodb::pb::store::KvBatchGetResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (int i = 0; i < opt.req_num; ++i) {
    std::string key = opt.prefix + Helper::GenRandomString(30);
    request.add_keys(key);
  }

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvBatchGet", request, response);
}

int SendKvPut(KvPutOptions const& opt, std::string value) {
  dingodb::pb::store::KvPutRequest request;
  dingodb::pb::store::KvPutResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  // request.mutable_context()->mutable_region_epoch()->set_version(0);
  auto* kv = request.mutable_kv();
  kv->set_key(opt.key);
  kv->set_value(value.empty() ? Helper::GenRandomString(256) : value);

  auto status = InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvPut", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "kv put failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return response.error().errcode();
  }
  std::cout << "value:" << value << std::endl;
  return response.error().errcode();
}

void SendKvBatchPut(KvBatchPutOptions const& opt) {
  dingodb::pb::store::KvBatchPutRequest request;
  dingodb::pb::store::KvBatchPutResponse response;
  std::string prefix = dingodb::Helper::HexToString(opt.prefix);
  std::string internal_prefix = prefix;

  if (internal_prefix.empty()) {
    dingodb::pb::common::Region region;
    if (!TxnGetRegion(opt.region_id, region)) {
      DINGO_LOG(ERROR) << "TxnGetRegion failed";
      return;
    }

    dingodb::pb::common::RangeWithOptions range;
    internal_prefix = region.definition().range().start_key();
  }

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (int i = 0; i < opt.count; ++i) {
    std::string key = internal_prefix + Helper::GenRandomString(30);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(Helper::GenRandomString(256));
  }

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvBatchPut", request, response);
}

void SendKvPutIfAbsent(KvPutIfAbsentOptions const& opt) {
  dingodb::pb::store::KvPutIfAbsentRequest request;
  dingodb::pb::store::KvPutIfAbsentResponse response;
  std::string key = dingodb::Helper::HexToString(opt.key);
  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(key);
  kv->set_value(Helper::GenRandomString(64));

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvPutIfAbsent", request, response);
}

void SendKvBatchPutIfAbsent(KvBatchPutIfAbsentOptions const& opt) {
  dingodb::pb::store::KvBatchPutIfAbsentRequest request;
  dingodb::pb::store::KvBatchPutIfAbsentResponse response;
  std::string prefix = dingodb::Helper::HexToString(opt.prefix);
  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (int i = 0; i < opt.count; ++i) {
    std::string key = prefix + Helper::GenRandomString(30);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(Helper::GenRandomString(64));
  }

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvBatchPutIfAbsent", request, response);
}

void SendKvBatchDelete(KvBatchDeleteOptions const& opt) {
  dingodb::pb::store::KvBatchDeleteRequest request;
  dingodb::pb::store::KvBatchDeleteResponse response;
  std::string key = dingodb::Helper::HexToString(opt.key);
  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.add_keys(key);

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvBatchDelete", request, response);
}

void SendKvDeleteRange(KvDeleteRangeOptions const& opt) {
  dingodb::pb::store::KvDeleteRangeRequest request;
  dingodb::pb::store::KvDeleteRangeResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  auto start_key = dingodb::Helper::StringToHex(opt.prefix);
  request.mutable_range()->mutable_range()->set_start_key(opt.prefix);
  request.mutable_range()->mutable_range()->set_end_key(dingodb::Helper::PrefixNext(opt.prefix));
  request.mutable_range()->set_with_start(true);
  request.mutable_range()->set_with_end(false);

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvDeleteRange", request, response);
  DINGO_LOG(INFO) << "delete count: " << response.delete_count();
}

void SendKvScan(KvScanOptions const& opt) {
  dingodb::pb::store::KvScanBeginRequest request;
  dingodb::pb::store::KvScanBeginResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  auto start_key = dingodb::Helper::HexToString(opt.prefix);
  request.mutable_range()->mutable_range()->set_start_key(start_key);
  request.mutable_range()->mutable_range()->set_end_key(dingodb::Helper::PrefixNext(start_key));
  request.mutable_range()->set_with_start(true);
  request.mutable_range()->set_with_end(false);

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvScanBegin", request, response);
  if (response.error().errcode() != 0) {
    return;
  }

  dingodb::pb::store::KvScanContinueRequest continue_request;
  dingodb::pb::store::KvScanContinueResponse continue_response;

  *(continue_request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  continue_request.set_scan_id(response.scan_id());
  int batch_size = 1000;
  continue_request.set_max_fetch_cnt(batch_size);

  int count = 0;
  for (;;) {
    InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvScanContinue", continue_request,
                                                             continue_response);
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

  *(release_request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  release_request.set_scan_id(response.scan_id());

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvScanRelease", release_request,
                                                           release_response);
}

void SendKvCompareAndSet(KvCompareAndSetOptions const& opt) {
  dingodb::pb::store::KvCompareAndSetRequest request;
  dingodb::pb::store::KvCompareAndSetResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(opt.key);
  kv->set_value(Helper::GenRandomString(64));
  request.set_expect_value("");

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvCompareAndSet", request, response);
}

void SendKvBatchCompareAndSet(KvBatchCompareAndSetOptions const& opt) {
  dingodb::pb::store::KvBatchCompareAndSetRequest request;
  dingodb::pb::store::KvBatchCompareAndSetResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (int i = 0; i < opt.count; ++i) {
    std::string key = opt.prefix + Helper::GenRandomString(30);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(Helper::GenRandomString(64));
  }

  for (int i = 0; i < opt.count; i++) {
    request.add_expect_values("");
  }

  request.set_is_atomic(false);

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvBatchCompareAndSet", request, response);
}

void SendKvScanBeginV2(KvScanBeginV2Options const& opt) {
  dingodb::pb::store::KvScanBeginRequestV2 request;
  dingodb::pb::store::KvScanBeginResponseV2 response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.mutable_request_info()->set_request_id(opt.scan_id);

  request.set_scan_id(opt.scan_id);

  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  request.mutable_range()->mutable_range()->set_start_key(region.definition().range().start_key());
  request.mutable_range()->mutable_range()->set_end_key(region.definition().range().end_key());
  request.mutable_range()->set_with_start(true);
  request.mutable_range()->set_with_end(false);

  request.set_max_fetch_cnt(0);

  request.set_key_only(false);

  request.set_disable_auto_release(false);

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvScanBeginV2", request, response);
  DINGO_LOG(INFO) << "KvScanBeginV2 response: " << response.DebugString();
}
void SendKvScanContinueV2(KvScanContinueV2Options const& opt) {
  dingodb::pb::store::KvScanContinueRequestV2 request;
  dingodb::pb::store::KvScanContinueResponseV2 response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.mutable_request_info()->set_request_id(opt.scan_id);
  request.set_scan_id(opt.scan_id);
  request.set_max_fetch_cnt(10);
  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvScanContinueV2", request, response);
  DINGO_LOG(INFO) << "KvScanContinueV2 response: " << response.DebugString();
}

void SendKvScanReleaseV2(KvScanReleaseV2Options const& opt) {
  dingodb::pb::store::KvScanReleaseRequestV2 request;
  dingodb::pb::store::KvScanReleaseResponseV2 response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.mutable_request_info()->set_request_id(opt.scan_id);

  request.set_scan_id(opt.scan_id);
  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvScanReleaseV2", request, response);
  DINGO_LOG(INFO) << "KvScanReleaseV2 response: " << response.DebugString();
}

std::string OctalToHex(const std::string& str) {
  std::string hex_str;
  for (std::size_t i = 0; i < str.size(); ++i) {
    if (str[i] == '\\' && i + 3 < str.size() && str[i + 1] >= '0' && str[i + 1] <= '7' && str[i + 2] >= '0' &&
        str[i + 2] <= '7' && str[i + 3] >= '0' && str[i + 3] <= '7') {
      // Convert octal escape sequence to hexadecimal
      int octal_value = (str[i + 1] - '0') * 64 + (str[i + 2] - '0') * 8 + (str[i + 3] - '0');
      hex_str += "\\x" + std::to_string(octal_value / 16) + std::to_string(octal_value % 16);
      //   hex_str += std::to_string(octal_value / 16) + std::to_string(octal_value % 16);
      i += 3;
    } else {
      // Copy non-escape-sequence characters to output
      hex_str += str[i];
    }
  }
  return hex_str;
}

std::string StringToHex(const std::string& key) { return dingodb::Helper::StringToHex(key); }

std::string HexToString(const std::string& hex) { return dingodb::Helper::HexToString(hex); }

std::string VectorPrefixToHex(char prefix, int64_t part_id) {
  return dingodb::Helper::StringToHex(dingodb::VectorCodec::PackageVectorKey(prefix, part_id));
}

std::string VectorPrefixToHex(char prefix, int64_t part_id, int64_t vector_id) {
  return dingodb::Helper::StringToHex(dingodb::VectorCodec::PackageVectorKey(prefix, part_id, vector_id));
}

std::string TablePrefixToHex(char prefix, const std::string& user_key) {
  std::string buf;
  buf.push_back(prefix);
  buf.append(user_key);
  return dingodb::Helper::StringToHex(buf);
}

std::string TablePrefixToHex(char prefix, int64_t part_id) {
  std::string buf;
  buf.push_back(prefix);
  dingodb::SerialHelper::WriteLong(part_id, buf);

  return dingodb::Helper::StringToHex(buf);
}

std::string TablePrefixToHex(char prefix, int64_t part_id, const std::string& user_key) {
  std::string buf;
  buf.push_back(prefix);
  dingodb::SerialHelper::WriteLong(part_id, buf);
  buf.append(user_key);

  return dingodb::Helper::StringToHex(buf);
}

std::string HexToTablePrefix(const std::string& hex, bool has_part_id) {
  std::string key = dingodb::Helper::HexToString(hex);
  dingodb::Buf buf(key);
  char prefix = buf.Read();
  int64_t part_id = 0;
  if (has_part_id) {
    part_id = buf.ReadLong();
  }
  auto user_key_data_size = key.size() - 1;
  if (has_part_id) {
    user_key_data_size = user_key_data_size - 8;
  }
  std::vector<uint8_t> user_key_data;
  user_key_data.reserve(user_key_data_size);
  for (int i = 0; i < user_key_data_size; i++) {
    user_key_data.push_back(buf.Read());
  }
  std::string user_key(user_key_data.begin(), user_key_data.end());

  return std::string(1, prefix) + "_" + std::to_string(part_id) + "_(hex_" + StringToHex(user_key) + ")_(raw_" +
         user_key + ")";
}

std::string HexToVectorPrefix(const std::string& hex) {
  std::string key = dingodb::Helper::HexToString(hex);
  dingodb::Buf buf(key);
  char prefix = buf.Read();
  int64_t part_id = buf.ReadLong();
  int64_t vector_id = buf.ReadLong();

  return std::string(1, prefix) + "_" + std::to_string(part_id) + "_" + std::to_string(vector_id);
}

bool TxnGetRegion(int64_t region_id, dingodb::pb::common::Region& region) {
  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(region_id);

  auto status = InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "QueryRegion",
                                                                            query_request, query_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << query_response.DebugString();

  if (query_response.region().definition().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return false;
  }

  region = query_response.region();
  return true;
}

std::string GetServiceName(const dingodb::pb::common::Region& region) {
  std::string service_name;
  if (region.region_type() == dingodb::pb::common::RegionType::STORE_REGION) {
    service_name = "StoreService";
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    service_name = "IndexService";
  } else if (region.region_type() == dingodb::pb::common::DOCUMENT_REGION &&
             region.definition().index_parameter().has_document_index_parameter()) {
    service_name = "DocumentService";
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
    exit(-1);
  }

  return service_name;
}

// unified
void SendTxnGet(TxnGetOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnGetRequest request;
  dingodb::pb::store::TxnGetResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = opt.key;
  if (opt.key_is_hex) {
    target_key = HexToString(opt.key);
  }
  request.set_key(target_key);

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.resolve_locks > 0) {
    request.mutable_context()->add_resolved_locks(opt.resolve_locks);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnScan(TxnScanOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    std::cout << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnScanRequest request;
  dingodb::pb::store::TxnScanResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  dingodb::pb::common::RangeWithOptions range;
  if (opt.start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    std::cout << "start_key is empty";
    return;
  } else {
    std::string key = opt.start_key;
    if (opt.key_is_hex) {
      key = HexToString(opt.start_key);
    }
    range.mutable_range()->set_start_key(key);
  }
  if (opt.end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    std::cout << "end_key is empty";
    return;
  } else {
    std::string key = opt.end_key;
    if (opt.key_is_hex) {
      key = HexToString(opt.end_key);
    }
    range.mutable_range()->set_end_key(key);
  }
  range.set_with_start(opt.with_start);
  range.set_with_end(opt.with_end);
  *request.mutable_range() = range;

  if (opt.limit == 0) {
    DINGO_LOG(ERROR) << "limit is empty";
    std::cout << "limit is empty";
    return;
  }
  request.set_limit(opt.limit);

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    std::cout << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  request.set_is_reverse(opt.is_reverse);
  request.set_key_only(opt.key_only);

  if (opt.resolve_locks > 0) {
    request.mutable_context()->add_resolved_locks(opt.resolve_locks);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnScan", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnPessimisticLock(TxnPessimisticLockOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnPessimisticLockRequest request;
  dingodb::pb::store::TxnPessimisticLockResponse response;

  request.mutable_context()->set_region_id(region.id());
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  } else {
    std::string key = opt.primary_lock;
    if (opt.key_is_hex) {
      key = HexToString(opt.primary_lock);
    }
    request.set_primary_lock(key);
  }

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(opt.lock_ttl);

  if (opt.for_update_ts == 0) {
    DINGO_LOG(ERROR) << "for_update_ts is empty";
    return;
  }
  request.set_for_update_ts(opt.for_update_ts);

  if (opt.mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [lock]";
    return;
  }
  if (opt.key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = opt.key;
  if (opt.key_is_hex) {
    target_key = HexToString(opt.key);
  }
  if (opt.value.empty()) {
    DINGO_LOG(ERROR) << "value is empty";
    return;
  }
  std::string target_value = opt.value;
  if (opt.value_is_hex) {
    target_value = HexToString(opt.value);
  }
  if (opt.mutation_op == "lock") {
    if (opt.value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Lock);
    mutation->set_key(target_key);
    mutation->set_value(target_value);
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be [lock]";
    return;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnPessimisticRollback(TxnPessimisticRollbackOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name;
  if (region.region_type() == dingodb::pb::common::RegionType::STORE_REGION) {
    service_name = "StoreService";
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    service_name = "IndexService";
  } else if (region.region_type() == dingodb::pb::common::DOCUMENT_REGION &&
             region.definition().index_parameter().has_document_index_parameter()) {
    service_name = "DocumentService";
  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
    return;
  }

  dingodb::pb::store::TxnPessimisticRollbackRequest request;
  dingodb::pb::store::TxnPessimisticRollbackResponse response;

  request.mutable_context()->set_region_id(region.id());
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.for_update_ts == 0) {
    DINGO_LOG(ERROR) << "for_update_ts is empty";
    return;
  }
  request.set_for_update_ts(opt.for_update_ts);

  if (opt.key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = opt.key;
  if (opt.key_is_hex) {
    target_key = HexToString(opt.key);
  }
  *request.add_keys() = target_key;

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticRollback", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnPrewrite(TxnPrewriteOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  if (region.region_type() == dingodb::pb::common::RegionType::STORE_REGION) {
    StoreSendTxnPrewrite(opt, region);
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    IndexSendTxnPrewrite(opt, region);
  } else if (region.region_type() == dingodb::pb::common::DOCUMENT_REGION &&
             region.definition().index_parameter().has_document_index_parameter()) {
    DocumentSendTxnPrewrite(opt, region);

  } else {
    DINGO_LOG(ERROR) << "region_type is invalid";
  }
}
void SendTxnCommit(TxnCommitOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnCommitRequest request;
  dingodb::pb::store::TxnCommitResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.commit_ts == 0) {
    DINGO_LOG(ERROR) << "commit_ts is empty";
    return;
  }
  request.set_commit_ts(opt.commit_ts);

  if (opt.key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = opt.key;
  if (opt.key_is_hex) {
    target_key = HexToString(opt.key);
  }
  request.add_keys()->assign(target_key);
  DINGO_LOG(INFO) << "key: " << opt.key;

  if (!opt.key2.empty()) {
    std::string target_key2 = opt.key2;
    if (opt.key_is_hex) {
      target_key2 = HexToString(opt.key2);
    }
    request.add_keys()->assign(target_key2);
    DINGO_LOG(INFO) << "key2: " << opt.key2;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnCommit", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnCheckTxnStatus(TxnCheckTxnStatusOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnCheckTxnStatusRequest request;
  dingodb::pb::store::TxnCheckTxnStatusResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.primary_key.empty()) {
    DINGO_LOG(ERROR) << "primary_key is empty";
    return;
  } else {
    std::string key = opt.primary_key;
    if (opt.key_is_hex) {
      key = HexToString(opt.primary_key);
    }
    request.set_primary_key(key);
  }

  if (opt.lock_ts == 0) {
    DINGO_LOG(ERROR) << "lock_ts is 0";
    return;
  }
  request.set_lock_ts(opt.lock_ts);

  if (opt.caller_start_ts == 0) {
    DINGO_LOG(ERROR) << "caller_start_ts is 0";
    return;
  }
  request.set_caller_start_ts(opt.caller_start_ts);

  if (opt.current_ts == 0) {
    DINGO_LOG(ERROR) << "current_ts is 0";
    return;
  }
  request.set_current_ts(opt.current_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnCheckTxnStatus", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnResolveLock(TxnResolveLockOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnResolveLockRequest request;
  dingodb::pb::store::TxnResolveLockResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is 0";
    return;
  }
  request.set_start_ts(opt.start_ts);

  DINGO_LOG(INFO) << "commit_ts is: " << opt.commit_ts;
  if (opt.commit_ts != 0) {
    DINGO_LOG(WARNING) << "commit_ts is not 0, will do commit";
  } else {
    DINGO_LOG(WARNING) << "commit_ts is 0, will do rollback";
  }
  request.set_commit_ts(opt.commit_ts);

  if (opt.key.empty()) {
    DINGO_LOG(INFO) << "key is empty, will do resolve lock for all keys of this transaction";
  } else {
    std::string target_key = opt.key;
    if (opt.key_is_hex) {
      target_key = HexToString(opt.key);
    }
    request.add_keys()->assign(target_key);
    DINGO_LOG(INFO) << "key: " << opt.key;
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnResolveLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnBatchGet(TxnBatchGetOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnBatchGetRequest request;
  dingodb::pb::store::TxnBatchGetResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  if (opt.key2.empty()) {
    DINGO_LOG(ERROR) << "key2 is empty";
    return;
  }
  std::string target_key = opt.key;
  if (opt.key_is_hex) {
    target_key = HexToString(opt.key);
  }
  std::string target_key2 = opt.key2;
  if (opt.key_is_hex) {
    target_key2 = HexToString(opt.key2);
  }
  request.add_keys()->assign(target_key);
  request.add_keys()->assign(target_key2);

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.resolve_locks > 0) {
    request.mutable_context()->add_resolved_locks(opt.resolve_locks);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnBatchGet", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnBatchRollback(TxnBatchRollbackOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnBatchRollbackRequest request;
  dingodb::pb::store::TxnBatchRollbackResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = opt.key;
  if (opt.key_is_hex) {
    target_key = HexToString(opt.key);
  }
  request.add_keys()->assign(target_key);

  if (!opt.key2.empty()) {
    std::string target_key2 = opt.key2;
    if (opt.key_is_hex) {
      target_key2 = HexToString(opt.key2);
    }
    request.add_keys()->assign(target_key2);
  }

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnBatchRollback", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnScanLock(TxnScanLockOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnScanLockRequest request;
  dingodb::pb::store::TxnScanLockResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.max_ts == 0) {
    DINGO_LOG(ERROR) << "max_ts is empty";
    return;
  }
  request.set_max_ts(opt.max_ts);

  if (opt.start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    return;
  } else {
    std::string key = opt.start_key;
    if (opt.key_is_hex) {
      key = HexToString(opt.start_key);
    }
    request.set_start_key(key);
  }

  if (opt.end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  } else {
    std::string key = opt.end_key;
    if (opt.key_is_hex) {
      key = HexToString(opt.end_key);
    }
    request.set_end_key(key);
  }

  if (opt.limit == 0) {
    DINGO_LOG(ERROR) << "limit is empty";
    return;
  }
  request.set_limit(opt.limit);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnScanLock", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnHeartBeat(TxnHeartBeatOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnHeartBeatRequest request;
  dingodb::pb::store::TxnHeartBeatResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  } else {
    std::string key = opt.primary_lock;
    if (opt.key_is_hex) {
      key = HexToString(opt.primary_lock);
    }
    request.set_primary_lock(key);
  }

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.advise_lock_ttl == 0) {
    DINGO_LOG(ERROR) << "advise_lock_ttl is empty";
    return;
  }
  request.set_advise_lock_ttl(opt.advise_lock_ttl);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnHeartBeat", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnGc(TxnGCOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnGcRequest request;
  dingodb::pb::store::TxnGcResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.safe_point_ts == 0) {
    DINGO_LOG(ERROR) << "safe_point_ts is empty";
    return;
  }
  request.set_safe_point_ts(opt.safe_point_ts);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnGc", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnDeleteRange(TxnDeleteRangeOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    DINGO_LOG(ERROR) << "TxnGetRegion failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnDeleteRangeRequest request;
  dingodb::pb::store::TxnDeleteRangeResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.start_key.empty()) {
    DINGO_LOG(ERROR) << "start_key is empty";
    return;
  } else {
    std::string key = opt.start_key;
    if (opt.key_is_hex) {
      key = HexToString(opt.start_key);
    }
    request.set_start_key(key);
  }

  if (opt.end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  } else {
    std::string key = opt.end_key;
    if (opt.key_is_hex) {
      key = HexToString(opt.end_key);
    }
    request.set_end_key(key);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnDeleteRange", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnDump(TxnDumpOptions const& opt) {
  dingodb::pb::common::Region region;
  if (!TxnGetRegion(opt.region_id, region)) {
    std::cout << "TxnGet Region: " << opt.region_id << " failed";
    return;
  }

  std::string service_name = GetServiceName(region);

  dingodb::pb::store::TxnDumpRequest request;
  dingodb::pb::store::TxnDumpResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.start_key.empty()) {
    std::cout << "start_key is empty";
    return;
  }
  std::string target_start_key = opt.start_key;

  if (opt.end_key.empty()) {
    std::cout << "end_key is empty";
    return;
  }
  std::string target_end_key = opt.end_key;

  if (opt.key_is_hex) {
    target_start_key = dingodb::Helper::HexToString(opt.start_key);
    target_end_key = dingodb::Helper::HexToString(opt.end_key);
  }

  request.set_start_key(target_start_key);
  request.set_end_key(target_end_key);

  if (opt.start_ts == 0) {
    std::cout << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.end_ts == 0) {
    std::cout << "end_ts is empty";
    return;
  }
  request.set_end_ts(opt.end_ts);

  auto status = InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnDump", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "txn dump  failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }

  if (response.has_txn_result()) {
    std::cout << "Response TxnResult: " << response.txn_result().ShortDebugString() << std::endl;
  }

  std::cout << "data_size: " << response.data_keys_size() << std::endl;
  std::cout << "lock_size: " << response.lock_keys_size() << std::endl;
  std::cout << "write_size: " << response.write_keys_size() << std::endl;

  for (int i = 0; i < response.data_keys_size(); i++) {
    std::cout << "data[" << i << "] hex_key: [" << StringToHex(response.data_keys(i).key()) << "] key: ["
              << response.data_keys(i).ShortDebugString() << "], value: [" << response.data_values(i).ShortDebugString()
              << "]" << std::endl;
  }

  for (int i = 0; i < response.lock_keys_size(); i++) {
    std::cout << "lock[" << i << "] hex_key: [" << StringToHex(response.lock_keys(i).key()) << "] key: ["
              << response.lock_keys(i).ShortDebugString() << "], value: [" << response.lock_values(i).ShortDebugString()
              << "]" << std::endl;
  }

  for (int i = 0; i < response.write_keys_size(); i++) {
    std::cout << "write[" << i << "] hex_key: [" << StringToHex(response.write_keys(i).key()) << "] key: ["
              << response.write_keys(i).ShortDebugString() << "], value: ["
              << response.write_values(i).ShortDebugString() << "]" << std::endl;
  }
}

// store
void StoreSendTxnPrewrite(TxnPrewriteOptions const& opt, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnPrewriteRequest request;
  dingodb::pb::store::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  }
  if (opt.key_is_hex) {
    request.set_primary_lock(HexToString(opt.primary_lock));
  } else {
    request.set_primary_lock(opt.primary_lock);
  }

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(opt.lock_ttl);

  if (opt.txn_size == 0) {
    DINGO_LOG(ERROR) << "txn_size is empty";
    return;
  }
  request.set_txn_size(opt.txn_size);

  request.set_try_one_pc(opt.try_one_pc);
  request.set_max_commit_ts(opt.max_commit_ts);

  if (opt.mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }
  if (opt.key.empty()) {
    DINGO_LOG(ERROR) << "key is empty";
    return;
  }
  std::string target_key = opt.key;
  std::string target_key2 = opt.key2;
  if (opt.key_is_hex) {
    target_key = HexToString(opt.key);
    target_key2 = HexToString(opt.key2);
  }
  if (opt.mutation_op == "put") {
    if (opt.value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Put);
    mutation->set_key(target_key);
    if (opt.value_is_hex) {
      mutation->set_value(HexToString(opt.value));
    } else {
      mutation->set_value(opt.value);
    }
    DINGO_LOG(INFO) << "key: " << opt.key << ", value: " << opt.value;

    if (!opt.key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::Put);
      mutation->set_key(target_key2);
      if (opt.value_is_hex) {
        mutation->set_value(HexToString(opt.value2));
      } else {
        mutation->set_value(opt.value2);
      }
      DINGO_LOG(INFO) << "key2: " << opt.key2 << ", value: " << opt.value2;
    }
  } else if (opt.mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(opt.key);
    DINGO_LOG(INFO) << "key: " << opt.key;

    if (!opt.key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::Delete);
      mutation->set_key(opt.key2);
      DINGO_LOG(INFO) << "key2: " << opt.key2;
    }
  } else if (opt.mutation_op == "check_not_exists") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::CheckNotExists);
    mutation->set_key(target_key);
    DINGO_LOG(INFO) << "key: " << target_key;

    if (!opt.key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::CheckNotExists);
      mutation->set_key(target_key2);
      DINGO_LOG(INFO) << "key2: " << target_key2;
    }
  } else if (opt.mutation_op == "insert") {
    if (opt.value.empty()) {
      DINGO_LOG(ERROR) << "value is empty";
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(target_key);
    if (opt.value_is_hex) {
      mutation->set_value(HexToString(opt.value));
    } else {
      mutation->set_value(opt.value);
    }
    DINGO_LOG(INFO) << "key: " << opt.key << ", value: " << opt.value;

    if (!opt.key2.empty()) {
      auto* mutation = request.add_mutations();
      mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
      mutation->set_key(target_key2);
      if (opt.value_is_hex) {
        mutation->set_value(HexToString(opt.value2));
      } else {
        mutation->set_value(opt.value2);
      }
      DINGO_LOG(INFO) << "key2: " << opt.key2 << ", value2: " << opt.value2;
    }
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
    return;
  }

  if (!opt.extra_data.empty()) {
    DINGO_LOG(INFO) << "extra_data is: " << opt.extra_data;
  }

  if (opt.for_update_ts > 0) {
    DINGO_LOG(INFO) << "for_update_ts > 0, do pessimistic check : " << opt.for_update_ts;
    for (int i = 0; i < request.mutations_size(); i++) {
      request.add_pessimistic_checks(::dingodb::pb::store::TxnPrewriteRequest_PessimisticCheck::
                                         TxnPrewriteRequest_PessimisticCheck_DO_PESSIMISTIC_CHECK);
      auto* for_update_ts_check = request.add_for_update_ts_checks();
      for_update_ts_check->set_expected_for_update_ts(opt.for_update_ts);
      for_update_ts_check->set_index(i);

      if (!opt.extra_data.empty()) {
        auto* extra_datas = request.add_lock_extra_datas();
        extra_datas->set_index(i);
        extra_datas->set_extra_data(opt.extra_data);
      }
    }
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("StoreService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void IndexSendTxnPrewrite(TxnPrewriteOptions const& opt, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnPrewriteRequest request;
  dingodb::pb::store::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  }
  request.set_primary_lock(opt.primary_lock);

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(opt.lock_ttl);

  if (opt.txn_size == 0) {
    DINGO_LOG(ERROR) << "txn_size is empty";
    return;
  }
  request.set_txn_size(opt.txn_size);

  request.set_try_one_pc(opt.try_one_pc);
  request.set_max_commit_ts(opt.max_commit_ts);

  if (opt.mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }

  if (opt.vector_id == 0) {
    DINGO_LOG(ERROR) << "vector_id is empty";
    return;
  }

  int64_t part_id = region.definition().part_id();
  int64_t dimension = 0;

  const auto& para = region.definition().index_parameter().vector_index_parameter();
  if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT) {
    dimension = para.flat_parameter().dimension();
  } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT) {
    dimension = para.ivf_flat_parameter().dimension();
  } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ) {
    dimension = para.ivf_pq_parameter().dimension();
  } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    dimension = para.hnsw_parameter().dimension();
  } else if (para.vector_index_type() == dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    dimension = para.diskann_parameter().dimension();
  } else {
    DINGO_LOG(ERROR) << "vector_index_type is empty";
    return;
  }

  char perfix = dingodb::Helper::GetKeyPrefix(region.definition().range());
  if (opt.mutation_op == "put") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Put);

    mutation->set_key(dingodb::VectorCodec::PackageVectorKey(perfix, part_id, opt.vector_id));

    dingodb::pb::common::VectorWithId vector_with_id;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0.0, 1.0);

    vector_with_id.set_id(opt.vector_id);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector_with_id.mutable_vector()->add_float_values(distrib(rng));
    }
    *mutation->mutable_vector() = vector_with_id;
  } else if (opt.mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(dingodb::VectorCodec::PackageVectorKey(perfix, part_id, opt.vector_id));
  } else if (opt.mutation_op == "check_not_exists") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::CheckNotExists);
    mutation->set_key(dingodb::VectorCodec::PackageVectorKey(perfix, part_id, opt.vector_id));
  } else if (opt.mutation_op == "insert") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(dingodb::VectorCodec::PackageVectorKey(perfix, part_id, opt.vector_id));

    dingodb::pb::common::VectorWithId vector_with_id;

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib(0.0, 1.0);

    vector_with_id.set_id(opt.vector_id);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector_with_id.mutable_vector()->add_float_values(distrib(rng));
    }
    *mutation->mutable_vector() = vector_with_id;
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
    return;
  }

  if (!opt.extra_data.empty()) {
    DINGO_LOG(INFO) << "extra_data is: " << opt.extra_data;
  }

  if (opt.for_update_ts > 0) {
    DINGO_LOG(INFO) << "for_update_ts > 0, do pessimistic check : " << opt.for_update_ts;
    for (int i = 0; i < request.mutations_size(); i++) {
      request.add_pessimistic_checks(::dingodb::pb::store::TxnPrewriteRequest_PessimisticCheck::
                                         TxnPrewriteRequest_PessimisticCheck_DO_PESSIMISTIC_CHECK);
      auto* for_update_ts_check = request.add_for_update_ts_checks();
      for_update_ts_check->set_expected_for_update_ts(opt.for_update_ts);
      for_update_ts_check->set_index(i);

      if (!opt.extra_data.empty()) {
        auto* extra_datas = request.add_lock_extra_datas();
        extra_datas->set_index(i);
        extra_datas->set_extra_data(opt.extra_data);
      }
    }
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void DocumentSendTxnPrewrite(TxnPrewriteOptions const& opt, const dingodb::pb::common::Region& region) {
  dingodb::pb::store::TxnPrewriteRequest request;
  dingodb::pb::store::TxnPrewriteResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (opt.rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  if (opt.primary_lock.empty()) {
    DINGO_LOG(ERROR) << "primary_lock is empty";
    return;
  }
  request.set_primary_lock(opt.primary_lock);

  if (opt.start_ts == 0) {
    DINGO_LOG(ERROR) << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.lock_ttl == 0) {
    DINGO_LOG(ERROR) << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(opt.lock_ttl);

  if (opt.txn_size == 0) {
    DINGO_LOG(ERROR) << "txn_size is empty";
    return;
  }
  request.set_txn_size(opt.txn_size);

  request.set_try_one_pc(opt.try_one_pc);
  request.set_max_commit_ts(opt.max_commit_ts);

  if (opt.mutation_op.empty()) {
    DINGO_LOG(ERROR) << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }

  if (opt.document_id == 0) {
    DINGO_LOG(ERROR) << "document_id is empty";
    return;
  }

  int64_t part_id = region.definition().part_id();
  int64_t dimension = 0;

  if (region.region_type() != dingodb::pb::common::RegionType::DOCUMENT_REGION) {
    DINGO_LOG(ERROR) << "region_type is invalid, only document region can use this function";
    return;
  }

  char prefix = dingodb::Helper::GetKeyPrefix(region.definition().range());
  if (opt.mutation_op == "put") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Put);

    mutation->set_key(dingodb::DocumentCodec::PackageDocumentKey(prefix, part_id, opt.document_id));

    dingodb::pb::common::DocumentWithId document_with_id;

    document_with_id.set_id(opt.document_id);
    auto* document = document_with_id.mutable_document();

    document_with_id.set_id(opt.document_id);
    auto* document_data = document_with_id.mutable_document()->mutable_document_data();

    if (opt.document_text1.empty()) {
      DINGO_LOG(ERROR) << "document_text1 is empty";
      return;
    }

    if (opt.document_text2.empty()) {
      DINGO_LOG(ERROR) << "document_text2 is empty";
      return;
    }

    // col1 text
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      document_value1.mutable_field_value()->set_string_data(opt.document_text1);
      (*document_data)["col1"] = document_value1;
    }

    // col2 int64
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
      document_value1.mutable_field_value()->set_long_data(opt.document_id);
      (*document_data)["col2"] = document_value1;
    }

    // col3 double
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
      document_value1.mutable_field_value()->set_double_data(opt.document_id * 1.0);
      (*document_data)["col3"] = document_value1;
    }

    // col4 text
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      document_value1.mutable_field_value()->set_string_data(opt.document_text2);
      (*document_data)["col4"] = document_value1;
    }

    *mutation->mutable_document() = document_with_id;
  } else if (opt.mutation_op == "delete") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Delete);
    mutation->set_key(dingodb::DocumentCodec::PackageDocumentKey(prefix, part_id, opt.document_id));
  } else if (opt.mutation_op == "check_not_exists") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::CheckNotExists);
    mutation->set_key(dingodb::DocumentCodec::PackageDocumentKey(prefix, part_id, opt.document_id));
  } else if (opt.mutation_op == "insert") {
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::PutIfAbsent);
    mutation->set_key(dingodb::DocumentCodec::PackageDocumentKey(prefix, part_id, opt.document_id));

    dingodb::pb::common::DocumentWithId document_with_id;

    document_with_id.set_id(opt.document_id);
    auto* document = document_with_id.mutable_document();

    document_with_id.set_id(opt.document_id);
    auto* document_data = document_with_id.mutable_document()->mutable_document_data();

    if (opt.document_text1.empty()) {
      DINGO_LOG(ERROR) << "document_text1 is empty";
      return;
    }

    if (opt.document_text2.empty()) {
      DINGO_LOG(ERROR) << "document_text2 is empty";
      return;
    }

    // col1 text
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      document_value1.mutable_field_value()->set_string_data(opt.document_text1);
      (*document_data)["col1"] = document_value1;
    }

    // col2 int64
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
      document_value1.mutable_field_value()->set_long_data(opt.document_id);
      (*document_data)["col2"] = document_value1;
    }

    // col3 double
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
      document_value1.mutable_field_value()->set_double_data(opt.document_id * 1.0);
      (*document_data)["col3"] = document_value1;
    }

    // col4 text
    {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      document_value1.mutable_field_value()->set_string_data(opt.document_text2);
      (*document_data)["col4"] = document_value1;
    }

    *mutation->mutable_document() = document_with_id;
  } else {
    DINGO_LOG(ERROR) << "mutation_op MUST be one of [put, delete, insert]";
    return;
  }

  if (!opt.extra_data.empty()) {
    DINGO_LOG(INFO) << "extra_data is: " << opt.extra_data;
  }

  if (opt.for_update_ts > 0) {
    DINGO_LOG(INFO) << "for_update_ts > 0, do pessimistic check : " << opt.for_update_ts;
    for (int i = 0; i < request.mutations_size(); i++) {
      request.add_pessimistic_checks(::dingodb::pb::store::TxnPrewriteRequest_PessimisticCheck::
                                         TxnPrewriteRequest_PessimisticCheck_DO_PESSIMISTIC_CHECK);
      auto* for_update_ts_check = request.add_for_update_ts_checks();
      for_update_ts_check->set_expected_for_update_ts(opt.for_update_ts);
      for_update_ts_check->set_index(i);

      if (!opt.extra_data.empty()) {
        auto* extra_datas = request.add_lock_extra_datas();
        extra_datas->set_index(i);
        extra_datas->set_extra_data(opt.extra_data);
      }
    }
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "TxnPrewrite", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

dingodb::pb::common::RegionDefinition BuildRegionDefinition(int64_t region_id, const std::string& raft_group,
                                                            std::vector<std::string>& raft_addrs,
                                                            const std::string& start_key, const std::string& end_key) {
  dingodb::pb::common::RegionDefinition region_definition;
  region_definition.set_id(region_id);
  region_definition.mutable_epoch()->set_conf_version(1);
  region_definition.mutable_epoch()->set_version(1);
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
struct AddRegionParam {
  int64_t start_region_id;
  int32_t region_count;
  std::string raft_group;
  int req_num;
  std::string prefix;

  std::vector<std::string> raft_addrs;
};

void* AdddRegionRoutine(void* arg) {
  std::unique_ptr<AddRegionParam> param(static_cast<AddRegionParam*>(arg));
  for (int i = 0; i < param->region_count; ++i) {
    dingodb::pb::debug::AddRegionRequest request;

    *(request.mutable_region()) =
        BuildRegionDefinition(param->start_region_id + i, param->raft_group, param->raft_addrs, "a", "z");

    dingodb::pb::debug::AddRegionResponse response;

    InteractionManager::GetInstance().AllSendRequestWithoutContext("DebugService", "AddRegion", request, response);

    bthread_usleep(3 * 1000 * 1000L);
  }

  return nullptr;
}

struct BatchPutGetParam {
  int64_t region_id;
  int32_t req_num;
  int32_t thread_no;
  std::string prefix;

  std::map<std::string, std::shared_ptr<dingodb::pb::store::StoreService_Stub>> stubs;
};

void BatchPutGet(int64_t region_id, const std::string& prefix, int req_num) {
  auto dataset = Helper::GenDataset(prefix, req_num);

  std::vector<int64_t> latencys;
  latencys.reserve(dataset.size());
  for (auto& [key, value] : dataset) {
    KvPutOptions opt;
    opt.region_id = region_id;
    opt.key = key;
    SendKvPut(opt, value);

    latencys.push_back(InteractionManager::GetInstance().GetLatency());
  }

  int64_t sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<int64_t>(0));
  DINGO_LOG(INFO) << "Put average latency: " << sum / latencys.size() << " us";

  latencys.clear();
  for (auto& [key, expect_value] : dataset) {
    std::string value;
    KvGetOptions opt;
    opt.key = key;
    opt.region_id = region_id;
    SendKvGet(opt, value);
    if (value != expect_value) {
      DINGO_LOG(INFO) << "Not match: " << key << " = " << value << " expected=" << expect_value;
    }
    latencys.push_back(InteractionManager::GetInstance().GetLatency());
  }

  sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<int64_t>(0));
  DINGO_LOG(INFO) << "Get average latency: " << sum / latencys.size() << " us";
}

void* OperationRegionRoutine(void* arg) {
  std::unique_ptr<AddRegionParam> param(static_cast<AddRegionParam*>(arg));

  bthread_usleep((Helper::GetRandInt() % 1000) * 1000L);
  for (int i = 0; i < param->region_count; ++i) {
    int64_t region_id = param->start_region_id + i;

    // Create region
    {
      DINGO_LOG(INFO) << "======Create region " << region_id;
      dingodb::pb::debug::AddRegionRequest request;
      *(request.mutable_region()) =
          BuildRegionDefinition(param->start_region_id + i, param->raft_group, param->raft_addrs, "a", "z");
      dingodb::pb::debug::AddRegionResponse response;

      InteractionManager::GetInstance().AllSendRequestWithoutContext("DebugService", "AddRegion", request, response);
    }

    // Put/Get
    {
      bthread_usleep(3 * 1000 * 1000L);
      DINGO_LOG(INFO) << "======Put region " << region_id;
      BatchPutGet(region_id, param->prefix, param->req_num);
    }

    // Destroy region
    {
      bthread_usleep(3 * 1000 * 1000L);
      DINGO_LOG(INFO) << "======Delete region " << region_id;
      dingodb::pb::debug::DestroyRegionRequest request;
      dingodb::pb::debug::DestroyRegionResponse response;

      request.set_region_id(region_id);

      InteractionManager::GetInstance().AllSendRequestWithoutContext("DebugService", "DestroyRegion", request,
                                                                     response);
    }

    bthread_usleep(1 * 1000 * 1000L);
  }

  return nullptr;
}

void BatchSendAddRegion(BatchAddRegionOptions const& opt) {
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);

  int32_t step = opt.region_count / opt.thread_num;
  std::vector<bthread_t> tids;
  tids.resize(opt.thread_num);
  for (int i = 0; i < opt.thread_num; ++i) {
    AddRegionParam* param = new AddRegionParam;
    param->start_region_id = opt.region_id + i * step;
    param->region_count = (i + 1 == opt.thread_num) ? opt.region_count - i * step : step;
    param->raft_group = opt.raft_group;
    param->raft_addrs = raft_addrs;

    if (bthread_start_background(&tids[i], nullptr, AdddRegionRoutine, param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < opt.thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void SendAddRegion(int64_t region_id, const std::string& raft_group, std::vector<std::string> raft_addrs) {
  dingodb::pb::debug::AddRegionRequest request;
  *(request.mutable_region()) = BuildRegionDefinition(region_id, raft_group, raft_addrs, "a", "z");
  dingodb::pb::debug::AddRegionResponse response;

  InteractionManager::GetInstance().AllSendRequestWithoutContext("DebugService", "AddRegion", request, response);
}

void SendChangeRegion(ChangeRegionOptions const& opt) {
  dingodb::pb::debug::ChangeRegionRequest request;
  dingodb::pb::debug::ChangeRegionResponse response;
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);

  *(request.mutable_region()) = BuildRegionDefinition(opt.region_id, opt.raft_group, raft_addrs, "a", "z");
  dingodb::pb::common::RegionDefinition* region = request.mutable_region();

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "ChangeRegion", request, response);
}

void SendMergeRegion(MergeRegionAtStoreOptions const& opt) {
  if (opt.source_id == 0 || opt.target_id == 0) {
    DINGO_LOG(INFO) << "Param source_region_id/target_region_id is error.";
    return;
  }
  dingodb::pb::debug::MergeRegionRequest request;
  dingodb::pb::debug::MergeRegionResponse response;

  request.set_source_region_id(opt.source_id);
  request.set_target_region_id(opt.target_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "MergeRegion", request, response);
}

void SendDestroyRegion(DestroyRegionOptions const& opt) {
  dingodb::pb::debug::DestroyRegionRequest request;
  dingodb::pb::debug::DestroyRegionResponse response;

  request.set_region_id(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "DestroyRegion", request, response);
}

void SendSnapshot(SnapshotOptions const& opt) {
  dingodb::pb::debug::SnapshotRequest request;
  dingodb::pb::debug::SnapshotResponse response;

  request.set_region_id(opt.region_id);

  auto status =
      InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "Snapshot", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "region " << opt.region_id << " do  snapshot failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
  }
  std::cout << "region " << opt.region_id << " do snapshot success " << std::endl;
}

void SendSnapshotVectorIndex(SnapshotVectorIndexOptions const& opt) {
  dingodb::pb::debug::SnapshotVectorIndexRequest request;
  dingodb::pb::debug::SnapshotVectorIndexResponse response;

  request.set_vector_index_id(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "SnapshotVectorIndex", request, response);
}

void SendCompact(const std::string& cf_name) {
  dingodb::pb::debug::CompactRequest request;
  dingodb::pb::debug::CompactResponse response;

  request.set_cf_name(cf_name);

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "Compact", request, response);
}

void GetMemoryStats() {
  dingodb::pb::debug::GetMemoryStatsRequest request;
  dingodb::pb::debug::GetMemoryStatsResponse response;

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "GetMemoryStats", request, response);

  DINGO_LOG(INFO) << response.memory_stats();
}

void ReleaseFreeMemory(ReleaseFreeMemoryOptions const& opt) {
  dingodb::pb::debug::ReleaseFreeMemoryRequest request;
  dingodb::pb::debug::ReleaseFreeMemoryResponse response;

  if (opt.rate < 0.00001) {
    request.set_is_force(true);
  } else {
    request.set_is_force(false);
    request.set_rate(opt.rate);
  }

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "ReleaseFreeMemory", request, response);
}

void SendTransferLeader(int64_t region_id, const dingodb::pb::common::Peer& peer) {
  dingodb::pb::debug::TransferLeaderRequest request;
  dingodb::pb::debug::TransferLeaderResponse response;

  request.set_region_id(region_id);
  *(request.mutable_peer()) = peer;

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "TransferLeader", request, response);
}

void SendTransferLeaderByCoordinator(int64_t region_id, int64_t leader_store_id) {
  dingodb::pb::coordinator::TransferLeaderRegionRequest request;
  dingodb::pb::coordinator::TransferLeaderRegionResponse response;

  request.set_region_id(region_id);
  request.set_leader_store_id(leader_store_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "TransferLeaderRegion", request,
                                                              response);
}

void SendMergeRegionToCoor(int64_t source_id, int64_t target_id) {
  dingodb::pb::coordinator::MergeRegionRequest request;
  dingodb::pb::coordinator::MergeRegionResponse response;
  if (source_id == 0 || target_id == 0) {
    DINGO_LOG(INFO) << fmt::format("source_id/target_id is 0");
    return;
  }

  request.mutable_merge_request()->set_source_region_id(source_id);
  request.mutable_merge_request()->set_target_region_id(target_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "MergeRegion", request, response);
}

uint32_t SendGetTaskList() {
  dingodb::pb::coordinator::GetTaskListRequest request;
  dingodb::pb::coordinator::GetTaskListResponse response;

  InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "GetTaskList", request, response);

  return response.task_lists().size();
}

bool QueryRegionIdByKey(dingodb::pb::meta::TableRange& table_range, const std::string& key, int64_t& region_id) {
  for (const auto& item : table_range.range_distribution()) {
    const auto& range = item.range();
    if (key >= range.start_key() && key < range.end_key()) {
      region_id = item.id().entity_id();
      return true;
    }
  }

  DINGO_LOG(ERROR) << fmt::format("query region id by key failed, key {}", dingodb::Helper::StringToHex(key));

  return false;
}

void PrintTableRange(dingodb::pb::meta::TableRange& table_range) {
  DINGO_LOG(INFO) << "refresh route...";
  for (const auto& item : table_range.range_distribution()) {
    DINGO_LOG(INFO) << fmt::format("region {} range [{}-{})", item.id().entity_id(),
                                   dingodb::Helper::StringToHex(item.range().start_key()),
                                   dingodb::Helper::StringToHex(item.range().end_key()));
  }
}

void TestBatchPutGet(TestBatchPutGetOptions const& opt) {
  std::vector<bthread_t> tids;
  tids.resize(opt.thread_num);
  for (int i = 0; i < opt.thread_num; ++i) {
    BatchPutGetParam* param = new BatchPutGetParam;
    param->req_num = opt.req_num;
    param->region_id = opt.region_id;
    param->thread_no = i;
    param->prefix = dingodb::Helper::HexToString(opt.prefix);

    if (bthread_start_background(
            &tids[i], nullptr,
            [](void* arg) -> void* {
              std::unique_ptr<BatchPutGetParam> param(static_cast<BatchPutGetParam*>(arg));

              LOG(INFO) << "========thread: " << param->thread_no;
              BatchPutGet(param->region_id, param->prefix, param->req_num);

              return nullptr;
            },
            param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < opt.thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void TestRegionLifecycle(TestRegionLifecycleOptions const& opt) {
  int32_t step = opt.region_count / opt.thread_num;
  std::vector<bthread_t> tids;
  tids.resize(opt.thread_num);
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);
  for (int i = 0; i < opt.thread_num; ++i) {
    AddRegionParam* param = new AddRegionParam;
    param->start_region_id = opt.region_id + i * step;
    param->region_count = (i + 1 == opt.thread_num) ? opt.region_count - i * step : step;
    param->raft_addrs = raft_addrs;
    param->raft_group = opt.raft_group;
    param->req_num = opt.req_num;
    param->prefix = opt.prefix;

    if (bthread_start_background(&tids[i], nullptr, OperationRegionRoutine, param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < opt.thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void TestDeleteRangeWhenTransferLeader(TestDeleteRangeWhenTransferLeaderOptions const& opt) {
  // put data
  DINGO_LOG(INFO) << "batch put...";
  // BatchPut( region_id, prefix, req_num);

  // transfer leader
  dingodb::pb::common::Peer new_leader_peer;
  auto region = SendQueryRegion(opt.region_id);
  for (const auto& peer : region.definition().peers()) {
    if (region.leader_store_id() != peer.store_id()) {
      new_leader_peer = peer;
    }
  }

  DINGO_LOG(INFO) << fmt::format("transfer leader {}:{}", new_leader_peer.raft_location().host(),
                                 new_leader_peer.raft_location().port());
  SendTransferLeader(opt.region_id, new_leader_peer);

  // delete range
  DINGO_LOG(INFO) << "delete range...";
  KvDeleteRangeOptions delete_opt;
  delete_opt.region_id = opt.region_id;
  delete_opt.prefix = opt.prefix;
  SendKvDeleteRange(delete_opt);
  KvScanOptions scan_opt;
  scan_opt.region_id = opt.region_id;
  scan_opt.prefix = opt.prefix;
  // scan data
  DINGO_LOG(INFO) << "scan...";
  SendKvScan(scan_opt);
}

dingodb::pb::common::StoreMap SendGetStoreMap() {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(1);

  InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "GetStoreMap", request, response);

  return response.storemap();
}

void SendChangePeer(const dingodb::pb::common::RegionDefinition& region_definition) {
  dingodb::pb::coordinator::ChangePeerRegionRequest request;
  dingodb::pb::coordinator::ChangePeerRegionResponse response;

  auto* mut_definition = request.mutable_change_peer_request()->mutable_region_definition();
  *mut_definition = region_definition;

  InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "ChangePeerRegion", request,
                                                              response);
}

void SendSplitRegion(const dingodb::pb::common::RegionDefinition& region_definition) {
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

  InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "SplitRegion", request, response);
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

bool IsSamePartition(dingodb::pb::common::Range source_range, dingodb::pb::common::Range target_range) {
  return dingodb::VectorCodec::UnPackagePartitionId(source_range.end_key()) ==
         dingodb::VectorCodec::UnPackagePartitionId(target_range.start_key());
}

void AutoMergeRegion(AutoMergeRegionOptions const& opt) {
  for (;;) {
    ::google::protobuf::RepeatedPtrField<::dingodb::pb::meta::RangeDistribution> range_dists;
    if (opt.table_id > 0) {
      auto table_range = SendGetTableRange(opt.table_id);
      range_dists = table_range.range_distribution();
    } else {
      auto index_range = SendGetIndexRange(opt.index_id);
      range_dists = index_range.range_distribution();
    }

    DINGO_LOG(INFO) << fmt::format("Table/index region count {}", range_dists.size());

    for (int i = 0; i < range_dists.size() - 1; ++i) {
      const auto& source_range_dist = range_dists.at(i);
      const auto& target_range_dist = range_dists.at(i + 1);
      int64_t source_region_id = source_range_dist.id().entity_id();
      if (source_region_id == 0) {
        DINGO_LOG(INFO) << fmt::format("Get range failed, region_id: {}", source_region_id);
        continue;
      }
      int64_t target_region_id = target_range_dist.id().entity_id();
      if (source_region_id == 0) {
        DINGO_LOG(INFO) << fmt::format("Get range failed, region_id: {}", target_region_id);
        continue;
      }

      if (source_range_dist.status().state() != dingodb::pb::common::RegionState::REGION_NORMAL ||
          target_range_dist.status().state() != dingodb::pb::common::RegionState::REGION_NORMAL) {
        DINGO_LOG(INFO) << fmt::format("Region state is not NORMAL, region state {}/{} {}/{}", source_region_id,
                                       dingodb::pb::common::RegionState_Name(source_range_dist.status().state()),
                                       target_region_id,
                                       dingodb::pb::common::RegionState_Name(target_range_dist.status().state())

        );
        continue;
      }

      if (!IsSamePartition(source_range_dist.range(), target_range_dist.range())) {
        DINGO_LOG(INFO) << fmt::format("Not same partition, region_id: {} {}", source_region_id, target_region_id);
        continue;
      }

      auto task_num = SendGetTaskList();
      if (task_num > 0) {
        DINGO_LOG(INFO) << fmt::format("Exist task, task num: {}", task_num);
        continue;
      }

      DINGO_LOG(INFO) << fmt::format("Launch merge region {} -> {}", source_region_id, target_region_id);
      SendMergeRegionToCoor(source_region_id, target_region_id);
    }

    bthread_usleep(1000 * 1000 * 10);
  }
}

void AutoDropTable(AutoDropTableOptions const& opt) {
  // Get all table
  auto table_ids = SendGetTablesBySchema();
  DINGO_LOG(INFO) << "table nums: " << table_ids.size();

  // Drop table
  std::sort(table_ids.begin(), table_ids.end());
  for (int i = 0; i < opt.req_num && i < table_ids.size(); ++i) {
    DINGO_LOG(INFO) << "Delete table: " << table_ids[i];
    SendDropTable(table_ids[i]);
  }
}

void CheckTableDistribution(CheckTableDistributionOptions const& opt) {
  auto table_range = SendGetTableRange(opt.table_id);

  std::map<std::string, dingodb::pb::meta::RangeDistribution> region_map;
  for (const auto& region_range : table_range.range_distribution()) {
    if (region_range.range().start_key() >= region_range.range().end_key()) {
      DINGO_LOG(ERROR) << fmt::format("Invalid region {} range [{}-{})", region_range.id().entity_id(),
                                      dingodb::Helper::StringToHex(region_range.range().start_key()),
                                      dingodb::Helper::StringToHex(region_range.range().end_key()));
    }

    auto it = region_map.find(region_range.range().start_key());
    if (it == region_map.end()) {
      region_map[region_range.range().start_key()] = region_range;
    } else {
      auto& tmp_region_range = it->second;
      DINGO_LOG(ERROR) << fmt::format(
          "Already exist region {} [{}-{}) curr region {} [{}-{})", tmp_region_range.id().entity_id(),
          dingodb::Helper::StringToHex(tmp_region_range.range().start_key()),
          dingodb::Helper::StringToHex(tmp_region_range.range().end_key()), region_range.id().entity_id(),
          dingodb::Helper::StringToHex(region_range.range().start_key()),
          dingodb::Helper::StringToHex(region_range.range().end_key()));
    }

    if (!opt.key.empty()) {
      if (opt.key >= dingodb::Helper::StringToHex(region_range.range().start_key()) &&
          opt.key < dingodb::Helper::StringToHex(region_range.range().end_key())) {
        DINGO_LOG(INFO) << fmt::format("key({}) at region {}", opt.key, region_range.id().entity_id());
      }
    }
  }

  std::string key;
  for (auto& [_, region_range] : region_map) {
    DINGO_LOG(INFO) << fmt::format("region {} range [{}-{})", region_range.id().entity_id(),
                                   dingodb::Helper::StringToHex(region_range.range().start_key()),
                                   dingodb::Helper::StringToHex(region_range.range().end_key()));
    if (!key.empty()) {
      if (key != region_range.range().start_key()) {
        DINGO_LOG(ERROR) << fmt::format("not continuous range, region {} [{}-{})", region_range.id().entity_id(),
                                        dingodb::Helper::StringToHex(region_range.range().start_key()),
                                        dingodb::Helper::StringToHex(region_range.range().end_key()));
      }
    }
    key = region_range.range().end_key();
  }
}

void CheckIndexDistribution(CheckIndexDistributionOptions const& opt) {
  auto index_range = SendGetIndexRange(opt.table_id);

  std::map<std::string, dingodb::pb::meta::RangeDistribution> region_map;
  for (const auto& region_range : index_range.range_distribution()) {
    if (region_range.range().start_key() >= region_range.range().end_key()) {
      DINGO_LOG(ERROR) << fmt::format("Invalid region {} range [{}-{})", region_range.id().entity_id(),
                                      dingodb::Helper::StringToHex(region_range.range().start_key()),
                                      dingodb::Helper::StringToHex(region_range.range().end_key()));
      continue;
    }

    auto it = region_map.find(region_range.range().start_key());
    if (it == region_map.end()) {
      region_map[region_range.range().start_key()] = region_range;
    } else {
      auto& tmp_region_range = it->second;
      DINGO_LOG(ERROR) << fmt::format(
          "Already exist region {} [{}-{}) curr region {} [{}-{})", tmp_region_range.id().entity_id(),
          dingodb::Helper::StringToHex(tmp_region_range.range().start_key()),
          dingodb::Helper::StringToHex(tmp_region_range.range().end_key()), region_range.id().entity_id(),
          dingodb::Helper::StringToHex(region_range.range().start_key()),
          dingodb::Helper::StringToHex(region_range.range().end_key()));
    }
  }

  std::string key;
  for (auto& [_, region_range] : region_map) {
    DINGO_LOG(INFO) << fmt::format("region {} range [{}-{})", region_range.id().entity_id(),
                                   dingodb::Helper::StringToHex(region_range.range().start_key()),
                                   dingodb::Helper::StringToHex(region_range.range().end_key()));
    if (!key.empty()) {
      if (key != region_range.range().start_key()) {
        DINGO_LOG(ERROR) << fmt::format("not continuous range, region {} [{}-{})", region_range.id().entity_id(),
                                        dingodb::Helper::StringToHex(region_range.range().start_key()),
                                        dingodb::Helper::StringToHex(region_range.range().end_key()));
      }
    }
    key = region_range.range().end_key();
  }
}

}  // namespace client_v2