
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
#include <iostream>
#include <string>

#include "client_v2/client_helper.h"
#include "client_v2/store_tool_dump.h"
#include "client_v2/subcommand_helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "proto/version.pb.h"
#include "subcommand_coordinator.h"
namespace client_v2 {

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

void SetUpSubcommandAddRegion(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandAddRegion(*opt); });
}

void RunSubcommandAddRegion(AddRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  std::string value;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);
  dingodb::pb::debug::AddRegionRequest request;
  *(request.mutable_region()) = BuildRegionDefinitionV2(opt.region_id, opt.raft_group, raft_addrs, "a", "z");
  dingodb::pb::debug::AddRegionResponse response;

  InteractionManager::GetInstance().AllSendRequestWithoutContext("DebugService", "AddRegion", request, response);
}

void SetUpSubcommandKvGet(CLI::App& app) {
  auto opt = std::make_shared<KvGetOptions>();
  auto coor = app.add_subcommand("KvGet", "Kv get")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandKvGet(*opt); });
}

void RunSubcommandKvGet(KvGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  std::string value;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvGet(opt.region_id, dingodb::Helper::HexToString(opt.key), value);
  DINGO_LOG(INFO) << "value: " << value;
  std::cout << "value: " << value << std::endl;
}

void SetUpSubcommandKvPut(CLI::App& app) {
  auto opt = std::make_shared<KvPutOptions>();
  auto coor = app.add_subcommand("KvPut", "Kv put")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--value", opt->value, "Request parameter value")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandKvPut(*opt); });
}

void RunSubcommandKvPut(KvPutOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  std::string value = opt.value.empty() ? client_v2::Helper::GenRandomString(256) : opt.value;
  client_v2::SendKvPut(opt.region_id, dingodb::Helper::HexToString(opt.key), value);
  DINGO_LOG(INFO) << "value:" << value;
  std::cout << "value:" << value << std::endl;
}

void SetUpSubcommandChangeRegion(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandChangeRegion(*opt); });
}

void RunSubcommandChangeRegion(ChangeRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);
  client_v2::SendChangeRegion(opt.region_id, opt.raft_group, raft_addrs);
}

void SetUpSubcommandMergeRegionAtStore(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandMergeRegionAtStore(*opt); });
}

void RunSubcommandMergeRegionAtStore(MergeRegionAtStoreOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, 0, opt.source_id)) {
    DINGO_LOG(ERROR) << "Set Up failed source_id=" << opt.source_id;
    std::cout << "Set Up failed source_id=" << opt.source_id;
    exit(-1);
  }
  client_v2::SendMergeRegion(opt.source_id, opt.target_id);
}

void SetUpSubcommandDestroyRegion(CLI::App& app) {
  auto opt = std::make_shared<DestroyRegionOptions>();
  auto coor = app.add_subcommand("DestroyRegion", "Destroy region")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandDestroyRegion(*opt); });
}

void RunSubcommandDestroyRegion(DestroyRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDestroyRegion(opt.region_id);
}

void SetUpSubcommandSnapshot(CLI::App& app) {
  auto opt = std::make_shared<SnapshotOptions>();
  auto coor = app.add_subcommand("Snapshot", "Snapshot")->group("Region Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  coor->callback([opt]() { RunSubcommandSnapshot(*opt); });
}

void RunSubcommandSnapshot(SnapshotOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendSnapshot(opt.region_id);
}

void SetUpSubcommandBatchAddRegion(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandBatchAddRegion(*opt); });
}

void RunSubcommandBatchAddRegion(BatchAddRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);
  client_v2::BatchSendAddRegion(opt.region_id, opt.region_count, opt.thread_num, opt.raft_group, raft_addrs);
}

void SetUpSubcommandSnapshotVectorIndex(CLI::App& app) {
  auto opt = std::make_shared<SnapshotVectorIndexOptions>();
  auto coor = app.add_subcommand("SnapshotVectorIndex", "Snapshot vector index")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandSnapshotVectorIndex(*opt); });
}

void RunSubcommandSnapshotVectorIndex(SnapshotVectorIndexOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendSnapshotVectorIndex(opt.region_id);
}

void SetUpSubcommandCompact(CLI::App& app) {
  auto opt = std::make_shared<CompactOptions>();
  auto coor = app.add_subcommand("Compact", "Compact ")->group("Store Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandCompact(*opt); });
}

void RunSubcommandCompact(CompactOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed store_addrs=" << opt.store_addrs;
    std::cout << "Set Up failed store_addrs=" << opt.store_addrs;
    exit(-1);
  }
  client_v2::SendCompact("");
}

void SetUpSubcommandGetMemoryStats(CLI::App& app) {
  auto opt = std::make_shared<GetMemoryStatsOptions>();
  auto coor = app.add_subcommand("GetMemoryStats", "GetMemory stats ")->group("Store Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandGetMemoryStats(*opt); });
}

void RunSubcommandGetMemoryStats(GetMemoryStatsOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed store_addrs=" << opt.store_addrs;
    std::cout << "Set Up failed store_addrs=" << opt.store_addrs;
    exit(-1);
  }
  client_v2::GetMemoryStats();
}

void SetUpSubcommandReleaseFreeMemory(CLI::App& app) {
  auto opt = std::make_shared<ReleaseFreeMemoryOptions>();
  auto coor = app.add_subcommand("ReleaseFreeMemory", "Release free memory ")->group("Store Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--rate", opt->rate, "server addrs")->default_val(0.0)->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandReleaseFreeMemory(*opt); });
}

void RunSubcommandReleaseFreeMemory(ReleaseFreeMemoryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed store_addrs=" << opt.store_addrs;
    std::cout << "Set Up failed store_addrs=" << opt.store_addrs;
    exit(-1);
  }
  client_v2::ReleaseFreeMemory(opt.rate);
}

void SetUpSubcommandKvBatchGet(CLI::App& app) {
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

  coor->callback([opt]() { RunSubcommandKvBatchGet(*opt); });
}

void RunSubcommandKvBatchGet(KvBatchGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvBatchGet(opt.region_id, opt.prefix, opt.req_num);
}

void SetUpSubcommandKvBatchPut(CLI::App& app) {
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

  coor->callback([opt]() { RunSubcommandKvBatchPut(*opt); });
}

void RunSubcommandKvBatchPut(KvBatchPutOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvBatchPut(opt.region_id, dingodb::Helper::HexToString(opt.prefix), opt.count);
}

void SetUpSubcommandKvPutIfAbsent(CLI::App& app) {
  auto opt = std::make_shared<KvPutIfAbsentOptions>();
  auto coor = app.add_subcommand("KvPutIfAbsent", "Kv put if absent")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter prefix")->required()->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandKvPutIfAbsent(*opt); });
}

void RunSubcommandKvPutIfAbsent(KvPutIfAbsentOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvPutIfAbsent(opt.region_id, dingodb::Helper::HexToString(opt.key));
}

void SetUpSubcommandKvBatchPutIfAbsent(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandKvBatchPutIfAbsent(*opt); });
}

void RunSubcommandKvBatchPutIfAbsent(KvBatchPutIfAbsentOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvBatchPutIfAbsent(opt.region_id, dingodb::Helper::HexToString(opt.prefix), opt.count);
}

void SetUpSubcommandKvBatchDelete(CLI::App& app) {
  auto opt = std::make_shared<KvBatchDeleteOptions>();
  auto coor = app.add_subcommand("KvBatchDelete", "Kv batch delete")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandKvBatchDelete(*opt); });
}

void RunSubcommandKvBatchDelete(KvBatchDeleteOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvBatchDelete(opt.region_id, dingodb::Helper::HexToString(opt.key));
}

void SetUpSubcommandKvDeleteRange(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandKvDeleteRange(*opt); });
}

void RunSubcommandKvDeleteRange(KvDeleteRangeOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvDeleteRange(opt.region_id, opt.prefix);
}

void SetUpSubcommandKvScan(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandKvScan(*opt); });
}

void RunSubcommandKvScan(KvScanOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvScan(opt.region_id, opt.prefix);
}

void SetUpSubcommandKvCompareAndSet(CLI::App& app) {
  auto opt = std::make_shared<KvCompareAndSetOptions>();
  auto coor = app.add_subcommand("KvCompareAndSet", "Kv compare and set")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandKvCompareAndSet(*opt); });
}

void RunSubcommandKvCompareAndSet(KvCompareAndSetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvCompareAndSet(opt.region_id, opt.key);
}

void SetUpSubcommandKvBatchCompareAndSet(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandKvBatchCompareAndSet(*opt); });
}

void RunSubcommandKvBatchCompareAndSet(KvBatchCompareAndSetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendKvBatchCompareAndSet(opt.region_id, opt.prefix, opt.count);
}

void SetUpSubcommandKvScanBeginV2(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandKvScanBeginV2(*opt); });
}

void RunSubcommandKvScanBeginV2(KvScanBeginV2Options const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvScanBeginV2(opt.region_id, opt.scan_id);
}

void SetUpSubcommandKvScanContinueV2(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandKvScanContinueV2(*opt); });
}

void RunSubcommandKvScanContinueV2(KvScanContinueV2Options const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvScanContinueV2(opt.region_id, opt.scan_id);
}

void SetUpSubcommandKvScanReleaseV2(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandKvScanReleaseV2(*opt); });
}

void RunSubcommandKvScanReleaseV2(KvScanReleaseV2Options const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendKvScanReleaseV2(opt.region_id, opt.scan_id);
}

void SetUpSubcommandTxnGet(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnGet(*opt); });
}

void RunSubcommandTxnGet(TxnGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnGet(opt.region_id, opt.rc, opt.key, opt.key_is_hex, opt.start_ts, opt.resolve_locks);
}

void SetUpSubcommandTxnScan(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnScan(*opt); });
}

void RunSubcommandTxnScan(TxnScanOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnScan(opt.region_id, opt.rc, opt.start_key, opt.end_key, opt.limit, opt.start_ts, opt.is_reverse,
                         opt.key_only, opt.resolve_locks, opt.key_is_hex, opt.with_start, opt.with_end);
}

void SetUpSubcommandTxnPessimisticLock(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnPessimisticLock(*opt); });
}

void RunSubcommandTxnPessimisticLock(TxnPessimisticLockOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnPessimisticLock(opt.region_id, opt.rc, opt.primary_lock, opt.key_is_hex, opt.start_ts, opt.lock_ttl,
                                    opt.for_update_ts, opt.mutation_op, opt.key, opt.value, opt.value_is_hex);
}

void SetUpSubcommandTxnPessimisticRollback(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnPessimisticRollback(*opt); });
}

void RunSubcommandTxnPessimisticRollback(TxnPessimisticRollbackOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnPessimisticRollback(opt.region_id, opt.rc, opt.start_ts, opt.for_update_ts, opt.key,
                                        opt.key_is_hex);
}

void SetUpSubcommandTxnPrewrite(CLI::App& app) {
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

  coor->callback([opt]() { RunSubcommandTxnPrewrite(*opt); });
}

void RunSubcommandTxnPrewrite(TxnPrewriteOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnPrewrite(opt.region_id, opt.rc, opt.primary_lock, opt.key_is_hex, opt.start_ts, opt.lock_ttl,
                             opt.txn_size, opt.try_one_pc, opt.max_commit_ts, opt.mutation_op, opt.key, opt.key2,
                             opt.value, opt.value2, opt.value_is_hex, opt.extra_data, opt.for_update_ts, opt.vector_id,
                             opt.document_id, opt.document_text1, opt.document_text2);
}

void SetUpSubcommandTxnCommit(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnCommit(*opt); });
}

void RunSubcommandTxnCommit(TxnCommitOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnCommit(opt.region_id, opt.rc, opt.start_ts, opt.commit_ts, opt.key, opt.key2, opt.key_is_hex);
}

void SetUpSubcommandTxnCheckTxnStatus(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnCheckTxnStatus(*opt); });
}

void RunSubcommandTxnCheckTxnStatus(TxnCheckTxnStatusOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnCheckTxnStatus(opt.region_id, opt.rc, opt.primary_key, opt.key_is_hex, opt.lock_ts,
                                   opt.caller_start_ts, opt.current_ts);
}

void SetUpSubcommandTxnResolveLock(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnResolveLock(*opt); });
}

void RunSubcommandTxnResolveLock(TxnResolveLockOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnResolveLock(opt.region_id, opt.rc, opt.start_ts, opt.commit_ts, opt.key, opt.key_is_hex);
}

void SetUpSubcommandTxnBatchGet(CLI::App& app) {
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

  coor->callback([opt]() { RunSubcommandTxnBatchGet(*opt); });
}

void RunSubcommandTxnBatchGet(TxnBatchGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnBatchGet(opt.region_id, opt.rc, opt.key, opt.key2, opt.key_is_hex, opt.start_ts, opt.resolve_locks);
}

void SetUpSubcommandTxnBatchRollback(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnBatchRollback(*opt); });
}

void RunSubcommandTxnBatchRollback(TxnBatchRollbackOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnBatchRollback(opt.region_id, opt.rc, opt.key, opt.key2, opt.key_is_hex, opt.start_ts);
}

void SetUpSubcommandTxnScanLock(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnScanLock(*opt); });
}

void RunSubcommandTxnScanLock(TxnScanLockOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnScanLock(opt.region_id, opt.rc, opt.max_ts, opt.start_key, opt.end_key, opt.key_is_hex, opt.limit);
}

void SetUpSubcommandTxnHeartBeat(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnHeartBeat(*opt); });
}

void RunSubcommandTxnHeartBeat(TxnHeartBeatOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnHeartBeat(opt.region_id, opt.rc, opt.primary_lock, opt.start_ts, opt.advise_lock_ttl,
                              opt.key_is_hex);
}

void SetUpSubcommandTxnGC(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnGC(*opt); });
}

void RunSubcommandTxnGC(TxnGCOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnGc(opt.region_id, opt.rc, opt.safe_point_ts);
}

void SetUpSubcommandTxnDeleteRange(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTxnDeleteRange(*opt); });
}

void RunSubcommandTxnDeleteRange(TxnDeleteRangeOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnDeleteRange(opt.region_id, opt.rc, opt.start_key, opt.end_key, opt.key_is_hex);
}

void SetUpSubcommandTxnDump(CLI::App& app) {
  auto opt = std::make_shared<TxnDumpOptions>();
  auto coor = app.add_subcommand("TxnDump", "Txn dump")->group("Tool Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  coor->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  coor->add_option("--start_key", opt->start_key, "Request parameter start_key")->required();
  coor->add_option("--end_key", opt->end_key, "Request parameter end_key")->required();
  coor->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")->default_val(false);
  coor->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  coor->add_option("--end_ts", opt->end_ts, "Request parameter end_ts")->required();
  coor->callback([opt]() { RunSubcommandTxnDump(*opt); });
}

void RunSubcommandTxnDump(TxnDumpOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendTxnDump(opt.region_id, opt.rc, opt.start_key, opt.end_key, opt.key_is_hex, opt.start_ts, opt.end_ts);
}

void SetUpSubcommandDocumentDelete(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandDocumentDelete(*opt); });
}

void RunSubcommandDocumentDelete(DocumentDeleteOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDocumentDelete(opt.region_id, opt.start_id, opt.count);
}

void SetUpSubcommandDocumentAdd(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandDocumentAdd(*opt); });
}

void RunSubcommandDocumentAdd(DocumentAddOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDocumentAdd(opt.region_id, opt.document_id, opt.document_text1, opt.document_text2, opt.is_update);
}

void SetUpSubcommandDocumentSearch(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandDocumentSearch(*opt); });
}

void RunSubcommandDocumentSearch(DocumentSearchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDocumentSearch(opt.region_id, opt.query_string, opt.topn, opt.without_scalar);
}

void SetUpSubcommandDocumentBatchQuery(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandDocumentBatchQuery(*opt); });
}

void RunSubcommandDocumentBatchQuery(DocumentBatchQueryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDocumentBatchQuery(opt.region_id, {static_cast<int64_t>(opt.document_id)}, opt.without_scalar,
                                    opt.key);
}

void SetUpSubcommandDocumentScanQuery(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandDocumentScanQuery(*opt); });
}

void RunSubcommandDocumentScanQuery(DocumentScanQueryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDocumentScanQuery(opt.region_id, opt.start_id, opt.end_id, opt.limit, opt.is_reverse,
                                   opt.without_scalar, opt.key);
}

void SetUpSubcommandDocumentGetMaxId(CLI::App& app) {
  auto opt = std::make_shared<DocumentGetMaxIdOptions>();
  auto coor = app.add_subcommand("DocumentGetMaxId", "Document get max id ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandDocumentGetMaxId(*opt); });
}

void RunSubcommandDocumentGetMaxId(DocumentGetMaxIdOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDocumentGetMaxId(opt.region_id);
}

void SetUpSubcommandDocumentGetMinId(CLI::App& app) {
  auto opt = std::make_shared<DocumentGetMinIdOptions>();
  auto coor = app.add_subcommand("DocumentGetMinId", "Document get min id ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandDocumentGetMinId(*opt); });
}

void RunSubcommandDocumentGetMinId(DocumentGetMinIdOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDocumentGetMinId(opt.region_id);
}

void SetUpSubcommandDocumentCount(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandDocumentCount(*opt); });
}

void RunSubcommandDocumentCount(DocumentCountOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDocumentCount(opt.region_id, opt.start_id, opt.end_id);
}

void SetUpSubcommandDocumentGetRegionMetrics(CLI::App& app) {
  auto opt = std::make_shared<DocumentGetRegionMetricsOptions>();
  auto coor =
      app.add_subcommand("DocumentGetRegionMetrics", "Document get region metrics ")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandDocumentGetRegionMetrics(*opt); });
}

void RunSubcommandDocumentGetRegionMetrics(DocumentGetRegionMetricsOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendDocumentGetRegionMetrics(opt.region_id);
}

void SetUpSubcommandVectorSearch(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandVectorSearch(*opt); });
}

void RunSubcommandVectorSearch(VectorSearchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendVectorSearch(opt.region_id, opt.dimension, opt.topn, opt.vector_data, opt.key, opt.without_vector,
                              opt.without_scalar, opt.without_table, opt.with_vector_ids, opt.with_scalar_pre_filter,
                              opt.with_table_pre_filter, opt.scalar_filter_key, opt.scalar_filter_value,
                              opt.scalar_filter_key2, opt.scalar_filter_value2, opt.with_scalar_post_filter,
                              opt.ef_search, opt.bruteforce, opt.print_vector_search_delay, opt.csv_output);
}

void SetUpSubcommandVectorSearchDebug(CLI::App& app) {
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

  coor->callback([opt]() { RunSubcommandVectorSearchDebug(*opt); });
}

void RunSubcommandVectorSearchDebug(VectorSearchDebugOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendVectorSearchDebug(opt.region_id, opt.dimension, opt.start_vector_id, opt.topn, opt.batch_count,
                                   opt.key, opt.value, opt.without_vector, opt.without_scalar, opt.without_table,
                                   opt.with_vector_ids, opt.vector_ids_count, opt.with_scalar_pre_filter,
                                   opt.with_scalar_post_filter, opt.print_vector_search_delay);
}

void SetUpSubcommandVectorRangeSearch(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandVectorRangeSearch(*opt); });
}

void RunSubcommandVectorRangeSearch(VectorRangeSearchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendVectorRangeSearch(
      opt.region_id, opt.dimension, opt.radius, opt.key, opt.without_vector, opt.without_scalar, opt.without_table,
      opt.with_vector_ids, opt.with_scalar_pre_filter, opt.with_scalar_post_filter, opt.print_vector_search_delay);
}

void SetUpSubcommandVectorRangeSearchDebug(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandVectorRangeSearchDebug(*opt); });
}

void RunSubcommandVectorRangeSearchDebug(VectorRangeSearchDebugOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendVectorRangeSearchDebug(opt.region_id, opt.dimension, opt.start_vector_id, opt.radius, opt.batch_count,
                                        opt.key, opt.value, opt.without_vector, opt.without_scalar, opt.without_table,
                                        opt.with_vector_ids, opt.vector_ids_count, opt.with_scalar_pre_filter,
                                        opt.with_scalar_post_filter, opt.print_vector_search_delay);
}

void SetUpSubcommandVectorBatchSearch(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandVectorBatchSearch(*opt); });
}

void RunSubcommandVectorBatchSearch(VectorBatchSearchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendVectorBatchSearch(opt.region_id, opt.dimension, opt.topn, opt.batch_count, opt.key, opt.without_vector,
                                   opt.without_scalar, opt.without_table, opt.with_vector_ids,
                                   opt.with_scalar_pre_filter, opt.with_scalar_post_filter,
                                   opt.print_vector_search_delay);
}

void SetUpSubcommandVectorBatchQuery(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandVectorBatchQuery(*opt); });
}

void RunSubcommandVectorBatchQuery(VectorBatchQueryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendVectorBatchQuery(opt.region_id, opt.vector_ids, opt.key, opt.without_vector, opt.without_scalar,
                                  opt.without_table);
}

void SetUpSubcommandVectorScanQuery(CLI::App& app) {
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

  coor->callback([opt]() { RunSubcommandVectorScanQuery(*opt); });
}

void RunSubcommandVectorScanQuery(VectorScanQueryOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendVectorScanQuery(opt.region_id, opt.start_id, opt.end_id, opt.limit, opt.is_reverse, opt.without_vector,
                                 opt.without_scalar, opt.without_table, opt.key, opt.scalar_filter_key,
                                 opt.scalar_filter_value, opt.scalar_filter_key2, opt.scalar_filter_value2);
}

void SetUpSubcommandVectorScanDump(CLI::App& app) {
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

  coor->callback([opt]() { RunSubcommandVectorScanDump(*opt); });
}

void RunSubcommandVectorScanDump(VectorScanDumpOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendVectorScanDump(opt.region_id, opt.start_id, opt.end_id, opt.limit, opt.is_reverse, opt.csv_output);
}

void SetUpSubcommandVectorGetRegionMetrics(CLI::App& app) {
  auto opt = std::make_shared<VectorGetRegionMetricsOptions>();
  auto coor =
      app.add_subcommand("VectorGetRegionMetrics", "Vector get region metrics")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandVectorGetRegionMetricsd(*opt); });
}

void RunSubcommandVectorGetRegionMetricsd(VectorGetRegionMetricsOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendVectorGetRegionMetrics(opt.region_id);
}

void SetUpSubcommandVectorAdd(CLI::App& app) {
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

  coor->callback([opt]() { RunSubcommandVectorAdd(*opt); });
}

void RunSubcommandVectorAdd(VectorAddOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  auto ctx = std::make_shared<client_v2::Context>();
  ctx->table_id = opt.table_id;
  ctx->region_id = opt.region_id;
  ctx->dimension = opt.dimension;
  ctx->start_id = opt.start_id;
  ctx->count = opt.count;
  ctx->step_count = opt.step_count;
  ctx->with_scalar = !opt.without_scalar;
  ctx->with_table = !opt.without_table;
  ctx->csv_data = opt.csv_data;
  ctx->json_data = opt.json_data;

  ctx->scalar_filter_key = opt.scalar_filter_key;
  ctx->scalar_filter_value = opt.scalar_filter_value;
  ctx->scalar_filter_key2 = opt.scalar_filter_key2;
  ctx->scalar_filter_value2 = opt.scalar_filter_value2;
  if (ctx->table_id > 0) {
    client_v2::SendVectorAddRetry(ctx);
  } else {
    client_v2::SendVectorAdd(ctx);
  }
  // client_v2::SendVectorGetRegionMetrics(opt.region_id);
}

void SetUpSubcommandVectorDelete(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandVectorDelete(*opt); });
}

void RunSubcommandVectorDelete(VectorDeleteOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendVectorDelete(opt.region_id, opt.start_id, opt.count);
}

void SetUpSubcommandVectorGetMaxId(CLI::App& app) {
  auto opt = std::make_shared<VectorGetMaxIdOptions>();
  auto coor = app.add_subcommand("VectorGetMaxId", "Vector get max id")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandVectorGetMaxId(*opt); });
}

void RunSubcommandVectorGetMaxId(VectorGetMaxIdOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendVectorGetMaxId(opt.region_id);
}

void SetUpSubcommandVectorGetMinId(CLI::App& app) {
  auto opt = std::make_shared<VectorGetMinIdOptions>();
  auto coor = app.add_subcommand("VectorGetMinId", "Vector get min id")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandVectorGetMinId(*opt); });
}

void RunSubcommandVectorGetMinId(VectorGetMinIdOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendVectorGetMaxId(opt.region_id);
}

void SetUpSubcommandVectorAddBatch(CLI::App& app) {
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

  coor->callback([opt]() { RunSubcommandVectorAddBatch(*opt); });
}

void RunSubcommandVectorAddBatch(VectorAddBatchOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendVectorAddBatch(opt.region_id, opt.dimension, opt.count, opt.step_count, opt.start_id,
                                opt.vector_index_add_cost_file, opt.without_scalar);
}

void SetUpSubcommandVectorAddBatchDebug(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandVectorAddBatchDebug(*opt); });
}

void RunSubcommandVectorAddBatchDebug(VectorAddBatchDebugOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::SendVectorAddBatchDebug(opt.region_id, opt.dimension, opt.count, opt.step_count, opt.start_id,
                                     opt.vector_index_add_cost_file, opt.without_scalar);
}

void SetUpSubcommandVectorCalcDistance(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandVectorCalcDistance(*opt); });
}

void RunSubcommandVectorCalcDistance(VectorCalcDistanceOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendVectorCalcDistance(opt.dimension, opt.alg_type, opt.metric_type, opt.left_vector_size,
                                    opt.right_vector_size, opt.is_return_normlize);
}

void SetUpSubcommandCalcDistance(CLI::App& app) {
  auto opt = std::make_shared<CalcDistanceOptions>();
  auto coor = app.add_subcommand("CalcDistance", "Calc distance")->group("Store Manager Commands");
  coor->add_option("--vector_data1", opt->vector_data1, "Request parameter vector_data1")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--vector_data2", opt->vector_data2, "Request parameter vector_data2")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandCalcDistance(*opt); });
}

void RunSubcommandCalcDistance(CalcDistanceOptions const& opt) {
  client_v2::SendCalcDistance(opt.vector_data1, opt.vector_data2);
}

void SetUpSubcommandVectorCount(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandVectorCount(*opt); });
}

void RunSubcommandVectorCount(VectorCountOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::SendVectorCount(opt.region_id, opt.start_id, opt.end_id);
}

void SetUpSubcommandCountVectorTable(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandCountVectorTable(*opt); });
}

void RunSubcommandCountVectorTable(CountVectorTableOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {opt.store_addrs}, 0, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed store_addrs=" << opt.store_addrs;
    std::cout << "Set Up failed store_addrs=" << opt.store_addrs;
    exit(-1);
  }
  auto ctx = std::make_shared<client_v2::Context>();
  ctx->table_id = opt.table_id;
  client_v2::CountVectorTable(ctx);
}

// test operation
void SetUpSubcommandTestBatchPutGet(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTestBatchPutGet(*opt); });
}

void RunSubcommandTestBatchPutGet(TestBatchPutGetOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }

  client_v2::TestBatchPutGet(opt.region_id, opt.thread_num, opt.req_num, opt.prefix);
}

void SetUpSubcommandTestRegionLifecycle(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTestRegionLifecycle(*opt); });
}

void RunSubcommandTestRegionLifecycle(TestRegionLifecycleOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);
  client_v2::TestRegionLifecycle(opt.region_id, opt.raft_group, raft_addrs, opt.region_count, opt.thread_num,
                                 opt.req_num, opt.prefix);
}

void SetUpSubcommandTestDeleteRangeWhenTransferLeader(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandTestDeleteRangeWhenTransferLeader(*opt); });
}

void RunSubcommandTestDeleteRangeWhenTransferLeader(TestDeleteRangeWhenTransferLeaderOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore(opt.coor_url, empty_vec, opt.region_id, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed region_id=" << opt.region_id;
    std::cout << "Set Up failed region_id=" << opt.region_id;
    exit(-1);
  }
  client_v2::TestDeleteRangeWhenTransferLeader(opt.region_id, opt.req_num, opt.prefix);
}

void SetUpSubcommandAutoTest(CLI::App& app) {
  auto opt = std::make_shared<AutoTestOptions>();
  auto coor = app.add_subcommand("AutoTest", "Auto test")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_name", opt->table_name, "Request parameter table_name")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--req_num", opt->req_num, "Number of requests")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->add_option("--partition_num", opt->partition_num, "Request parameter partition_num")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandAutoTest(*opt); });
}

void RunSubcommandAutoTest(AutoTestOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed store_addrs=" << opt.store_addrs;
    std::cout << "Set Up failed store_addrs=" << opt.store_addrs;
    exit(-1);
  }
  auto ctx = std::make_shared<client_v2::Context>();
  ctx->table_name = opt.table_name;
  ctx->partition_num = opt.partition_num;
  ctx->req_num = opt.req_num;
  AutoTest(ctx);
}

void SetUpSubcommandAutoMergeRegion(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandAutoMergeRegion(*opt); });
}

void RunSubcommandAutoMergeRegion(AutoMergeRegionOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed store_addrs=" << opt.store_addrs;
    std::cout << "Set Up failed store_addrs=" << opt.store_addrs;
    exit(-1);
  }
  auto ctx = std::make_shared<client_v2::Context>();
  ctx->table_id = opt.table_id;
  ctx->index_id = opt.index_id;
  AutoMergeRegion(ctx);
}

void SetUpSubcommandAutoDropTable(CLI::App& app) {
  auto opt = std::make_shared<AutoDropTableOptions>();
  auto coor = app.add_subcommand("AutoDropTable", "Auto drop table")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--req_num", opt->req_num, "Number of requests")
      ->default_val(1)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandAutoDropTable(*opt); });
}

void RunSubcommandAutoDropTable(AutoDropTableOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  auto ctx = std::make_shared<client_v2::Context>();

  ctx->req_num = opt.req_num;
  client_v2::AutoDropTable(ctx);
}

void SetUpSubcommandCheckTableDistribution(CLI::App& app) {
  auto opt = std::make_shared<CheckTableDistributionOptions>();
  auto coor = app.add_subcommand("CheckTableDistribution", "Check table distribution")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Number of table_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Number of key")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandCheckTableDistribution(*opt); });
}

void RunSubcommandCheckTableDistribution(CheckTableDistributionOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  auto ctx = std::make_shared<client_v2::Context>();
  ctx->table_id = opt.table_id;
  ctx->key = opt.key;
  client_v2::CheckTableDistribution(ctx);
}

void SetUpSubcommandCheckIndexDistribution(CLI::App& app) {
  auto opt = std::make_shared<CheckIndexDistributionOptions>();
  auto coor = app.add_subcommand("CheckIndexDistribution", "Check index distribution")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Number of table_id")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandCheckIndexDistribution(*opt); });
}

void RunSubcommandCheckIndexDistribution(CheckIndexDistributionOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  auto ctx = std::make_shared<client_v2::Context>();
  ctx->table_id = opt.table_id;
  client_v2::CheckIndexDistribution(ctx);
}

void SetUpSubcommandDumpDb(CLI::App& app) {
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
  coor->callback([opt]() { RunSubcommandDumpDb(*opt); });
}

void RunSubcommandDumpDb(DumpDbOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  auto ctx = std::make_shared<client_v2::Context>();
  ctx->table_id = opt.table_id;
  ctx->index_id = opt.index_id;
  ctx->db_path = opt.db_path;
  ctx->offset = opt.offset;
  ctx->limit = opt.limit;
  ctx->show_vector = opt.show_vector;
  ctx->show_lock = opt.show_lock;
  ctx->show_write = opt.show_write;
  ctx->show_last_data = opt.show_last_data;
  ctx->show_all_data = opt.show_all_data;
  ctx->show_pretty = opt.show_pretty;
  ctx->print_column_width = opt.print_column_width;
  if (ctx->table_id == 0 && ctx->index_id == 0) {
    DINGO_LOG(ERROR) << "Param table_id|index_id is error.";
    return;
  }
  if (ctx->db_path.empty()) {
    DINGO_LOG(ERROR) << "Param db_path is error.";
    return;
  }
  if (ctx->offset < 0) {
    DINGO_LOG(ERROR) << "Param offset is error.";
    return;
  }
  if (ctx->limit < 0) {
    DINGO_LOG(ERROR) << "Param limit is error.";
    return;
  }
  client_v2::DumpDb(ctx);
}

void SetUpSubcommandWhichRegion(CLI::App& app) {
  auto opt = std::make_shared<WhichRegionOptions>();
  auto coor = app.add_subcommand("DumpDb", "Dump rocksdb")->group("Store Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--table_id", opt->table_id, "Number of table_id")->group("Coordinator Manager Commands");
  coor->add_option("--index_id", opt->index_id, "Number of index_id")->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandWhichRegion(*opt); });
}

void RunSubcommandWhichRegion(WhichRegionOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  auto ctx = std::make_shared<client_v2::Context>();
  ctx->table_id = opt.table_id;
  ctx->index_id = opt.index_id;
  ctx->key = opt.key;
  if (ctx->table_id == 0 && ctx->index_id == 0) {
    DINGO_LOG(ERROR) << "Param table_id|index_id is error.";
    return;
  }
  if (ctx->key.empty()) {
    DINGO_LOG(ERROR) << "Param key is error.";
    return;
  }
  WhichRegion(ctx);
}

void SetUpSubcommandRegionMetrics(CLI::App& app) {
  auto opt = std::make_shared<RegionMetricsOptions>();
  auto coor = app.add_subcommand("GetRegionMetrics", "Get region metrics ")->group("Region Manager Commands");
  coor->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  coor->add_option("--region_ids", opt->region_ids, "Request parameter, empty means query all regions");
  coor->add_option("--type", opt->type,
                   "Request parameter type, only support 6 or 8, 6 means store region metrics;8 means store region "
                   "actual metrics.")
      ->required();
  coor->callback([opt]() { RunSubcommandRegionMetrics(*opt); });
}

void RunSubcommandRegionMetrics(RegionMetricsOptions const& opt) {
  std::vector<std::string> empty_vec;
  if (!SetUpStore("", {opt.store_addrs}, 0, 0)) {
    DINGO_LOG(ERROR) << "Set Up failed store_addrs=" << opt.store_addrs;
    std::cout << "Set Up failed store_addrs=" << opt.store_addrs;
    exit(-1);
  }
  dingodb::pb::debug::DebugRequest request;
  dingodb::pb::debug::DebugResponse response;
  if (opt.type != 6 && opt.type != 8) {
    DINGO_LOG(ERROR) << "type only support 6 or 8";
    return;
  }
  request.set_type(::dingodb::pb::debug::DebugType(opt.type));
  // request.set_type(opt.type);
  for (const int64_t& region_id : opt.region_ids) {
    request.add_region_ids(region_id);
  }

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "Debug", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "Get region metrics  failed, error:"
                     << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name()
                     << " " << response.error().errmsg();
  } else {
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
}

}  // namespace client_v2