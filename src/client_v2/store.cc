
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

#include "client_v2/store.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "bthread/bthread.h"
#include "butil/status.h"
#include "client_v2/coordinator.h"
#include "client_v2/dump.h"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "client_v2/meta.h"
#include "client_v2/pretty.h"
#include "client_v2/router.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/serial_helper.h"
#include "common/version.h"
#include "coprocessor/utils.h"
#include "document/codec.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/document.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "proto/version.pb.h"
#include "serial/buf.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/schema/long_schema.h"
#include "vector/codec.h"

const int kBatchSize = 3;

namespace client_v2 {

void SetUpStoreSubCommands(CLI::App& app) {
  SetUpKvGet(app);
  SetUpKvPut(app);

  SetUpSnapshot(app);
  SetUpRegionMetrics(app);

  SetUpDumpRegion(app);
  SetUpDumpDb(app);

  // txn
  SetUpTxnScan(app);
  SetUpTxnScanLock(app);
  SetUpTxnPrewrite(app);
  SetUpTxnCommit(app);
  SetUpTxnPessimisticLock(app);
  SetUpTxnPessimisticRollback(app);
  SetUpTxnBatchGet(app);
  SetUpTxnDump(app);
  SetUpTxnGC(app);
  SetUpTxnCount(app);

  SetUpWhichRegion(app);
  SetUpQueryRegionStatusMetrics(app);
  SetUpModifyRegionMeta(app);
  SetUpCompact(app);
  SetUpQueryMemoryLocks(app);
}

static bool SetUpStore(const std::string& url, const std::vector<std::string>& addrs, int64_t region_id) {
  if (Helper::SetUp(url) < 0) {
    exit(-1);
  }

  if (!addrs.empty()) {
    return client_v2::InteractionManager::GetInstance().CreateStoreInteraction(addrs);
  } else {
    // Get store addr from coordinator
    auto status = client_v2::InteractionManager::GetInstance().CreateStoreInteraction(region_id);
    if (!status.ok()) {
      std::cout << "Create store interaction failed, error: " << status.error_cstr() << '\n';
      return false;
    }
  }
  return true;
}

void PrintTableRange(dingodb::pb::meta::TableRange& table_range) {
  DINGO_LOG(INFO) << "refresh route...";
  for (const auto& item : table_range.range_distribution()) {
    DINGO_LOG(INFO) << fmt::format("region {} range [{}-{})", item.id().entity_id(),
                                   dingodb::Helper::StringToHex(item.range().start_key()),
                                   dingodb::Helper::StringToHex(item.range().end_key()));
  }
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

dingodb::pb::store::TxnScanResponse SendTxnScanByNormalMode(dingodb::pb::common::Region region,
                                                            const dingodb::pb::common::Range& range, size_t limit,
                                                            int64_t start_ts, int64_t resolve_locks, bool key_only,
                                                            bool is_reverse) {
  dingodb::pb::store::TxnScanRequest request;
  dingodb::pb::store::TxnScanResponse response;

  request.mutable_context()->set_region_id(region.id());
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  request.mutable_context()->add_resolved_locks(resolve_locks);

  dingodb::pb::common::RangeWithOptions range_with_option;
  range_with_option.mutable_range()->CopyFrom(range);
  range_with_option.set_with_start(true);
  range_with_option.set_with_end(false);

  request.mutable_range()->Swap(&range_with_option);

  request.set_limit(kBatchSize);
  request.set_start_ts(start_ts);

  request.set_is_reverse(is_reverse);
  request.set_key_only(key_only);

  for (;;) {
    dingodb::pb::store::TxnScanResponse sub_response;

    // maybe current store interaction is not store node, so need reset.
    InteractionManager::GetInstance().ResetStoreInteraction();
    auto status = InteractionManager::GetInstance().SendRequestWithContext(GetServiceName(region), "TxnScan", request,
                                                                           sub_response);
    if (!status.ok()) {
      response.mutable_error()->set_errcode(dingodb::pb::error::Errno(status.error_code()));
      response.mutable_error()->set_errmsg(status.error_str());
      break;
    }
    if (sub_response.error().errcode() != dingodb::pb::error::OK) {
      *response.mutable_error() = sub_response.error();
      break;
    }
    // adjust range
    dingodb::pb::common::RangeWithOptions range_with_option;
    range_with_option.mutable_range()->set_start_key(sub_response.end_key());
    range_with_option.mutable_range()->set_end_key(range.end_key());
    range_with_option.set_with_start(false);
    range_with_option.set_with_end(false);

    request.mutable_range()->Swap(&range_with_option);

    // copy data
    for (int i = 0; i < sub_response.kvs_size(); ++i) {
      if (response.kvs_size() < limit) {
        response.add_kvs()->Swap(&sub_response.mutable_kvs()->at(i));
      }
    }

    for (int i = 0; i < sub_response.vectors_size(); ++i) {
      if (response.vectors_size() < limit) {
        response.add_vectors()->Swap(&sub_response.mutable_vectors()->at(i));
      }
    }

    for (int i = 0; i < sub_response.documents_size(); ++i) {
      if (response.documents_size() < limit) {
        response.add_documents()->Swap(&sub_response.mutable_documents()->at(i));
      }
    }

    if (response.kvs_size() >= limit || response.vectors_size() >= limit || response.documents_size() >= limit) {
      break;
    }

    if (!sub_response.has_more()) {
      break;
    }
  }

  return response;
}

dingodb::pb::store::TxnScanResponse SendTxnScanByStreamMode(dingodb::pb::common::Region region,
                                                            const dingodb::pb::common::Range& range, size_t limit,
                                                            int64_t start_ts, int64_t resolve_locks, bool key_only,
                                                            bool is_reverse) {
  dingodb::pb::store::TxnScanResponse response;
  dingodb::pb::store::TxnScanRequest request;
  request.mutable_context()->set_region_id(region.id());
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  request.mutable_context()->add_resolved_locks(resolve_locks);
  request.mutable_request_info()->set_request_id(1111111111);

  dingodb::pb::common::RangeWithOptions range_with_option;
  range_with_option.mutable_range()->CopyFrom(range);
  range_with_option.set_with_start(true);
  range_with_option.set_with_end(false);

  request.mutable_range()->Swap(&range_with_option);

  request.set_start_ts(start_ts);

  request.set_is_reverse(is_reverse);
  request.set_key_only(key_only);
  request.mutable_stream_meta()->set_limit(kBatchSize);

  for (;;) {
    dingodb::pb::store::TxnScanResponse sub_response;

    // maybe current store interaction is not store node, so need reset.
    InteractionManager::GetInstance().ResetStoreInteraction();
    auto status = InteractionManager::GetInstance().SendRequestWithContext(GetServiceName(region), "TxnScan", request,
                                                                           sub_response);
    if (!status.ok()) {
      response.mutable_error()->set_errcode(dingodb::pb::error::Errno(status.error_code()));
      response.mutable_error()->set_errmsg(status.error_str());
      break;
    }

    if (sub_response.error().errcode() != dingodb::pb::error::OK) {
      *response.mutable_error() = sub_response.error();
      break;
    }

    // set request stream id
    if (!sub_response.stream_meta().stream_id().empty()) {
      request.mutable_stream_meta()->set_stream_id(sub_response.stream_meta().stream_id());
    }

    // copy data
    for (int i = 0; i < sub_response.kvs_size(); ++i) {
      if (response.kvs_size() < limit) {
        response.add_kvs()->Swap(&sub_response.mutable_kvs()->at(i));
      }
    }

    for (int i = 0; i < sub_response.vectors_size(); ++i) {
      if (response.vectors_size() < limit) {
        response.add_vectors()->Swap(&sub_response.mutable_vectors()->at(i));
      }
    }

    for (int i = 0; i < sub_response.documents_size(); ++i) {
      if (response.documents_size() < limit) {
        response.add_documents()->Swap(&sub_response.mutable_documents()->at(i));
      }
    }

    if (response.kvs_size() >= limit || response.vectors_size() >= limit || response.documents_size() >= limit) {
      break;
    }

    if (!sub_response.stream_meta().has_more()) {
      break;
    }
  }

  return response;
}

dingodb::pb::store::TxnScanLockResponse SendTxnScanLockByStreamMode(const dingodb::pb::common::Region& region,
                                                                    const dingodb::pb::common::Range& range, bool is_rc,
                                                                    int64_t ts, size_t limit) {
  dingodb::pb::store::TxnScanLockRequest request;
  dingodb::pb::store::TxnScanLockResponse response;

  request.mutable_context()->set_region_id(region.id());
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  if (is_rc) {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::ReadCommitted);
  } else {
    request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  }

  request.set_max_ts(ts);

  request.set_start_key(range.start_key());
  request.set_end_key(range.end_key());

  request.mutable_stream_meta()->set_limit(kBatchSize);

  for (;;) {
    dingodb::pb::store::TxnScanLockResponse sub_response;

    auto status = InteractionManager::GetInstance().SendRequestWithContext(GetServiceName(region), "TxnScanLock",
                                                                           request, sub_response);
    if (!status.ok()) {
      response.mutable_error()->set_errcode(dingodb::pb::error::Errno(status.error_code()));
      response.mutable_error()->set_errmsg(status.error_str());
      break;
    }
    if (sub_response.error().errcode() != dingodb::pb::error::OK) {
      *response.mutable_error() = sub_response.error();
      break;
    }

    // set request stream id
    if (!sub_response.stream_meta().stream_id().empty()) {
      request.mutable_stream_meta()->set_stream_id(sub_response.stream_meta().stream_id());
    }

    // copy data
    for (int i = 0; i < sub_response.locks_size(); ++i) {
      if (response.locks_size() < limit) {
        response.add_locks()->Swap(&sub_response.mutable_locks()->at(i));
      }
    }

    if (response.locks_size() >= limit) {
      break;
    }

    if (!sub_response.stream_meta().has_more()) {
      break;
    }
  }

  return response;
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
  auto* cmd = app.add_subcommand("AddRegion", "Add Region")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--raft_addrs", opt->raft_addrs,
                  "example --raft_addr:127.0.0.1:10101:0,127.0.0.1:10102:0,127.0.0.1:10103:0")
      ->required();
  cmd->add_option("--raft_group", opt->raft_group, "Request parameter raft_group")->required();
  cmd->add_option("--region_id", opt->region_id, "Request parameter raft_group")->required();
  cmd->callback([opt]() { RunAddRegion(*opt); });
}

void RunAddRegion(AddRegionOptions const& opt) {
  std::string value;
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
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
  auto* cmd = app.add_subcommand("KvGet", "Kv get")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->callback([opt]() { RunKvGet(*opt); });
}

void RunKvGet(KvGetOptions const& opt) {
  std::string value;
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvGet(opt, value);
}

void SetUpKvPut(CLI::App& app) {
  auto opt = std::make_shared<KvPutOptions>();
  auto* cmd = app.add_subcommand("KvPut", "Kv put")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->add_option("--value", opt->value, "Request parameter value");
  cmd->callback([opt]() { RunKvPut(*opt); });
}

void RunKvPut(KvPutOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  std::string value = opt.value.empty() ? client_v2::Helper::GenRandomString(256) : opt.value;
  client_v2::SendKvPut(opt, value);
}

void SetUpChangeRegion(CLI::App& app) {
  auto opt = std::make_shared<ChangeRegionOptions>();
  auto* cmd = app.add_subcommand("ChangeRegion", "Change region")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--raft_group", opt->raft_group, "Request parameter raft_group")->required();
  cmd->add_option("--raft_addrs", opt->raft_addrs, "Request parameter value");
  cmd->callback([opt]() { RunChangeRegion(*opt); });
}

void RunChangeRegion(ChangeRegionOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendChangeRegion(opt);
}

void SetUpMergeRegionAtStore(CLI::App& app) {
  auto opt = std::make_shared<MergeRegionAtStoreOptions>();
  auto* cmd = app.add_subcommand("MergeRegionAtStore", "Merge region at store ")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--source_id", opt->source_id, "Request parameter source region id")->required();
  cmd->add_option("--target_id", opt->target_id, "Request parameter target region id")->required();
  cmd->callback([opt]() { RunMergeRegionAtStore(*opt); });
}

void RunMergeRegionAtStore(MergeRegionAtStoreOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.source_id)) {
    exit(-1);
  }
  client_v2::SendMergeRegion(opt);
}

void SetUpDestroyRegion(CLI::App& app) {
  auto opt = std::make_shared<DestroyRegionOptions>();
  auto* cmd = app.add_subcommand("DestroyRegion", "Destroy region")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunDestroyRegion(*opt); });
}

void RunDestroyRegion(DestroyRegionOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDestroyRegion(opt);
}

void SetUpSnapshot(CLI::App& app) {
  auto opt = std::make_shared<SnapshotOptions>();
  auto* cmd = app.add_subcommand("Snapshot", "Snapshot")->group("Region Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunSnapshot(*opt); });
}

void RunSnapshot(SnapshotOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendSnapshot(opt);
}

void SetUpBatchAddRegion(CLI::App& app) {
  auto opt = std::make_shared<BatchAddRegionOptions>();
  auto* cmd = app.add_subcommand("BatchAddRegion", "Batch add region")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();

  cmd->add_option("--region_count", opt->region_count, "Request parameter region id")->default_val(1);
  cmd->add_option("--thread_num", opt->thread_num, "Request parameter region id")->default_val(1);
  cmd->add_option("--raft_group", opt->raft_group, "Request parameter raft_group")->required();
  cmd->add_option("--raft_addrs", opt->raft_addrs, "Request parameter value");
  cmd->callback([opt]() { RunBatchAddRegion(*opt); });
}

void RunBatchAddRegion(BatchAddRegionOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::BatchSendAddRegion(opt);
}

void SetUpSnapshotVectorIndex(CLI::App& app) {
  auto opt = std::make_shared<SnapshotVectorIndexOptions>();
  auto* cmd = app.add_subcommand("SnapshotVectorIndex", "Snapshot vector index")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunSnapshotVectorIndex(*opt); });
}

void RunSnapshotVectorIndex(SnapshotVectorIndexOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendSnapshotVectorIndex(opt);
}

void SetUpCompact(CLI::App& app) {
  auto opt = std::make_shared<CompactOptions>();
  auto* cmd = app.add_subcommand("Compact", "Compact ")->group("Store Command");
  cmd->add_option("--store_addrs", opt->store_addrs, "server addrs");
  cmd->callback([opt]() { RunCompact(*opt); });
}

void RunCompact(CompactOptions const& opt) {
  if (!SetUpStore("", {opt.store_addrs}, 0)) {
    exit(-1);
  }
  client_v2::SendCompact("");
}

void SetUpGetMemoryStats(CLI::App& app) {
  auto opt = std::make_shared<GetMemoryStatsOptions>();
  auto* cmd = app.add_subcommand("GetMemoryStats", "GetMemory stats ")->group("Store Command");
  cmd->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  cmd->callback([opt]() { RunGetMemoryStats(*opt); });
}

void RunGetMemoryStats(GetMemoryStatsOptions const& opt) {
  if (!SetUpStore("", {opt.store_addrs}, 0)) {
    exit(-1);
  }
  client_v2::GetMemoryStats();
}

void SetUpReleaseFreeMemory(CLI::App& app) {
  auto opt = std::make_shared<ReleaseFreeMemoryOptions>();
  auto* cmd = app.add_subcommand("ReleaseFreeMemory", "Release free memory ")->group("Store Command");
  cmd->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  cmd->add_option("--rate", opt->rate, "server addrs")->default_val(0.0);
  cmd->callback([opt]() { RunReleaseFreeMemory(*opt); });
}

void RunReleaseFreeMemory(ReleaseFreeMemoryOptions const& opt) {
  if (!SetUpStore("", {opt.store_addrs}, 0)) {
    exit(-1);
  }
  client_v2::ReleaseFreeMemory(opt);
}

void SetUpKvBatchGet(CLI::App& app) {
  auto opt = std::make_shared<KvBatchGetOptions>();
  auto* cmd = app.add_subcommand("KvBatchGet", "Kv batch get")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--prefix", opt->prefix, "Request parameter prefix")->required();
  cmd->add_option("--req_num", opt->req_num, "Request parameter region id")->default_val(1);

  cmd->callback([opt]() { RunKvBatchGet(*opt); });
}

void RunKvBatchGet(KvBatchGetOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvBatchGet(opt);
}

void SetUpKvBatchPut(CLI::App& app) {
  auto opt = std::make_shared<KvBatchPutOptions>();
  auto* cmd = app.add_subcommand("KvBatchPut", "Kv batch put")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--prefix", opt->prefix, "Request parameter prefix")->required();
  cmd->add_option("--count", opt->count, "Request parameter region id")->default_val(50);

  cmd->callback([opt]() { RunKvBatchPut(*opt); });
}

void RunKvBatchPut(KvBatchPutOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvBatchPut(opt);
}

void SetUpKvPutIfAbsent(CLI::App& app) {
  auto opt = std::make_shared<KvPutIfAbsentOptions>();
  auto* cmd = app.add_subcommand("KvPutIfAbsent", "Kv put if absent")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--key", opt->key, "Request parameter prefix")->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->callback([opt]() { RunKvPutIfAbsent(*opt); });
}

void RunKvPutIfAbsent(KvPutIfAbsentOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvPutIfAbsent(opt);
}

void SetUpKvBatchPutIfAbsent(CLI::App& app) {
  auto opt = std::make_shared<KvBatchPutIfAbsentOptions>();
  auto* cmd = app.add_subcommand("KvBatchPutIfAbsent", "Kv batch put if absent")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--prefix", opt->prefix, "Request parameter prefix")->required();
  cmd->add_option("--count", opt->count, "Request parameter region id")->default_val(50);
  cmd->callback([opt]() { RunKvBatchPutIfAbsent(*opt); });
}

void RunKvBatchPutIfAbsent(KvBatchPutIfAbsentOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvBatchPutIfAbsent(opt);
}

void SetUpKvBatchDelete(CLI::App& app) {
  auto opt = std::make_shared<KvBatchDeleteOptions>();
  auto* cmd = app.add_subcommand("KvBatchDelete", "Kv batch delete")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->callback([opt]() { RunKvBatchDelete(*opt); });
}

void RunKvBatchDelete(KvBatchDeleteOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvBatchDelete(opt);
}

void SetUpKvDeleteRange(CLI::App& app) {
  auto opt = std::make_shared<KvDeleteRangeOptions>();
  auto* cmd = app.add_subcommand("KvDeleteRange", "Kv delete range")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--prefix", opt->prefix, "Request parameter prefix")->required();
  cmd->callback([opt]() { RunKvDeleteRange(*opt); });
}

void RunKvDeleteRange(KvDeleteRangeOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvDeleteRange(opt);
}

void SetUpKvScan(CLI::App& app) {
  auto opt = std::make_shared<KvScanOptions>();
  auto* cmd = app.add_subcommand("KvScan", "Kv scan")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--prefix", opt->prefix, "Request parameter prefix")->required();
  cmd->callback([opt]() { RunKvScan(*opt); });
}

void RunKvScan(KvScanOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvScan(opt);
}

void SetUpKvCompareAndSet(CLI::App& app) {
  auto opt = std::make_shared<KvCompareAndSetOptions>();
  auto* cmd = app.add_subcommand("KvCompareAndSet", "Kv compare and set")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->callback([opt]() { RunKvCompareAndSet(*opt); });
}

void RunKvCompareAndSet(KvCompareAndSetOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvCompareAndSet(opt);
}

void SetUpKvBatchCompareAndSet(CLI::App& app) {
  auto opt = std::make_shared<KvBatchCompareAndSetOptions>();
  auto* cmd = app.add_subcommand("KvBatchCompareAndSet", "Kv batch compare and set")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--prefix", opt->prefix, "Request parameter prefix")->required();
  ;
  cmd->add_option("--count", opt->count, "Request parameter count")->default_val(100);
  cmd->callback([opt]() { RunKvBatchCompareAndSet(*opt); });
}

void RunKvBatchCompareAndSet(KvBatchCompareAndSetOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendKvBatchCompareAndSet(opt);
}

void SetUpKvScanBeginV2(CLI::App& app) {
  auto opt = std::make_shared<KvScanBeginV2Options>();
  auto* cmd = app.add_subcommand("KvScanBeginV2", "Kv scan beginV2")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();

  cmd->add_option("--scan_id", opt->scan_id, "Request parameter scan id")->default_val(1);
  cmd->callback([opt]() { RunKvScanBeginV2(*opt); });
}

void RunKvScanBeginV2(KvScanBeginV2Options const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvScanBeginV2(opt);
}

void SetUpKvScanContinueV2(CLI::App& app) {
  auto opt = std::make_shared<KvScanContinueV2Options>();
  auto* cmd = app.add_subcommand("KvScanContinueV2", "Kv scan continueV2 ")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();

  cmd->add_option("--scan_id", opt->scan_id, "Request parameter scan id")->default_val(1);
  cmd->callback([opt]() { RunKvScanContinueV2(*opt); });
}

void RunKvScanContinueV2(KvScanContinueV2Options const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvScanContinueV2(opt);
}

void SetUpKvScanReleaseV2(CLI::App& app) {
  auto opt = std::make_shared<KvScanReleaseV2Options>();
  auto* cmd = app.add_subcommand("KvScanReleaseV2", "Kv scan releaseV2 ")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();

  cmd->add_option("--scan_id", opt->scan_id, "Request parameter scan id")->default_val(1);
  cmd->callback([opt]() { RunKvScanReleaseV2(*opt); });
}

void RunKvScanReleaseV2(KvScanReleaseV2Options const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendKvScanReleaseV2(opt);
}

void SetUpTxnGet(CLI::App& app) {
  auto opt = std::make_shared<TxnGetOptions>();
  auto* cmd = app.add_subcommand("TxnGet", "Txn get")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->add_flag("--is_hex", opt->is_hex, "key is hex")->default_val(false);
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--resolve_locks", opt->resolve_locks, "Request parameter resolve_locks");
  cmd->callback([opt]() { RunTxnGet(*opt); });
}

void RunTxnGet(TxnGetOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnGet(opt);
}

void SetUpTxnScan(CLI::App& app) {
  auto opt = std::make_shared<TxnScanOptions>();
  auto* cmd = app.add_subcommand("TxnScan", "Txn scan")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter region id")->required();
  cmd->add_option("--start_key", opt->start_key, "Request parameter start_key");
  cmd->add_option("--end_key", opt->end_key, "Request parameter start_key");
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->default_val(INT64_MAX);
  cmd->add_option("--limit", opt->limit, "Request parameter limit")->default_val(20);
  cmd->add_option("--rc", opt->rc, "read commit")->default_val(false)->default_str("false");
  cmd->add_option("--is_reverse", opt->is_reverse, "Request parameter is_reverse")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--key_only", opt->key_only, "Request parameter key_only")->default_val(false)->default_str("false");
  cmd->add_option("--resolve_locks", opt->resolve_locks, "Request parameter resolve_locks");
  cmd->add_option("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(true)->default_str("true");

  cmd->callback([opt]() { RunTxnScan(*opt); });
}

void RunTxnScan(TxnScanOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.id)) {
    exit(-1);
  }

  dingodb::pb::common::Region region = SendQueryRegion(opt.id);
  if (region.id() == 0) {
    std::cout << "Get region failed." << '\n';
    return;
  }

  dingodb::pb::common::Range range;
  if (opt.start_key.empty() || opt.end_key.empty()) {
    range.set_start_key(region.definition().range().start_key());
    range.set_end_key(region.definition().range().end_key());
  } else {
    range.set_start_key(opt.is_hex ? HexToString(opt.start_key) : opt.start_key);
    range.set_end_key(opt.is_hex ? HexToString(opt.end_key) : opt.end_key);
  }

  auto response =
      SendTxnScanByStreamMode(region, range, opt.limit, opt.start_ts, opt.resolve_locks, opt.key_only, opt.is_reverse);

  // auto response =
  //     SendTxnScanByNormalMode(region, range, opt.limit, opt.start_ts, opt.resolve_locks, opt.key_only,
  //     opt.is_reverse);

  Pretty::Show(response);
}

void SetUpTxnScanLock(CLI::App& app) {
  auto opt = std::make_shared<TxnScanLockOptions>();
  auto* cmd = app.add_subcommand("TxnScanLock", "Txn scan lock")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter region id")->required();
  cmd->add_option("--is_rc", opt->rc, "read commit")->default_val(false)->default_str("false");
  cmd->add_option("--max_ts", opt->max_ts, "Request parameter max_ts")->required();
  cmd->add_option("--start_key", opt->start_key, "Request parameter start_key")->required();
  cmd->add_option("--end_key", opt->end_key, "Request parameter end_key")->required();
  cmd->add_option("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(true)->default_str("true");
  cmd->add_option("--limit", opt->limit, "Request parameter limit")->default_val(20);

  cmd->callback([opt]() { RunTxnScanLock(*opt); });
}

void RunTxnScanLock(TxnScanLockOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.id)) {
    exit(-1);
  }

  dingodb::pb::common::Region region = SendQueryRegion(opt.id);
  if (region.id() == 0) {
    std::cout << "Get region failed." << '\n';
    return;
  }

  dingodb::pb::common::Range range;
  if (opt.start_key.empty() || opt.end_key.empty()) {
    range.set_start_key(region.definition().range().start_key());
    range.set_end_key(region.definition().range().end_key());
  } else {
    range.set_start_key(opt.is_hex ? HexToString(opt.start_key) : opt.start_key);
    range.set_end_key(opt.is_hex ? HexToString(opt.end_key) : opt.end_key);
  }

  auto response = SendTxnScanLockByStreamMode(region, range, opt.rc, opt.max_ts, opt.limit);
  Pretty::Show(response);
}

void SetUpTxnPessimisticLock(CLI::App& app) {
  auto opt = std::make_shared<TxnPessimisticLockOptions>();
  auto* cmd = app.add_subcommand("TxnPessimisticLock", "Txn pessimistic lock")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--primary_lock", opt->primary_lock, "Request parameter primary_lock")->required();
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--lock_ttl", opt->lock_ttl, "Request parameter lock_ttl")->required();
  cmd->add_option("--for_update_ts", opt->for_update_ts, "Request parameter for_update_ts ")->required();
  cmd->add_option("--mutation_op", opt->mutation_op, "Request parameter mutation_op must be one of [lock]")->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->add_flag("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false);
  cmd->add_option("--value", opt->value, "Request parameter with_start")->required();
  cmd->add_flag("--value_is_hex", opt->value_is_hex, "Request parameter value_is_hex")->default_val(false);
  cmd->add_flag("--return_values", opt->return_values, "Request parameter return_values")->default_val(false);
  cmd->callback([opt]() { RunTxnPessimisticLock(*opt); });
}

void RunTxnPessimisticLock(TxnPessimisticLockOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnPessimisticLock(opt);
}

void SetUpTxnPessimisticRollback(CLI::App& app) {
  auto opt = std::make_shared<TxnPessimisticRollbackOptions>();
  auto* cmd = app.add_subcommand("TxnPessimisticRollback", "Txn pessimistic rollback")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--for_update_ts", opt->for_update_ts, "Request parameter for_update_ts ")->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->add_flag("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false);
  cmd->callback([opt]() { RunTxnPessimisticRollback(*opt); });
}

void RunTxnPessimisticRollback(TxnPessimisticRollbackOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnPessimisticRollback(opt);
}

void SetUpTxnPrewrite(CLI::App& app) {
  auto opt = std::make_shared<TxnPrewriteOptions>();
  auto* cmd = app.add_subcommand("TxnPrewrite", "Txn prewrite")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--primary_lock", opt->primary_lock, "Request parameter primary_lock")->required();
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--lock_ttl", opt->lock_ttl, "Request parameter lock_ttl")->required();
  cmd->add_option("--txn_size", opt->txn_size, "Request parameter txn_size")->required();
  cmd->add_flag("--try_one_pc", opt->try_one_pc, "Request parameter try_one_pc")->default_val(false);
  cmd->add_option("--max_commit_ts", opt->max_commit_ts, "Request parameter max_commit_ts ")->default_val(0);
  cmd->add_option("--mutation_op", opt->mutation_op,
                  "Request parameter mutation_op must be one of [put, delete, insert]")
      ->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->add_option("--key2", opt->key2, "Request parameter key2");
  cmd->add_flag("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false);
  cmd->add_option("--value", opt->value, "Request parameter value2");
  cmd->add_option("--value2", opt->value2, "Request parameter value2");
  cmd->add_flag("--value_is_hex", opt->value_is_hex, "Request parameter value_is_hex")->default_val(false);
  cmd->add_option("--extra_data", opt->extra_data, "Request parameter extra_data ");
  cmd->add_option("--for_update_ts", opt->for_update_ts, "Request parameter for_update_ts ");
  cmd->add_option("--vector_id", opt->vector_id, "Request parameter vector_id ");
  cmd->add_option("--document_id", opt->document_id, "Request parameter document_id ");
  cmd->add_option("--document_text1", opt->document_text1, "Request parameter document_text1 ");
  cmd->add_option("--document_text2", opt->document_text2, "Request parameter document_text2 ");

  cmd->callback([opt]() { RunTxnPrewrite(*opt); });
}

void RunTxnPrewrite(TxnPrewriteOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnPrewrite(opt);
}

void SetUpTxnCommit(CLI::App& app) {
  auto opt = std::make_shared<TxnCommitOptions>();
  auto* cmd = app.add_subcommand("TxnCommit", "Txn commit")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--rc", opt->rc, "read commit")->default_val(false)->default_str("false");
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--commit_ts", opt->commit_ts, "Request parameter commit_ts")->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->add_option("--key2", opt->key2, "Request parameter key2");
  cmd->add_option("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false)->default_str("false");
  cmd->callback([opt]() { RunTxnCommit(*opt); });
}

void RunTxnCommit(TxnCommitOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnCommit(opt);
}

void SetUpTxnCheckTxnStatus(CLI::App& app) {
  auto opt = std::make_shared<TxnCheckTxnStatusOptions>();
  auto* cmd = app.add_subcommand("TxnCheckTxnStatus", "Txn check txn status")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--primary_key", opt->primary_key, "Request parameter primary_key")->required();
  cmd->add_option("--lock_ts", opt->lock_ts, "Request parameter lock_ts")->required();
  cmd->add_option("--caller_start_ts", opt->caller_start_ts, "Request parameter caller_start_ts")->required();
  cmd->add_option("--current_ts", opt->current_ts, "Request parameter current_ts")->required();
  cmd->add_flag("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false);
  cmd->callback([opt]() { RunTxnCheckTxnStatus(*opt); });
}

void RunTxnCheckTxnStatus(TxnCheckTxnStatusOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnCheckTxnStatus(opt);
}

void SetUpTxnResolveLock(CLI::App& app) {
  auto opt = std::make_shared<TxnResolveLockOptions>();
  auto* cmd = app.add_subcommand("TxnResolveLock", "Txn resolve lock")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--commit_ts", opt->commit_ts, "Request parameter commit_ts, if commmit=0, will do rollback")
      ->required();
  cmd->add_option("--key", opt->key,
                  "Request parameter key, if key is empty, will do resolve lock for all keys of this transaction");
  cmd->add_flag("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false);
  cmd->callback([opt]() { RunTxnResolveLock(*opt); });
}

void RunTxnResolveLock(TxnResolveLockOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnResolveLock(opt);
}

void SetUpTxnBatchGet(CLI::App& app) {
  auto opt = std::make_shared<TxnBatchGetOptions>();
  auto* cmd = app.add_subcommand("TxnBatchGet", "Txn batch get ")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--resolve_locks", opt->resolve_locks, "Request parameter resolve_locks");
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->add_option("--key2", opt->key2, "Request parameter key2")->required();
  cmd->add_flag("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false);

  cmd->callback([opt]() { RunTxnBatchGet(*opt); });
}

void RunTxnBatchGet(TxnBatchGetOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnBatchGet(opt);
}

void SetUpTxnBatchRollback(CLI::App& app) {
  auto opt = std::make_shared<TxnBatchRollbackOptions>();
  auto* cmd = app.add_subcommand("TxnBatchRollback", "Txn batch rollback ")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--key", opt->key, "Request parameter key")->required();
  cmd->add_option("--key2", opt->key2, "Request parameter key2");
  cmd->add_flag("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false);
  cmd->callback([opt]() { RunTxnBatchRollback(*opt); });
}

void RunTxnBatchRollback(TxnBatchRollbackOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnBatchRollback(opt);
}

void SetUpTxnHeartBeat(CLI::App& app) {
  auto opt = std::make_shared<TxnHeartBeatOptions>();
  auto* cmd = app.add_subcommand("TxnHeartBeat", "Txn heart beat ")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--primary_lock", opt->primary_lock, "Request parameter primary_lock")->required();
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--advise_lock_ttl", opt->advise_lock_ttl, "Request parameter advise_lock_ttl")->required();
  cmd->add_flag("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false);
  cmd->callback([opt]() { RunTxnHeartBeat(*opt); });
}

void RunTxnHeartBeat(TxnHeartBeatOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnHeartBeat(opt);
}

void SetUpTxnGC(CLI::App& app) {
  auto opt = std::make_shared<TxnGCOptions>();
  auto* cmd = app.add_subcommand("TxnGC", "Txn gc ")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--rc", opt->rc, "read commit")->default_val(false)->default_str("false");
  cmd->add_option("--safe_point_ts", opt->safe_point_ts, "Request parameter safe_point_ts")->required();
  cmd->callback([opt]() { RunTxnGC(*opt); });
}

void RunTxnGC(TxnGCOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnGc(opt);
}

void SetUpTxnCount(CLI::App& app) {
  auto opt = std::make_shared<TxnCountOptions>();
  auto* cmd = app.add_subcommand("TxnCount", "Txn count")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter region id")->required();
  cmd->add_option("--start_key", opt->start_key, "Request parameter start_key");
  cmd->add_option("--end_key", opt->end_key, "Request parameter end_key");
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->default_val(INT64_MAX);
  cmd->add_option("--rc", opt->rc, "read commit")->default_val(false)->default_str("false");
  cmd->add_option("--resolve_locks", opt->resolve_locks, "Request parameter resolve_locks");
  cmd->add_option("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(true)->default_str("true");

  cmd->callback([opt]() { RunTxnCount(*opt); });
}
void RunTxnCount(TxnCountOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.id)) {
    exit(-1);
  }

  dingodb::pb::common::Region region = SendQueryRegion(opt.id);
  if (region.id() == 0) {
    std::cout << "Get region failed." << '\n';
    return;
  }

  dingodb::pb::common::Range range;
  if (opt.start_key.empty() || opt.end_key.empty()) {
    range.set_start_key(region.definition().range().start_key());
    range.set_end_key(region.definition().range().end_key());
  } else {
    range.set_start_key(opt.is_hex ? HexToString(opt.start_key) : opt.start_key);
    range.set_end_key(opt.is_hex ? HexToString(opt.end_key) : opt.end_key);
  }

  auto response = SendTxnCount(region, range, opt.start_ts, opt.resolve_locks);

  Pretty::Show(response, true);
}

void SetUpTxnDeleteRange(CLI::App& app) {
  auto opt = std::make_shared<TxnDeleteRangeOptions>();
  auto* cmd = app.add_subcommand("TxnDeleteRange", "Txn delete range ")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_flag("--rc", opt->rc, "read commit")->default_val(false);
  cmd->add_option("--start_key", opt->start_key, "Request parameter start_key")->required();
  cmd->add_option("--end_key", opt->end_key, "Request parameter end_key")->required();
  cmd->add_flag("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(false);
  cmd->callback([opt]() { RunTxnDeleteRange(*opt); });
}

void RunTxnDeleteRange(TxnDeleteRangeOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnDeleteRange(opt);
}

void SetUpTxnDump(CLI::App& app) {
  auto opt = std::make_shared<TxnDumpOptions>();
  auto* cmd = app.add_subcommand("TxnDump", "Txn dump")->group("Region Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--rc", opt->rc, "read commit")->default_val(false)->default_str("false");
  cmd->add_option("--start_key", opt->start_key, "Request parameter start_key")->required();
  cmd->add_option("--end_key", opt->end_key, "Request parameter end_key")->required();
  cmd->add_option("--is_hex", opt->is_hex, "Request parameter is_hex")->default_val(true)->default_str("true");
  cmd->add_option("--start_ts", opt->start_ts, "Request parameter start_ts")->required();
  cmd->add_option("--end_ts", opt->end_ts, "Request parameter end_ts")->required();
  cmd->callback([opt]() { RunTxnDump(*opt); });
}

void RunTxnDump(TxnDumpOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendTxnDump(opt);
}

// test operation
void SetUpTestBatchPutGet(CLI::App& app) {
  auto opt = std::make_shared<TestBatchPutGetOptions>();
  auto* cmd = app.add_subcommand("TestBatchPutGet", "Test batch put and get")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--table_id", opt->table_id, "Request parameter table_id")->required();
  cmd->add_option("--thread_num", opt->thread_num, "Number of threads sending requests")->default_val(1);
  cmd->add_option("--req_num", opt->req_num, "Number of requests")->default_val(1);
  cmd->add_option("--prefix", opt->prefix, "key prefix");
  cmd->callback([opt]() { RunTestBatchPutGet(*opt); });
}

struct AddRegionParam {
  int64_t start_region_id;
  int32_t region_count;
  std::string raft_group;
  int req_num;
  std::string prefix;

  std::vector<std::string> raft_addrs;
};

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

void RunTestBatchPutGet(TestBatchPutGetOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::TestBatchPutGet(opt);
}

void SetUpTestRegionLifecycle(CLI::App& app) {
  auto opt = std::make_shared<TestRegionLifecycleOptions>();
  auto* cmd = app.add_subcommand("TestRegionLifecycle", "Test region lifecycle")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--raft_group", opt->raft_group, "Request parameter raft group")->required();
  cmd->add_option("--raft_addrs", opt->raft_addrs,
                  "example --raft_addr:127.0.0.1:10101:0,127.0.0.1:10102:0,127.0.0.1:10103:0")
      ->required();
  cmd->add_option("--thread_num", opt->thread_num, "Number of threads sending requests")->default_val(1);
  cmd->add_option("--req_num", opt->req_num, "Number of requests")->default_val(1);
  cmd->add_option("--prefix", opt->prefix, "key prefix");
  cmd->callback([opt]() { RunTestRegionLifecycle(*opt); });
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

void RunTestRegionLifecycle(TestRegionLifecycleOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  std::vector<std::string> raft_addrs;
  butil::SplitString(opt.raft_addrs, ',', &raft_addrs);
  client_v2::TestRegionLifecycle(opt);
}

void SetUpTestDeleteRangeWhenTransferLeader(CLI::App& app) {
  auto opt = std::make_shared<TestDeleteRangeWhenTransferLeaderOptions>();
  auto* cmd =
      app.add_subcommand("TestDeleteRangeWhenTransferLeaderOptions", "Test delete range when transfer leader options")
          ->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--req_num", opt->req_num, "Number of requests")->default_val(1);
  cmd->add_option("--prefix", opt->prefix, "key prefix");
  cmd->callback([opt]() { RunTestDeleteRangeWhenTransferLeader(*opt); });
}

void SendTransferLeader(int64_t region_id, const dingodb::pb::common::Peer& peer) {
  dingodb::pb::debug::TransferLeaderRequest request;
  dingodb::pb::debug::TransferLeaderResponse response;

  request.set_region_id(region_id);
  *(request.mutable_peer()) = peer;

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "TransferLeader", request, response);
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

void RunTestDeleteRangeWhenTransferLeader(TestDeleteRangeWhenTransferLeaderOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::TestDeleteRangeWhenTransferLeader(opt);
}

void SetUpAutoMergeRegion(CLI::App& app) {
  auto opt = std::make_shared<AutoMergeRegionOptions>();
  auto* cmd = app.add_subcommand("AutoMergeRegion", "Auto merge region")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  cmd->add_option("--table_id", opt->table_id, "Request parameter table_id")->required();
  cmd->add_option("--index_id", opt->index_id, "Request parameter index_id")->required();
  cmd->callback([opt]() { RunAutoMergeRegion(*opt); });
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

      auto task_num = SendGetJobList();
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

void RunAutoMergeRegion(AutoMergeRegionOptions const& opt) {
  if (!SetUpStore("", {opt.store_addrs}, 0)) {
    exit(-1);
  }

  AutoMergeRegion(opt);
}

void SetUpAutoDropTable(CLI::App& app) {
  auto opt = std::make_shared<AutoDropTableOptions>();
  auto* cmd = app.add_subcommand("AutoDropTable", "Auto drop table")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--req_num", opt->req_num, "Number of requests")->default_val(1);
  cmd->callback([opt]() { RunAutoDropTable(*opt); });
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

void RunAutoDropTable(AutoDropTableOptions const& opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  client_v2::AutoDropTable(opt);
}

void SetUpCheckTableDistribution(CLI::App& app) {
  auto opt = std::make_shared<CheckTableDistributionOptions>();
  auto* cmd = app.add_subcommand("CheckTableDistribution", "Check table distribution")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--table_id", opt->table_id, "Number of table_id")->required();
  cmd->add_option("--key", opt->key, "Number of key");
  cmd->callback([opt]() { RunCheckTableDistribution(*opt); });
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

void RunCheckTableDistribution(CheckTableDistributionOptions const& opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  client_v2::CheckTableDistribution(opt);
}

void SetUpCheckIndexDistribution(CLI::App& app) {
  auto opt = std::make_shared<CheckIndexDistributionOptions>();
  auto* cmd = app.add_subcommand("CheckIndexDistribution", "Check index distribution")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--table_id", opt->table_id, "Number of table_id")->required();
  cmd->callback([opt]() { RunCheckIndexDistribution(*opt); });
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

void RunCheckIndexDistribution(CheckIndexDistributionOptions const& opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  client_v2::CheckIndexDistribution(opt);
}

void SetUpDumpDb(CLI::App& app) {
  auto opt = std::make_shared<DumpDbOptions>();
  auto* cmd = app.add_subcommand("DumpDb", "Dump rocksdb")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Number of table_id/index_id")->required();
  cmd->add_option("--db_path", opt->db_path, "rocksdb path")->required();
  cmd->add_option("--offset", opt->offset, "Number of offset, must greatern than 0")->default_val(0);
  cmd->add_option("--limit", opt->limit, "Number of limit")->default_val(20);
  cmd->add_option("--exclude_columns", opt->exclude_columns, "Exclude columns, e.g. col1,col2");

  cmd->callback([opt]() { RunDumpDb(*opt); });
}

void RunDumpDb(DumpDbOptions const& opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  auto status = client_v2::DumpDb(opt);
  if (!status.ok()) {
    std::cout << fmt::format("Error: {} {}", status.error_code(), status.error_str()) << '\n';
  }
}

void SetUpWhichRegion(CLI::App& app) {
  auto opt = std::make_shared<WhichRegionOptions>();
  auto* cmd = app.add_subcommand("WhichRegion", "Which region")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Number of table_id/index_id")->required();
  cmd->add_option("--key", opt->key, "Param key, e.g. primary key")->required();
  cmd->callback([opt]() { RunWhichRegion(*opt); });
}

std::vector<dingodb::pb::coordinator::ScanRegionInfo> GetRegionsByRange(const dingodb::pb::common::Range& range) {
  dingodb::pb::coordinator::ScanRegionsRequest request;
  dingodb::pb::coordinator::ScanRegionsResponse response;

  request.set_key(range.start_key());
  request.set_range_end(range.end_key());

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("ScanRegions", request, response);
  if (!status.ok()) {
    return {};
  }

  return dingodb::Helper::PbRepeatedToVector(response.regions());
}

butil::Status WhichRegionForOldTable(const dingodb::pb::meta::TableDefinition& table_definition,
                                     int64_t table_or_index_id, const std::string& key) {
  // get region range
  dingodb::pb::meta::IndexRange index_range;
  dingodb::pb::meta::TableRange table_range = SendGetTableRange(table_or_index_id);
  if (table_range.range_distribution().empty()) {
    index_range = SendGetIndexRange(table_or_index_id);
  }
  auto range_distribution =
      !table_range.range_distribution().empty() ? table_range.range_distribution() : index_range.range_distribution();
  if (range_distribution.empty()) {
    return butil::Status(-1, "Not found table/index range distribution");
  }

  const auto index_type = table_definition.index_parameter().index_type();

  for (int i = range_distribution.size() - 1; i >= 0; --i) {
    const auto& distribution = range_distribution.at(i);
    int64_t partition_id = distribution.id().parent_entity_id();
    const auto& range = distribution.range();
    const char perfix = dingodb::Helper::GetKeyPrefix(range.start_key());

    std::string plain_key;
    if (index_type == dingodb::pb::common::INDEX_TYPE_NONE || index_type == dingodb::pb::common::INDEX_TYPE_SCALAR) {
      auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
      auto record_encoder = std::make_shared<dingodb::RecordEncoder>(1, serial_schema, partition_id);

      std::vector<std::string> origin_keys;
      dingodb::Helper::SplitString(key, ',', origin_keys);
      for (auto& key : origin_keys) {
        if (Helper::IsDateString(key)) {
          int64_t timestamp = Helper::StringToTimestamp(key);
          if (timestamp <= 0) {
            return butil::Status(-1, fmt::format("Invalid date({})", key));
          }
          key = std::to_string(timestamp * 1000);
        }
      }
      record_encoder->EncodeKeyPrefix(perfix, origin_keys, plain_key);

    } else if (index_type == dingodb::pb::common::INDEX_TYPE_VECTOR) {
      int64_t vector_id = dingodb::Helper::StringToInt64(key);

      plain_key = dingodb::VectorCodec::PackageVectorKey(perfix, partition_id, vector_id);

    } else if (index_type == dingodb::pb::common::INDEX_TYPE_DOCUMENT) {
      int64_t document_id = dingodb::Helper::StringToInt64(key);

      plain_key = dingodb::DocumentCodec::PackageDocumentKey(perfix, partition_id, document_id);
    }

    DINGO_LOG(INFO) << fmt::format("key: {} region: {} range: {}", dingodb::Helper::StringToHex(plain_key),
                                   distribution.id().entity_id(), dingodb::Helper::RangeToString(range));

    if (plain_key >= range.start_key() && plain_key < range.end_key()) {
      std::cout << fmt::format("key({}) in region({}).", key, distribution.id().entity_id()) << '\n';
      return butil::Status::OK();
    }
  }

  std::cout << "Not found key in any region." << '\n';

  return butil::Status::OK();
}

butil::Status WhichRegionForNewTable(const dingodb::pb::meta::TableDefinition& table_definition,
                                     const std::string& key) {
  std::map<int64_t, int64_t> region_partition_map;
  std::vector<dingodb::pb::coordinator::ScanRegionInfo> region_infos;
  for (const auto& partition : table_definition.table_partition().partitions()) {
    auto tmp_region_infos = GetRegionsByRange(partition.range());
    for (const auto& region_info : tmp_region_infos) {
      region_infos.push_back(region_info);
      region_partition_map[region_info.region_id()] = partition.id().entity_id();
    }
  }

  std::sort(region_infos.begin(), region_infos.end(),
            [](dingodb::pb::coordinator::ScanRegionInfo& r1, dingodb::pb::coordinator::ScanRegionInfo& r2) -> bool {
              return r1.range().start_key() > r2.range().start_key();
            });

  const auto index_type = table_definition.index_parameter().index_type();

  for (auto& region_info : region_infos) {
    int64_t partition_id = region_partition_map[region_info.region_id()];
    const auto& range = region_info.range();
    const char perfix = dingodb::Helper::GetKeyPrefix(range.start_key());

    std::string plain_key;
    if (index_type == dingodb::pb::common::INDEX_TYPE_NONE || index_type == dingodb::pb::common::INDEX_TYPE_SCALAR) {
      auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
      auto record_encoder = std::make_shared<dingodb::RecordEncoder>(1, serial_schema, partition_id);

      std::vector<std::string> origin_keys;
      dingodb::Helper::SplitString(key, ',', origin_keys);
      for (auto& key : origin_keys) {
        if (Helper::IsDateString(key)) {
          int64_t timestamp = Helper::StringToTimestamp(key);
          if (timestamp <= 0) {
            return butil::Status(-1, fmt::format("Invalid date({})", key));
          }
          key = std::to_string(timestamp * 1000);
        }
      }

      record_encoder->EncodeKeyPrefix(perfix, origin_keys, plain_key);

    } else if (index_type == dingodb::pb::common::INDEX_TYPE_VECTOR) {
      int64_t vector_id = dingodb::Helper::StringToInt64(key);

      plain_key = dingodb::VectorCodec::PackageVectorKey(perfix, partition_id, vector_id);

    } else if (index_type == dingodb::pb::common::INDEX_TYPE_DOCUMENT) {
      int64_t document_id = dingodb::Helper::StringToInt64(key);

      plain_key = dingodb::DocumentCodec::PackageDocumentKey(perfix, partition_id, document_id);
    }

    DINGO_LOG(INFO) << fmt::format("key: {} region: {} range: {}", dingodb::Helper::StringToHex(plain_key),
                                   region_info.region_id(), dingodb::Helper::RangeToString(range));

    if (plain_key >= range.start_key() && plain_key < range.end_key()) {
      std::cout << fmt::format("key({}) in region({}).", key, region_info.region_id()) << '\n';
      return butil::Status::OK();
    }
  }

  std::cout << "Not found key in any region." << '\n';

  return butil::Status();
}

butil::Status WhichRegion(WhichRegionOptions const& opt) {
  if (opt.id <= 0) {
    return butil::Status(-1, "Param id is error");
  }
  if (opt.key.empty()) {
    return butil::Status(-1, "Param key is empty");
  }

  dingodb::pb::meta::TableDefinition table_definition;
  auto status = GetTableOrIndexDefinition(opt.id, table_definition);
  if (!status.ok()) {
    return status;
  }
  if (table_definition.name().empty()) {
    return butil::Status(-1, "Not found table/index");
  }
  if (table_definition.table_partition().strategy() == dingodb::pb::meta::PT_STRATEGY_HASH) {
    return butil::Status(-1, "Not support hash partition table/index");
  }

  status = WhichRegionForOldTable(table_definition, opt.id, opt.key);
  if (status.ok()) {
    return status;
  }

  WhichRegionForNewTable(table_definition, opt.key);

  return butil::Status::OK();
}

void RunWhichRegion(WhichRegionOptions const& opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  auto status = WhichRegion(opt);
  if (!status.ok()) {
    Pretty::ShowError(status);
    return;
  }
}

void SetUpDumpRegion(CLI::App& app) {
  auto opt = std::make_shared<DumpRegionOptions>();
  auto* cmd = app.add_subcommand("DumpRegion", "Dump region")->group("Store Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Number of region_id")->required();
  cmd->add_option("--offset", opt->offset, "Request parameter offset")->default_val(0);
  cmd->add_option("--limit", opt->limit, "Request parameter limit")->default_val(10);
  cmd->add_option("--exclude_columns", opt->exclude_columns, "Exclude columns, e.g. col1,col2");
  cmd->callback([opt]() { RunDumpRegion(*opt); });
}

void SendDumpRegion(int64_t region_id, int64_t offset, int64_t limit, std::vector<std::string> exclude_columns) {
  dingodb::pb::debug::DumpRegionRequest request;
  dingodb::pb::debug::DumpRegionResponse response;

  request.set_region_id(region_id);
  request.set_offset(offset);
  request.set_limit(limit);

  InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "DumpRegion", request, response);
  if (Pretty::ShowError(response.error())) {
    return;
  }

  // get table definition
  dingodb::pb::meta::TableDefinition table_definition;
  auto status = GetTableOrIndexDefinition(response.table_id(), table_definition);
  if (!status.ok()) {
    Pretty::Show(response.data());
    Pretty::ShowError(status);
    return;
  }

  Pretty::Show(response.data(), table_definition, exclude_columns);
}

void RunDumpRegion(DumpRegionOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  std::vector<std::string> exclude_columns;
  dingodb::Helper::SplitString(opt.exclude_columns, ',', exclude_columns);

  SendDumpRegion(opt.region_id, opt.offset, opt.limit, exclude_columns);
}

void SetUpRegionMetrics(CLI::App& app) {
  auto opt = std::make_shared<RegionMetricsOptions>();
  auto* cmd = app.add_subcommand("GetRegionMetrics", "Get region metrics ")->group("Region Command");
  cmd->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  cmd->add_option("--region_ids", opt->region_ids, "Request parameter, empty means query all regions");
  cmd->add_option("--type", opt->type,
                  "Request parameter type, only support 6 or 8, 6 means store region metrics;8 means store region "
                  "actual metrics.")
      ->required();
  cmd->callback([opt]() { RunRegionMetrics(*opt); });
}

void RunRegionMetrics(RegionMetricsOptions const& opt) {
  if (!SetUpStore("", {opt.store_addrs}, 0)) {
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
      std::cout << it.DebugString() << '\n';
    }
  } else if (opt.type == 8) {
    for (const auto& it : response.region_actual_metrics().region_metricses()) {
      std::cout << it.DebugString() << '\n';
    }
  }
}

void SetUpQueryRegionStatusMetrics(CLI::App& app) {
  auto opt = std::make_shared<QueryRegionStatusOptions>();
  auto* cmd = app.add_subcommand("QueryRegionStatus", "Query region status ")->group("Region Command");
  cmd->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  cmd->add_option("--region_ids", opt->region_ids, "Request parameter, empty means query all regions");
  cmd->callback([opt]() { RunQueryRegionStatus(*opt); });
}

void RunQueryRegionStatus(QueryRegionStatusOptions const& opt) {
  if (!SetUpStore("", {opt.store_addrs}, 0)) {
    exit(-1);
  }
  dingodb::pb::debug::DebugRequest request;
  dingodb::pb::debug::DebugResponse response;

  request.set_type(::dingodb::pb::debug::DebugType::STORE_REGION_META_DETAILS);

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
  for (auto const& it : response.region_meta_details().regions()) {
    std::cout << it.DebugString() << '\n';
  }
  if (response.region_meta_details().regions_size() == 0) {
    std::string region_ids;
    for (size_t i = 0; i < opt.region_ids.size(); ++i) {
      region_ids += std::to_string(opt.region_ids[i]);
      if (i != opt.region_ids.size() - 1) {
        region_ids += ", ";
      }
    }
    std::cout << "Region: " << region_ids << " not exist." << '\n';
  }
}

void SetUpModifyRegionMeta(CLI::App& app) {
  auto opt = std::make_shared<ModifyRegionMetaOptions>();
  auto* cmd = app.add_subcommand("ModifyRegionMeta", "Modify region meta ")->group("Region Command");
  cmd->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  cmd->add_option("--region_id", opt->region_id, "Request parameter, empty means query all regions");
  cmd->add_option("--state", opt->state,
                  "Request parameter, 0:new, 1: normal, 2: standby, 3: splitting, 4: merge, 5: deleting, 6: deleted, "
                  "7: orphan, 8: tombstone")
      ->required()
      ->check(CLI::Range(0, 8));
  cmd->callback([opt]() { RunModifyRegionMeta(*opt); });
}

void RunModifyRegionMeta(ModifyRegionMetaOptions const& opt) {
  if (!SetUpStore("", {opt.store_addrs}, 0)) {
    exit(-1);
  }
  dingodb::pb::debug::ModifyRegionMetaRequest request;
  dingodb::pb::debug::ModifyRegionMetaResponse response;

  request.set_region_id(opt.region_id);
  request.set_state(::dingodb::pb::common::StoreRegionState(opt.state));
  request.add_fields("state");

  auto status = InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "ModifyRegionMeta", request,
                                                                            response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Get region metrics  failed, error:"
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "modify region: " << opt.region_id
            << ", state: " << ::dingodb::pb::common::StoreRegionState_Name(opt.state) << " success.\n";
}

void SetUpQueryMemoryLocks(CLI::App& app) {
  auto opt = std::make_shared<QueryMemoryLocksOptions>();
  auto* cmd = app.add_subcommand("QueryMemoryLocks", "Query region memory locks ")->group("Region Command");
  cmd->add_option("--region_id", opt->region_id, "Request parameter, empty means query all regions")->required();
  cmd->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  cmd->callback([opt]() { RunQueryMemoryLocks(*opt); });
}

void RunQueryMemoryLocks(QueryMemoryLocksOptions const& opt) {
  if (!SetUpStore("", {opt.store_addrs}, 0)) {
    exit(-1);
  }

  dingodb::pb::debug::DumpMemoryLockRequest request;
  dingodb::pb::debug::DumpMemoryLockResponse response;

  request.set_region_id(opt.region_id);

  auto status = InteractionManager::GetInstance().SendRequestWithoutContext("DebugService", "DumpRegionMemoryLock",
                                                                            request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Query region memory locks , error:"
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "region memory locks: " << response.DebugString() << std::endl;
}

void SendKvGet(KvGetOptions const& opt, std::string& value) {
  dingodb::pb::store::KvGetRequest request;
  dingodb::pb::store::KvGetResponse response;
  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_key(dingodb::Helper::HexToString(opt.key));

  auto status = InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvGet", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "kv get failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  value = response.value();
  std::cout << "value: " << value << '\n';
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
  kv->set_key(dingodb::Helper::HexToString(opt.key));
  kv->set_value(value.empty() ? Helper::GenRandomString(256) : value);

  auto status = InteractionManager::GetInstance().SendRequestWithContext("StoreService", "KvPut", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "kv put failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return response.error().errcode();
  }
  std::cout << "value:" << value << '\n';
  return response.error().errcode();
}

void SendKvBatchPut(KvBatchPutOptions const& opt) {
  dingodb::pb::store::KvBatchPutRequest request;
  dingodb::pb::store::KvBatchPutResponse response;
  std::string prefix = dingodb::Helper::HexToString(opt.prefix);
  std::string internal_prefix = prefix;

  if (internal_prefix.empty()) {
    dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
    if (region.id() == 0) {
      DINGO_LOG(ERROR) << "GetRegion failed";
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

  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    DINGO_LOG(ERROR) << "GetRegion failed";
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

std::string GetServiceName(const dingodb::pb::common::Region& region) {
  CHECK(region.id() > 0) << "region id is invalid.";

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
    DINGO_LOG(FATAL) << "region_type is invalid";
  }

  return service_name;
}

// unified
void SendTxnGet(TxnGetOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    DINGO_LOG(ERROR) << "GetRegion failed";
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
  if (opt.is_hex) {
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

void SendTxnPessimisticLock(TxnPessimisticLockOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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
  if (opt.return_values) {
    request.set_return_values(true);
  }
  if (opt.primary_lock.empty()) {
    std::cout << "primary_lock is empty" << '\n';
    return;
  } else {
    std::string key = opt.primary_lock;
    if (opt.is_hex) {
      key = HexToString(opt.primary_lock);
    }
    request.set_primary_lock(key);
  }

  if (opt.start_ts == 0) {
    std::cout << "start_ts is empty" << '\n';
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.lock_ttl == 0) {
    std::cout << "lock_ttl is empty" << '\n';
    return;
  }
  request.set_lock_ttl(opt.lock_ttl);

  if (opt.for_update_ts == 0) {
    std::cout << "for_update_ts is empty" << '\n';
    return;
  }
  request.set_for_update_ts(opt.for_update_ts);

  if (opt.mutation_op.empty()) {
    std::cout << "mutation_op is empty, mutation MUST be one of [lock]" << '\n';
    return;
  }
  if (opt.key.empty()) {
    std::cout << "key is empty" << '\n';
    return;
  }
  std::string target_key = opt.key;
  if (opt.is_hex) {
    target_key = HexToString(opt.key);
  }
  if (opt.value.empty()) {
    std::cout << "value is empty" << '\n';
    return;
  }
  std::string target_value = opt.value;
  if (opt.value_is_hex) {
    target_value = HexToString(opt.value);
  }
  if (opt.mutation_op == "lock") {
    if (opt.value.empty()) {
      std::cout << "value is empty" << '\n';
      return;
    }
    auto* mutation = request.add_mutations();
    mutation->set_op(::dingodb::pb::store::Op::Lock);
    mutation->set_key(target_key);
    mutation->set_value(target_value);
  } else {
    std::cout << "mutation_op MUST be [lock]" << '\n';
    return;
  }

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticLock", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "txn pessimistic lock failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg() << '\n';
    return;
  }
  std::cout << "txn pessimistic lock success." << '\n';
  if (response.txn_result_size() > 0) {
    std::cout << "txn_result:{ \n";
    for (auto const& txn_result : response.txn_result()) {
      std::cout << "\t " << txn_result.DebugString() << '\n';
    }
    std::cout << "}\n";
  } else if (opt.return_values) {
    if (region.region_type() == dingodb::pb::common::RegionType::STORE_REGION) {
      std::cout << "KeyValue:{ \n";
      for (auto const& kv : response.kvs()) {
        std::cout << kv.DebugString() << "\n";
      }
      std::cout << "}\n";
    } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
               region.definition().index_parameter().has_vector_index_parameter()) {
      std::cout << "VectorWithId:{ \n";
      for (auto const& vector : response.vector()) {
        std::cout << vector.DebugString() << "\n";
      }
      std::cout << "}\n";
    } else if (region.region_type() == dingodb::pb::common::DOCUMENT_REGION &&
               region.definition().index_parameter().has_document_index_parameter()) {
      std::cout << "DocumentWithId:{ \n";
      for (auto const& document : response.documents()) {
        std::cout << document.DebugString() << "\n";
      }
      std::cout << "}\n";
    }
  }
  std::cout << '\n';
  // DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnPessimisticRollback(TxnPessimisticRollbackOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
    return;
  }

  std::string service_name = GetServiceName(region);

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
    std::cout << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.for_update_ts == 0) {
    std::cout << "for_update_ts is empty";
    return;
  }
  request.set_for_update_ts(opt.for_update_ts);

  if (opt.key.empty()) {
    std::cout << "key is empty";
    return;
  }
  std::string target_key = opt.key;
  if (opt.is_hex) {
    target_key = HexToString(opt.key);
  }
  *request.add_keys() = target_key;

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnPessimisticRollback", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "txn pessimistic rollback failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  if (response.txn_result_size() > 0) {
    std::cout << "txn_result:{\n";
    for (auto const& txn_result : response.txn_result()) {
      std::cout << "\t " << txn_result.DebugString() << "\n";
    }
    std::cout << "}\n";
  } else {
    std::cout << "txn pessimistic rollback success.\n";
  }
  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnPrewrite(TxnPrewriteOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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
    std::cout << "region_type is invalid" << '\n';
  }
  std::cout << "Prewrite success." << '\n';
}
void SendTxnCommit(TxnCommitOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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
    std::cout << "start_ts is empty" << '\n';
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.commit_ts == 0) {
    std::cout << "commit_ts is empty" << '\n';
    return;
  }
  request.set_commit_ts(opt.commit_ts);

  if (opt.key.empty()) {
    std::cout << "key is empty" << '\n';
    return;
  }
  std::string target_key = opt.key;
  if (opt.is_hex) {
    target_key = HexToString(opt.key);
  }
  request.add_keys()->assign(target_key);

  if (!opt.key2.empty()) {
    std::string target_key2 = opt.key2;
    if (opt.is_hex) {
      target_key2 = HexToString(opt.key2);
    }
    request.add_keys()->assign(target_key2);
  }

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnCommit", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "txn commit failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg() << "txn_result: " << response.txn_result().DebugString();
    return;
  }
  std::cout << "txn commit success, txn_result: " << response.txn_result().DebugString()
            << " commit_ts: " << response.commit_ts() << '\n';
}

void SendTxnCheckTxnStatus(TxnCheckTxnStatusOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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
    if (opt.is_hex) {
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
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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
    if (opt.is_hex) {
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
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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
    std::cout << "key is empty";
    return;
  }
  if (opt.key2.empty()) {
    std::cout << "key2 is empty";
    return;
  }
  std::string target_key = opt.key;
  if (opt.is_hex) {
    target_key = HexToString(opt.key);
  }
  std::string target_key2 = opt.key2;
  if (opt.is_hex) {
    target_key2 = HexToString(opt.key2);
  }
  request.add_keys()->assign(target_key);
  request.add_keys()->assign(target_key2);

  if (opt.start_ts == 0) {
    std::cout << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.resolve_locks > 0) {
    request.mutable_context()->add_resolved_locks(opt.resolve_locks);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnBatchGet", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "txn batch get failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "txn_result: " << response.txn_result().DebugString() << "\n";
  if (region.region_type() == dingodb::pb::common::RegionType::STORE_REGION) {
    std::cout << "KeyValue:{ \n";
    for (auto const& kv : response.kvs()) {
      std::cout << kv.DebugString() << "\n";
    }
    std::cout << "}\n";
  } else if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
             region.definition().index_parameter().has_vector_index_parameter()) {
    std::cout << "VectorWithId:{ \n";
    for (auto const& vector : response.vectors()) {
      std::cout << vector.DebugString() << "\n";
    }
    std::cout << "}\n";
  } else if (region.region_type() == dingodb::pb::common::DOCUMENT_REGION &&
             region.definition().index_parameter().has_document_index_parameter()) {
    std::cout << "DocumentWithId:{ \n";
    for (auto const& document : response.documents()) {
      std::cout << document.DebugString() << "\n";
    }
    std::cout << "}\n";
  }

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendTxnBatchRollback(TxnBatchRollbackOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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
  if (opt.is_hex) {
    target_key = HexToString(opt.key);
  }
  request.add_keys()->assign(target_key);

  if (!opt.key2.empty()) {
    std::string target_key2 = opt.key2;
    if (opt.is_hex) {
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

void SendTxnHeartBeat(TxnHeartBeatOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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
    if (opt.is_hex) {
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
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnGc", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "txn gc failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  if (response.has_txn_result()) {
    std::cout << "txn_result: " << response.txn_result().DebugString() << std::endl;
  } else {
    std::cout << "txn gc success." << std::endl;
  }
}

dingodb::pb::store::TxnScanResponse SendTxnCount(dingodb::pb::common::Region region,
                                                 const dingodb::pb::common::Range& range, int64_t start_ts,
                                                 int64_t resolve_locks) {
  dingodb::pb::store::TxnScanResponse response;
  dingodb::pb::store::TxnScanRequest request;
  request.mutable_context()->set_region_id(region.id());
  *request.mutable_context()->mutable_region_epoch() = region.definition().epoch();
  request.mutable_context()->set_isolation_level(dingodb::pb::store::IsolationLevel::SnapshotIsolation);
  request.mutable_context()->add_resolved_locks(resolve_locks);
  request.mutable_request_info()->set_request_id(1111111111);

  dingodb::pb::common::RangeWithOptions range_with_option;
  range_with_option.mutable_range()->CopyFrom(range);
  range_with_option.set_with_start(true);
  range_with_option.set_with_end(false);

  request.mutable_range()->Swap(&range_with_option);

  request.set_start_ts(start_ts);

  request.set_limit(1);

  // do not modify coprocessor . call me first.
  dingodb::pb::common::CoprocessorV2 coprocessor;
  std::string rel_expr;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmultichar"
  rel_expr.push_back(static_cast<char>(0x74));
  rel_expr.push_back(static_cast<char>(0x01));
  rel_expr.push_back(static_cast<char>(0x10));
  // std::string rel_expr_str = dingodb::Helper::HexToString(rel_expr);
  std::string rel_expr_str = rel_expr;
#pragma GCC diagnostic pop
  coprocessor.set_schema_version(1);

  coprocessor.mutable_original_schema()->set_common_id(60059);
  dingodb::pb::common::Schema original_schema;
  original_schema.set_type(dingodb::pb::common::Schema::Type::Schema_Type_INTEGER);
  original_schema.set_is_key(true);
  original_schema.set_is_nullable(false);
  original_schema.set_index(0);
  original_schema.set_name("");
  coprocessor.mutable_original_schema()->mutable_schema()->Add()->CopyFrom(original_schema);

  coprocessor.mutable_result_schema()->set_common_id(60059);
  dingodb::pb::common::Schema result_schema;
  result_schema.set_type(dingodb::pb::common::Schema::Type::Schema_Type_LONG);
  result_schema.set_is_key(false);
  result_schema.set_is_nullable(false);
  result_schema.set_index(0);
  result_schema.set_name("");
  coprocessor.mutable_result_schema()->mutable_schema()->Add()->CopyFrom(result_schema);

  coprocessor.add_selection_columns(0);

  coprocessor.set_rel_expr(rel_expr_str);
  coprocessor.set_codec_version(2);
  coprocessor.set_for_agg_count(true);

  *request.mutable_coprocessor() = coprocessor;

  for (;;) {
    dingodb::pb::store::TxnScanResponse sub_response;

    // maybe current store interaction is not store node, so need reset.
    InteractionManager::GetInstance().ResetStoreInteraction();
    auto status = InteractionManager::GetInstance().SendRequestWithContext(GetServiceName(region), "TxnScan", request,
                                                                           sub_response);
    if (!status.ok()) {
      response.mutable_error()->set_errcode(dingodb::pb::error::Errno(status.error_code()));
      response.mutable_error()->set_errmsg(status.error_str());
      break;
    }

    if (sub_response.error().errcode() != dingodb::pb::error::OK) {
      *response.mutable_error() = sub_response.error();
      break;
    }

    response = sub_response;
    break;
  }

  return response;
}

void SendTxnDeleteRange(TxnDeleteRangeOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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
    if (opt.is_hex) {
      key = HexToString(opt.start_key);
    }
    request.set_start_key(key);
  }

  if (opt.end_key.empty()) {
    DINGO_LOG(ERROR) << "end_key is empty";
    return;
  } else {
    std::string key = opt.end_key;
    if (opt.is_hex) {
      key = HexToString(opt.end_key);
    }
    request.set_end_key(key);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext(service_name, "TxnDeleteRange", request, response);

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}
void SendTxnDump(TxnDumpOptions const& opt) {
  dingodb::pb::common::Region region = SendQueryRegion(opt.region_id);
  if (region.id() == 0) {
    std::cout << "GetRegion failed." << '\n';
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

  if (opt.is_hex) {
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
    std::cout << "Response TxnResult: " << response.txn_result().ShortDebugString() << '\n';
  }

  std::cout << "data_size: " << response.data_keys_size() << '\n';
  std::cout << "lock_size: " << response.lock_keys_size() << '\n';
  std::cout << "write_size: " << response.write_keys_size() << '\n';

  for (int i = 0; i < response.data_keys_size(); i++) {
    std::cout << "data[" << i << "] hex_key: [" << StringToHex(response.data_keys(i).key()) << "] key: ["
              << response.data_keys(i).ShortDebugString() << "], value: [" << response.data_values(i).ShortDebugString()
              << "]" << '\n';
  }

  for (int i = 0; i < response.lock_keys_size(); i++) {
    std::cout << "lock[" << i << "] hex_key: [" << StringToHex(response.lock_keys(i).key()) << "] key: ["
              << response.lock_keys(i).ShortDebugString() << "], value: [" << response.lock_values(i).ShortDebugString()
              << "]" << '\n';
  }

  for (int i = 0; i < response.write_keys_size(); i++) {
    std::cout << "write[" << i << "] hex_key: [" << StringToHex(response.write_keys(i).key()) << "] key: ["
              << response.write_keys(i).ShortDebugString() << "], value: ["
              << response.write_values(i).ShortDebugString() << "]" << '\n';
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
    std::cout << "primary_lock is empty";
    return;
  }
  if (opt.is_hex) {
    request.set_primary_lock(HexToString(opt.primary_lock));
  } else {
    request.set_primary_lock(opt.primary_lock);
  }

  if (opt.start_ts == 0) {
    std::cout << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.lock_ttl == 0) {
    std::cout << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(opt.lock_ttl);

  if (opt.txn_size == 0) {
    std::cout << "txn_size is empty";
    return;
  }
  request.set_txn_size(opt.txn_size);

  request.set_try_one_pc(opt.try_one_pc);
  request.set_max_commit_ts(opt.max_commit_ts);

  if (opt.mutation_op.empty()) {
    std::cout << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }
  if (opt.key.empty()) {
    std::cout << "key is empty";
    return;
  }
  std::string target_key = opt.key;
  std::string target_key2 = opt.key2;
  if (opt.is_hex) {
    target_key = HexToString(opt.key);
    target_key2 = HexToString(opt.key2);
  }
  if (opt.mutation_op == "put") {
    if (opt.value.empty()) {
      std::cout << "value is empty";
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
      std::cout << "value is empty";
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
    std::cout << "mutation_op MUST be one of [put, delete, insert]";
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
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "txn prewrite failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "prewrite success.\n";
  std::cout << "txn_result:{ \n";
  for (auto const& txn_result : response.txn_result()) {
    std::cout << "\t " << txn_result.DebugString();
  }
  std::cout << " }\n";
  std::cout << "keys_already_exist: {\n";
  for (auto const& keys : response.keys_already_exist()) {
    std::cout << "\t " << keys.DebugString();
  }
  std::cout << " }\n";
  std::cout << "one_pc_commit_ts: " << response.one_pc_commit_ts();
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
    std::cout << "primary_lock is empty";
    return;
  }
  request.set_primary_lock(opt.primary_lock);

  if (opt.start_ts == 0) {
    std::cout << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.lock_ttl == 0) {
    std::cout << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(opt.lock_ttl);

  if (opt.txn_size == 0) {
    std::cout << "txn_size is empty";
    return;
  }
  request.set_txn_size(opt.txn_size);

  request.set_try_one_pc(opt.try_one_pc);
  request.set_max_commit_ts(opt.max_commit_ts);

  if (opt.mutation_op.empty()) {
    std::cout << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }

  if (opt.vector_id == 0) {
    std::cout << "vector_id is empty";
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
    std::cout << "vector_index_type is empty";
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
    std::cout << "mutation_op MUST be one of [put, delete, insert]";
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
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "txn prewrite failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "prewrite success.\n";
  std::cout << "txn_result:{ \n";
  for (auto const& txn_result : response.txn_result()) {
    std::cout << "\t " << txn_result.DebugString();
  }
  std::cout << " }\n";
  std::cout << "keys_already_exist: {\n";
  for (auto const& keys : response.keys_already_exist()) {
    std::cout << "\t " << keys.DebugString();
  }
  std::cout << " }\n";
  std::cout << "one_pc_commit_ts: " << response.one_pc_commit_ts();
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
    std::cout << "primary_lock is empty";
    return;
  }
  request.set_primary_lock(opt.primary_lock);

  if (opt.start_ts == 0) {
    std::cout << "start_ts is empty";
    return;
  }
  request.set_start_ts(opt.start_ts);

  if (opt.lock_ttl == 0) {
    std::cout << "lock_ttl is empty";
    return;
  }
  request.set_lock_ttl(opt.lock_ttl);

  if (opt.txn_size == 0) {
    std::cout << "txn_size is empty";
    return;
  }
  request.set_txn_size(opt.txn_size);

  request.set_try_one_pc(opt.try_one_pc);
  request.set_max_commit_ts(opt.max_commit_ts);

  if (opt.mutation_op.empty()) {
    std::cout << "mutation_op is empty, mutation MUST be one of [put, delete, insert]";
    return;
  }

  if (opt.document_id == 0) {
    std::cout << "document_id is empty";
    return;
  }

  int64_t part_id = region.definition().part_id();
  int64_t dimension = 0;

  if (region.region_type() != dingodb::pb::common::RegionType::DOCUMENT_REGION) {
    std::cout << "region_type is invalid, only document region can use this function";
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
      std::cout << "document_text1 is empty";
      return;
    }

    if (opt.document_text2.empty()) {
      std::cout << "document_text2 is empty";
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
      std::cout << "document_text1 is empty";
      return;
    }

    if (opt.document_text2.empty()) {
      std::cout << "document_text2 is empty";
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
    std::cout << "mutation_op MUST be one of [put, delete, insert]";
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
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "txn prewrite failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "prewrite success.\n";
  std::cout << "txn_result:{ \n";
  for (auto const& txn_result : response.txn_result()) {
    std::cout << "\t " << txn_result.DebugString();
  }
  std::cout << " }\n";
  std::cout << "keys_already_exist: {\n";
  for (auto const& keys : response.keys_already_exist()) {
    std::cout << "\t " << keys.DebugString();
  }
  std::cout << " }\n";
  std::cout << "one_pc_commit_ts: " << response.one_pc_commit_ts();
  DINGO_LOG(INFO) << "Response: " << response.DebugString();
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
  std::cout << "region " << opt.region_id << " do snapshot success " << '\n';
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
  if (Pretty::ShowError(response.error())) {
    return;
  }
  std::cout << "Compact success." << std::endl;
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

}  // namespace client_v2