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

#include "sdk/transaction/txn_impl.h"

#include <cstdint>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "sdk/client.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/common/param_config.h"
#include "sdk/status.h"
#include "sdk/store/store_rpc.h"
#include "sdk/transaction/txn_buffer.h"
#include "sdk/transaction/txn_common.h"

namespace dingodb {
namespace sdk {

Transaction::TxnImpl::TxnImpl(const ClientStub& stub, const TransactionOptions& options)
    : stub_(stub), options_(options), state_(kInit), buffer_(new TxnBuffer()) {}

Status Transaction::TxnImpl::Begin() {
  pb::meta::TsoTimestamp tso;
  Status ret = stub_.GetAdminTool()->GetCurrentTsoTimeStamp(tso);
  if (ret.ok()) {
    start_tso_ = tso;
    start_ts_ = Tso2Timestamp(start_tso_);
    state_ = kActive;
  }
  return ret;
}

std::unique_ptr<TxnGetRpc> Transaction::TxnImpl::PrepareTxnGetRpc(const std::shared_ptr<Region>& region) const {
  auto rpc = std::make_unique<TxnGetRpc>();
  rpc->MutableRequest()->set_start_ts(start_ts_);
  FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 TransactionIsolation2IsolationLevel(options_.isolation));
  return std::move(rpc);
}

Status Transaction::TxnImpl::DoTxnGet(const std::string& key, std::string& value) {
  std::shared_ptr<Region> region;
  Status ret = stub_.GetMetaCache()->LookupRegionByKey(key, region);
  if (!ret.IsOK()) {
    return ret;
  }

  std::unique_ptr<TxnGetRpc> rpc = PrepareTxnGetRpc(region);
  rpc->MutableRequest()->set_key(key);

  int retry = 0;
  while (true) {
    DINGO_RETURN_NOT_OK(LogAndSendRpc(stub_, *rpc, region));

    const auto* response = rpc->Response();
    if (response->has_txn_result()) {
      ret = CheckTxnResultInfo(response->txn_result());
    }

    if (ret.ok()) {
      break;
    } else if (ret.IsTxnLockConflict()) {
      ret = stub_.GetTxnLockResolver()->ResolveLock(response->txn_result().locked(), start_ts_);
      if (!ret.ok()) {
        break;
      }
    } else {
      DINGO_LOG(WARNING) << "unexpect txn get rpc response, status:" << ret.ToString()
                         << " response:" << response->DebugString();
      break;
    }

    if (NeedRetryAndInc(retry)) {
      // TODO: set txn retry ms
      DINGO_LOG(INFO) << "try to delay:" << FLAGS_txn_op_delay_ms << "ms";
      DelayRetry(FLAGS_txn_op_delay_ms);
    } else {
      break;
    }
  }

  if (ret.ok()) {
    const auto* response = rpc->Response();
    if (response->value().empty()) {
      ret = Status::NotFound(fmt::format("key:{} not found", key));
    } else {
      value = response->value();
    }
  }

  return ret;
}

Status Transaction::TxnImpl::Get(const std::string& key, std::string& value) {
  TxnMutation mutation;
  Status ret = buffer_->Get(key, mutation);
  if (ret.ok()) {
    switch (mutation.type) {
      case kPut:
        value = mutation.value;
        return Status::OK();
      case kDelete:
        return Status::NotFound("");
      case kPutIfAbsent:
        // NOTE: directy return is ok?
        value = mutation.value;
        return Status::OK();
      default:
        CHECK(false) << "unknow mutation type, mutation:" << mutation.ToString();
    }
  }

  return DoTxnGet(key, value);
}

void Transaction::TxnImpl::ProcessTxnBatchGetSubTask(TxnSubTask* sub_task) {
  auto* rpc = CHECK_NOTNULL(dynamic_cast<TxnBatchGetRpc*>(sub_task->rpc));

  Status res;
  int retry = 0;
  while (true) {
    res = LogAndSendRpc(stub_, *rpc, sub_task->region);

    if (!res.ok()) {
      break;
    }

    const auto* response = rpc->Response();
    if (response->has_txn_result()) {
      res = CheckTxnResultInfo(response->txn_result());
    }

    if (res.ok()) {
      break;
    } else if (res.IsTxnLockConflict()) {
      res = stub_.GetTxnLockResolver()->ResolveLock(response->txn_result().locked(), start_ts_);
      if (!res.ok()) {
        break;
      }
    } else {
      DINGO_LOG(WARNING) << "unexpect txn batch get rpc response, status:" << res.ToString()
                         << " response:" << response->DebugString();
      break;
    }

    if (NeedRetryAndInc(retry)) {
      // TODO: set txn retry ms
      DINGO_LOG(INFO) << "try to delay:" << FLAGS_txn_op_delay_ms << "ms";
      DelayRetry(FLAGS_txn_op_delay_ms);
    } else {
      break;
    }
  }

  if (res.ok()) {
    for (const auto& kv : rpc->Response()->kvs()) {
      if (!kv.value().empty()) {
        sub_task->result_kvs.push_back({kv.key(), kv.value()});
      } else {
        DINGO_LOG(DEBUG) << "Ignore kv key:" << kv.key() << " because value is empty";
      }
    }
  }

  sub_task->status = res;
}

std::unique_ptr<TxnBatchGetRpc> Transaction::TxnImpl::PrepareTxnBatchGetRpc(
    const std::shared_ptr<Region>& region) const {
  auto rpc = std::make_unique<TxnBatchGetRpc>();
  rpc->MutableRequest()->set_start_ts(start_ts_);
  FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 TransactionIsolation2IsolationLevel(options_.isolation));
  return std::move(rpc);
}

// TODO: return not found keys
Status Transaction::TxnImpl::DoTxnBatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  auto meta_cache = stub_.GetMetaCache();
  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<std::string>> region_keys;

  for (const auto& key : keys) {
    std::shared_ptr<Region> tmp;
    Status got = meta_cache->LookupRegionByKey(key, tmp);
    if (!got.IsOK()) {
      return got;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_keys[tmp->RegionId()].push_back(key);
  }

  std::vector<TxnSubTask> sub_tasks;
  std::vector<std::unique_ptr<TxnBatchGetRpc>> rpcs;

  for (const auto& entry : region_keys) {
    auto region_id = entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = PrepareTxnBatchGetRpc(region);

    for (const auto& key : entry.second) {
      auto* fill = rpc->MutableRequest()->add_keys();
      *fill = key;
    }

    sub_tasks.emplace_back(rpc.get(), region);
    rpcs.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs.size(), region_keys.size());
  DCHECK_EQ(rpcs.size(), sub_tasks.size());

  std::vector<std::thread> thread_pool;
  for (auto i = 1; i < sub_tasks.size(); i++) {
    thread_pool.emplace_back(&Transaction::TxnImpl::ProcessTxnBatchGetSubTask, this, &sub_tasks[i]);
  }

  ProcessTxnBatchGetSubTask(sub_tasks.data());

  for (auto& thread : thread_pool) {
    thread.join();
  }

  Status result;
  std::vector<KVPair> tmp_kvs;
  for (auto& state : sub_tasks) {
    if (!state.status.IsOK()) {
      DINGO_LOG(WARNING) << "Fail txn_batch_get_sub_task, rpc: " << state.rpc->Method()
                         << " send to region: " << state.region->RegionId() << " status: " << state.status.ToString();
      if (result.ok()) {
        // only return first fail status
        result = state.status;
      }
    } else {
      tmp_kvs.insert(tmp_kvs.end(), std::make_move_iterator(state.result_kvs.begin()),
                     std::make_move_iterator(state.result_kvs.end()));
    }
  }

  kvs = std::move(tmp_kvs);
  return result;
}

Status Transaction::TxnImpl::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  std::vector<std::string> not_found;
  std::vector<KVPair> to_return;
  Status ret;
  for (const auto& key : keys) {
    TxnMutation mutation;
    ret = buffer_->Get(key, mutation);
    if (ret.IsOK()) {
      switch (mutation.type) {
        case kPut:
          to_return.push_back({key, mutation.value});
          continue;
        case kDelete:
          continue;
        case kPutIfAbsent:
          // NOTE: use this value is ok?
          to_return.push_back({key, mutation.value});
          continue;
        default:
          CHECK(false) << "unknow mutation type, mutation:" << mutation.ToString();
      }
    } else {
      CHECK(ret.IsNotFound());
      not_found.push_back(key);
    }
  }

  if (!not_found.empty()) {
    std::vector<KVPair> batch_get;
    ret = DoTxnBatchGet(not_found, batch_get);
    to_return.insert(to_return.end(), std::make_move_iterator(batch_get.begin()),
                     std::make_move_iterator(batch_get.end()));
  }

  kvs = std::move(to_return);

  return ret;
}

Status Transaction::TxnImpl::Put(const std::string& key, const std::string& value) { return buffer_->Put(key, value); }

Status Transaction::TxnImpl::BatchPut(const std::vector<KVPair>& kvs) { return buffer_->BatchPut(kvs); }

Status Transaction::TxnImpl::PutIfAbsent(const std::string& key, const std::string& value) {
  return buffer_->PutIfAbsent(key, value);
}

Status Transaction::TxnImpl::BatchPutIfAbsent(const std::vector<KVPair>& kvs) { return buffer_->BatchPutIfAbsent(kvs); }

Status Transaction::TxnImpl::Delete(const std::string& key) { return buffer_->Delete(key); }

Status Transaction::TxnImpl::BatchDelete(const std::vector<std::string>& keys) { return buffer_->BatchDelete(keys); }

Status Transaction::TxnImpl::Scan(const std::string& start_key, const std::string& end_key, uint64_t limit,
                                  std::vector<KVPair>& kvs) {
  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty, check params");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key, check params");
  }

  auto meta_cache = stub_.GetMetaCache();
  {
    // precheck: return not found if no region in [start, end_key)
    std::shared_ptr<Region> region;
    Status ret = meta_cache->LookupRegionBetweenRange(start_key, end_key, region);
    if (!ret.IsOK()) {
      if (ret.IsNotFound()) {
        DINGO_LOG(WARNING) << fmt::format("region not found between [{},{}), no need retry, status:{}", start_key,
                                          end_key, ret.ToString());
      } else {
        DINGO_LOG(WARNING) << fmt::format("lookup region fail between [{},{}), need retry, status:{}", start_key,
                                          end_key, ret.ToString());
      }
      return ret;
    }
  }

  std::vector<TxnMutation> range_mutations;
  CHECK(buffer_->Range(start_key, end_key, range_mutations).ok());

  uint64_t redundant_limit = limit;
  if (redundant_limit != 0) {
    uint64_t delete_count = 0;
    for (const auto& mutaion : range_mutations) {
      if (mutaion.type == TxnMutationType::kDelete) {
        delete_count++;
      }
    }
    redundant_limit = limit + delete_count;
  }

  std::string next_start = start_key;
  std::map<std::string, std::string> tmp_kvs;

  DINGO_LOG(INFO) << fmt::format("txn scan start between [{},{}), next_start:{}, limit:{}, redundant_limit:{}",
                                 start_key, end_key, next_start, limit, redundant_limit);

  while (next_start < end_key) {
    std::shared_ptr<Region> region;
    Status ret = meta_cache->LookupRegionBetweenRange(next_start, end_key, region);

    if (ret.IsNotFound()) {
      DINGO_LOG(INFO) << fmt::format("Break scan because region not found  between [{},{}), start_key:{} status:{}",
                                     next_start, end_key, start_key, ret.ToString());
      break;
    }

    if (!ret.IsOK()) {
      DINGO_LOG(WARNING) << fmt::format("region look fail between [{},{}), start_key:{} status:{}", next_start, end_key,
                                        start_key, ret.ToString());
      return ret;
    }

    std::string scanner_start_key =
        next_start <= region->Range().start_key() ? region->Range().start_key() : next_start;
    std::string scanner_end_key = end_key <= region->Range().end_key() ? end_key : region->Range().end_key();
    ScannerOptions scan_options(stub_, region, scanner_start_key, scanner_end_key, options_, start_ts_);
    std::shared_ptr<RegionScanner> scanner;
    CHECK(stub_.GetTxnRegionScannerFactory()->NewRegionScanner(scan_options, scanner).IsOK());
    ret = scanner->Open();
    CHECK(ret.ok());

    DINGO_LOG(INFO) << fmt::format("region:{} scan start, region range:({}-{})", region->RegionId(),
                                   region->Range().start_key(), region->Range().end_key());

    while (scanner->HasMore()) {
      DINGO_LOG(DEBUG) << fmt::format("start call next batch, limit:{}, redundant_limit:{}, tmp_kvs_size:{}", limit,
                                      redundant_limit, tmp_kvs.size());
      std::vector<KVPair> scan_kvs;
      ret = scanner->NextBatch(scan_kvs);
      if (!ret.IsOK()) {
        DINGO_LOG(WARNING) << fmt::format("txn region scanner NextBatch fail, region:{}, status:{}", region->RegionId(),
                                          ret.ToString());
        return ret;
      }

      if (!scan_kvs.empty()) {
        for (auto& scan_kv : scan_kvs) {
          CHECK(tmp_kvs.insert(std::make_pair(std::move(scan_kv.key), std::move(scan_kv.value))).second);
        }

        if (redundant_limit != 0 && (tmp_kvs.size() >= redundant_limit)) {
          break;
        }
      } else {
        DINGO_LOG(INFO) << fmt::format("txn region:{} scanner NextBatch is empty", region->RegionId());
        CHECK(!scanner->HasMore());
      }
    }

    if (redundant_limit != 0 && (tmp_kvs.size() >= redundant_limit)) {
      DINGO_LOG(INFO) << fmt::format(
          "region:{} scan finished, stop to scan between [{},{}), next_start:{}, limit:{}, redundant_limit:{}, "
          "scan_cnt:{}",
          region->RegionId(), start_key, end_key, next_start, limit, redundant_limit, tmp_kvs.size());
      break;
    } else {
      next_start = region->Range().end_key();
      DINGO_LOG(INFO) << fmt::format("region:{} scan finished, continue to scan between [{},{}), next_start:{}, ",
                                     region->RegionId(), start_key, end_key, next_start);
      continue;
    }
  }

  DINGO_LOG(INFO) << fmt::format("scan end between [{},{}), next_start:{}", start_key, end_key, next_start);

  // overwide use local buffer
  for (const auto& mutaion : range_mutations) {
    if (mutaion.type == TxnMutationType::kDelete) {
      tmp_kvs.erase(mutaion.key);
    } else if (mutaion.type == TxnMutationType::kPut) {
      tmp_kvs.insert_or_assign(mutaion.key, mutaion.value);
    } else if (mutaion.type == TxnMutationType::kPutIfAbsent) {
      auto iter = tmp_kvs.find(mutaion.key);
      if (iter == tmp_kvs.end()) {
        CHECK(tmp_kvs.insert(std::make_pair(mutaion.key, mutaion.value)).second);
      }
    } else {
      CHECK(false) << "unexpect txn mutation:" << mutaion.ToString();
    }
  }

  std::vector<KVPair> to_return;
  to_return.reserve(tmp_kvs.size());
  for (auto& pair : tmp_kvs) {
    to_return.push_back({pair.first, std::move(pair.second)});
  }
  if (limit != 0 && (to_return.size() >= limit)) {
    to_return.resize(limit);
  }

  kvs = std::move(to_return);

  return Status::OK();
}

std::unique_ptr<TxnPrewriteRpc> Transaction::TxnImpl::PrepareTxnPrewriteRpc(
    const std::shared_ptr<Region>& region) const {
  auto rpc = std::make_unique<TxnPrewriteRpc>();

  rpc->MutableRequest()->set_start_ts(start_ts_);
  FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 TransactionIsolation2IsolationLevel(options_.isolation));

  std::string pk = buffer_->GetPrimaryKey();
  rpc->MutableRequest()->set_primary_lock(pk);
  rpc->MutableRequest()->set_txn_size(buffer_->MutationsSize());

  // FIXME: set ttl
  rpc->MutableRequest()->set_lock_ttl(INT64_MAX);

  return std::move(rpc);
}

void Transaction::TxnImpl::CheckAndLogPreCommitPrimaryKeyResponse(
    const pb::store::TxnPrewriteResponse* response) const {
  std::string pk = buffer_->GetPrimaryKey();
  auto txn_result_size = response->txn_result_size();
  if (0 == txn_result_size) {
    DINGO_LOG(DEBUG) << "success pre_commit_primary_key:" << pk;
  } else if (1 == txn_result_size) {
    const auto& txn_result = response->txn_result(0);
    DINGO_LOG(INFO) << "lock or confict pre_commit_primary_key:" << pk << " txn_result:" << txn_result.DebugString();
  } else {
    DINGO_LOG(FATAL) << "unexpected pre_commit_primary_key response txn_result_size size: " << txn_result_size
                     << ", response:" << response->DebugString();
  }
}

Status Transaction::TxnImpl::TryResolveTxnPrewriteLockConflict(const pb::store::TxnPrewriteResponse* response) const {
  Status ret;
  std::string pk = buffer_->GetPrimaryKey();
  for (const auto& txn_result : response->txn_result()) {
    ret = CheckTxnResultInfo(txn_result);

    if (ret.ok()) {
      continue;
    } else if (ret.IsTxnLockConflict()) {
      Status resolve = stub_.GetTxnLockResolver()->ResolveLock(txn_result.locked(), start_ts_);
      if (!resolve.ok()) {
        DINGO_LOG(WARNING) << "fail resolve lock pk:" << pk << ", status:" << ret.ToString()
                           << " txn_result:" << txn_result.DebugString();
        ret = resolve;
      }
    } else if (ret.IsTxnWriteConflict()) {
      DINGO_LOG(WARNING) << "write conflict pk:" << pk << ", status:" << ret.ToString()
                         << " txn_result:" << txn_result.DebugString();
      return ret;
    } else {
      DINGO_LOG(WARNING) << "unexpect txn pre commit rpc response, status:" << ret.ToString()
                         << " response:" << response->DebugString();
    }
  }

  return ret;
}

Status Transaction::TxnImpl::PreCommitPrimaryKey() {
  std::string pk = buffer_->GetPrimaryKey();

  std::shared_ptr<Region> region;
  Status ret = stub_.GetMetaCache()->LookupRegionByKey(pk, region);
  if (!ret.IsOK()) {
    return ret;
  }

  std::unique_ptr<TxnPrewriteRpc> rpc = PrepareTxnPrewriteRpc(region);
  TxnMutation mutation;
  CHECK(buffer_->Get(pk, mutation).ok());
  TxnMutation2MutationPB(mutation, rpc->MutableRequest()->add_mutations());

  int retry = 0;
  while (true) {
    DINGO_RETURN_NOT_OK(LogAndSendRpc(stub_, *rpc, region));

    const auto* response = rpc->Response();
    CheckAndLogPreCommitPrimaryKeyResponse(response);

    ret = TryResolveTxnPrewriteLockConflict(response);

    if (ret.ok()) {
      break;
    } else if (ret.IsTxnWriteConflict()) {
      // no need retry
      // TODO: should we change txn state?
      DINGO_LOG(WARNING) << "write conflict, txn need abort and restart, pre_commit_primary:" << pk;
      break;
    }

    if (NeedRetryAndInc(retry)) {
      // TODO: set txn retry ms
      DINGO_LOG(INFO) << "try to delay:" << FLAGS_txn_op_delay_ms << "ms";
      DelayRetry(FLAGS_txn_op_delay_ms);
    } else {
      break;
    }
  }

  return ret;
}

void Transaction::TxnImpl::ProcessTxnPrewriteSubTask(TxnSubTask* sub_task) {
  auto* rpc = CHECK_NOTNULL(dynamic_cast<TxnPrewriteRpc*>(sub_task->rpc));
  std::string pk = buffer_->GetPrimaryKey();
  Status ret;
  int retry = 0;
  while (true) {
    ret = LogAndSendRpc(stub_, *rpc, sub_task->region);
    if (!ret.ok()) {
      break;
    }

    const auto* response = rpc->Response();
    ret = TryResolveTxnPrewriteLockConflict(response);

    if (ret.ok()) {
      break;
    } else if (ret.IsTxnWriteConflict()) {
      // no need retry
      // TODO: should we change txn state?
      DINGO_LOG(WARNING) << "write conflict, txn need abort and restart, pre_commit_primary:" << pk;
      break;
    }
    if (NeedRetryAndInc(retry)) {
      // TODO: set txn retry ms
      DINGO_LOG(INFO) << "try to delay:" << FLAGS_txn_op_delay_ms << "ms";
      DelayRetry(FLAGS_txn_op_delay_ms);
    } else {
      // TODO: maybe set ret as meaningful status
      break;
    }
  }

  sub_task->status = ret;
}

// TODO: process AlreadyExist if mutaion is PutIfAbsent
Status Transaction::TxnImpl::PreCommit() {
  state_ = kPreCommitting;

  if (buffer_->IsEmpty()) {
    state_ = kPreCommitted;
    return Status::OK();
  }

  DINGO_RETURN_NOT_OK(PreCommitPrimaryKey());

  // TODO: start heartbeat

  auto meta_cache = stub_.GetMetaCache();
  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<TxnMutation>> region_mutations;

  std::string pk = buffer_->GetPrimaryKey();
  for (const auto& mutaion_entry : buffer_->Mutations()) {
    if (mutaion_entry.first == pk) {
      continue;
    }

    std::shared_ptr<Region> tmp;
    Status got = meta_cache->LookupRegionByKey(mutaion_entry.first, tmp);
    if (!got.IsOK()) {
      return got;
    }

    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_mutations[tmp->RegionId()].push_back(mutaion_entry.second);
  }

  std::vector<TxnSubTask> sub_tasks;
  std::vector<std::unique_ptr<TxnPrewriteRpc>> rpcs;

  for (const auto& mutation_entry : region_mutations) {
    auto region_id = mutation_entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = PrepareTxnPrewriteRpc(region);

    for (const auto& mutation : mutation_entry.second) {
      TxnMutation2MutationPB(mutation, rpc->MutableRequest()->add_mutations());
    }

    sub_tasks.emplace_back(rpc.get(), region);
    rpcs.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs.size(), region_mutations.size());
  DCHECK_EQ(rpcs.size(), sub_tasks.size());

  std::vector<std::thread> thread_pool;
  thread_pool.reserve(sub_tasks.size());
  for (auto& sub_task : sub_tasks) {
    thread_pool.emplace_back(&Transaction::TxnImpl::ProcessTxnPrewriteSubTask, this, &sub_task);
  }

  for (auto& thread : thread_pool) {
    thread.join();
  }

  Status result;
  for (auto& state : sub_tasks) {
    if (!state.status.IsOK()) {
      DINGO_LOG(WARNING) << "fail txn_pre_write_sub_task, rpc: " << state.rpc->Method()
                         << " send to region: " << state.region->RegionId() << " status: " << state.status.ToString();
      if (result.ok()) {
        // only return first fail status
        result = state.status;
      }
    }
  }

  if (result.ok()) {
    state_ = kPreCommitted;
  }

  return result;
}

std::unique_ptr<TxnCommitRpc> Transaction::TxnImpl::PrepareTxnCommitRpc(const std::shared_ptr<Region>& region) const {
  auto rpc = std::make_unique<TxnCommitRpc>();
  FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 TransactionIsolation2IsolationLevel(options_.isolation));

  rpc->MutableRequest()->set_start_ts(start_ts_);
  rpc->MutableRequest()->set_commit_ts(commit_ts_);

  return std::move(rpc);
}

Status Transaction::TxnImpl::ProcessTxnCommitResponse(const pb::store::TxnCommitResponse* response,
                                                      bool is_primary) const {
  std::string pk = buffer_->GetPrimaryKey();
  DINGO_LOG(DEBUG) << "After commit txn, start_ts:" << start_ts_ << " pk:" << pk
                   << ", response:" << response->DebugString();

  if (response->has_txn_result()) {
    const auto& txn_result = response->txn_result();
    if (txn_result.has_locked()) {
      const auto& lock_info = txn_result.locked();
      DINGO_LOG(FATAL) << "internal error, txn lock confilict start_ts:" << start_ts_ << " pk:" << pk
                       << ", response:" << response->DebugString();
    }

    if (txn_result.has_txn_not_found()) {
      DINGO_LOG(FATAL) << "internal error, txn not found start_ts:" << start_ts_ << " pk:" << pk
                       << ", response:" << response->DebugString();
    }

    if (txn_result.has_write_conflict()) {
      const auto& write_conflict = txn_result.write_conflict();
      if (!is_primary) {
        DINGO_LOG(FATAL) << "internal error, txn write conlict start_ts:" << start_ts_ << " pk:" << pk
                         << ", response:" << response->DebugString();
      }
      return Status::TxnRolledBack("");
    }

    return Status::OK();
  }

  return Status::OK();
}

Status Transaction::TxnImpl::CommitPrimaryKey() {
  std::string pk = buffer_->GetPrimaryKey();
  std::shared_ptr<Region> region;
  Status ret = stub_.GetMetaCache()->LookupRegionByKey(pk, region);
  if (!ret.IsOK()) {
    return ret;
  }

  std::unique_ptr<TxnCommitRpc> rpc = PrepareTxnCommitRpc(region);
  auto* fill = rpc->MutableRequest()->add_keys();
  *fill = pk;

  DINGO_RETURN_NOT_OK(LogAndSendRpc(stub_, *rpc, region));

  const auto* response = rpc->Response();
  return ProcessTxnCommitResponse(response, true);
}

void Transaction::TxnImpl::ProcessTxnCommitSubTask(TxnSubTask* sub_task) {
  auto* rpc = CHECK_NOTNULL(dynamic_cast<TxnCommitRpc*>(sub_task->rpc));
  std::string pk = buffer_->GetPrimaryKey();
  Status ret;

  ret = LogAndSendRpc(stub_, *rpc, sub_task->region);
  if (!ret.ok()) {
    sub_task->status = ret;
    return;
  }

  const auto* response = rpc->Response();
  ret = ProcessTxnCommitResponse(response, true);
  sub_task->status = ret;
}

Status Transaction::TxnImpl::Commit() {
  if (state_ != kPreCommitted) {
    return Status::IllegalState(fmt::format("forbid commit, txn state is:{}, expect:{}", TransactionState2Str(state_),
                                            TransactionState2Str(kPreCommitted)));
  }

  if (buffer_->IsEmpty()) {
    state_ = kCommitted;
    return Status::OK();
  }

  state_ = kCommitting;

  pb::meta::TsoTimestamp tso;
  DINGO_RETURN_NOT_OK(stub_.GetAdminTool()->GetCurrentTsoTimeStamp(tso));
  commit_tso_ = tso;
  commit_ts_ = Tso2Timestamp(commit_tso_);
  CHECK(commit_ts_ > start_ts_) << "commit_ts:" << commit_ts_ << " must greater than start_ts:" << start_ts_
                                << ", commit_tso:" << commit_tso_.DebugString()
                                << ", start_tso:" << start_tso_.DebugString();
  // TODO: if commit primary key and find txn is rolled back, should we rollback all the mutation?
  Status ret = CommitPrimaryKey();
  if (!ret.ok()) {
    if (ret.IsTxnRolledBack()) {
      state_ = kRollbackted;
    } else {
      DINGO_LOG(INFO) << "unexpect commit primary key status:" << ret.ToString();
    }
  } else {
    state_ = kCommitted;

    {
      // we commit primary key is success, and then we try best to commit other keys, if fail we ignore
      auto meta_cache = stub_.GetMetaCache();
      std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
      std::unordered_map<int64_t, std::vector<std::string>> region_commit_keys;

      std::string pk = buffer_->GetPrimaryKey();
      for (const auto& mutaion_entry : buffer_->Mutations()) {
        if (mutaion_entry.first == pk) {
          continue;
        }

        std::shared_ptr<Region> tmp;
        Status got = meta_cache->LookupRegionByKey(mutaion_entry.first, tmp);
        if (!got.IsOK()) {
          continue;
        }

        auto iter = region_id_to_region.find(tmp->RegionId());
        if (iter == region_id_to_region.end()) {
          region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
        }

        region_commit_keys[tmp->RegionId()].push_back(mutaion_entry.second.key);
      }

      std::vector<TxnSubTask> sub_tasks;
      std::vector<std::unique_ptr<TxnCommitRpc>> rpcs;
      for (const auto& entry : region_commit_keys) {
        auto region_id = entry.first;
        auto iter = region_id_to_region.find(region_id);
        CHECK(iter != region_id_to_region.end());
        auto region = iter->second;

        std::unique_ptr<TxnCommitRpc> rpc = PrepareTxnCommitRpc(region);
        for (const auto& key : entry.second) {
          auto* fill = rpc->MutableRequest()->add_keys();
          *fill = key;
        }
        sub_tasks.emplace_back(rpc.get(), region);
        rpcs.push_back(std::move(rpc));
      }

      DCHECK_EQ(rpcs.size(), region_commit_keys.size());
      DCHECK_EQ(rpcs.size(), sub_tasks.size());

      std::vector<std::thread> thread_pool;
      thread_pool.reserve(sub_tasks.size());
      for (auto& sub_task : sub_tasks) {
        thread_pool.emplace_back(&Transaction::TxnImpl::ProcessTxnCommitSubTask, this, &sub_task);
      }

      for (auto& thread : thread_pool) {
        thread.join();
      }

      for (auto& state : sub_tasks) {
        // ignore
        if (!state.status.IsOK()) {
          DINGO_LOG(INFO) << "Fail txn_commit_sub_task but ignore, rpc: " << state.rpc->Method()
                          << " send to region: " << state.region->RegionId() << " status: " << state.status.ToString();
        }
      }
    }
  }

  return ret;
}

std::unique_ptr<TxnBatchRollbackRpc> Transaction::TxnImpl::PrepareTxnBatchRollbackRpc(
    const std::shared_ptr<Region>& region) const {
  auto rpc = std::make_unique<TxnBatchRollbackRpc>();
  FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 TransactionIsolation2IsolationLevel(options_.isolation));
  rpc->MutableRequest()->set_start_ts(start_ts_);
  return std::move(rpc);
}

void Transaction::TxnImpl::CheckAndLogTxnBatchRollbackResponse(
    const pb::store::TxnBatchRollbackResponse* response) const {
  if (response->has_txn_result()) {
    std::string pk = buffer_->GetPrimaryKey();
    const auto& txn_result = response->txn_result();
    DINGO_LOG(WARNING) << "Fail rollback txn, start_ts:" << start_ts_ << " pk:" << pk
                       << " txn_result:" << txn_result.DebugString();
  }
}

void Transaction::TxnImpl::ProcessBatchRollbackSubTask(TxnSubTask* sub_task) {
  auto* rpc = CHECK_NOTNULL(dynamic_cast<TxnBatchRollbackRpc*>(sub_task->rpc));
  std::string pk = buffer_->GetPrimaryKey();
  Status ret;

  ret = LogAndSendRpc(stub_, *rpc, sub_task->region);
  if (!ret.ok()) {
    sub_task->status = ret;
    return;
  }

  const auto* response = rpc->Response();
  CheckAndLogTxnBatchRollbackResponse(response);
  if (response->has_txn_result()) {
    const auto& txn_result = response->txn_result();
    if (txn_result.has_locked()) {
      sub_task->status = Status::TxnLockConflict("");
      return;
    }
  }

  sub_task->status = Status::OK();
}

Status Transaction::TxnImpl::Rollback() {
  // TODO: client txn status maybe inconsistence with server
  // so we should check txn status first and then take action
  // TODO: maybe support rollback when txn is active
  if (state_ != kRollbacking && state_ != kPreCommitting && state_ != kPreCommitted) {
    return Status::IllegalState(fmt::format("forbid rollback, txn state is:{}", TransactionState2Str(state_)));
  }

  state_ = kRollbacking;
  {
    // rollback primary key
    std::string pk = buffer_->GetPrimaryKey();
    std::shared_ptr<Region> region;
    Status ret = stub_.GetMetaCache()->LookupRegionByKey(pk, region);
    if (!ret.IsOK()) {
      return ret;
    }

    std::unique_ptr<TxnBatchRollbackRpc> rpc = PrepareTxnBatchRollbackRpc(region);
    auto* fill = rpc->MutableRequest()->add_keys();
    *fill = pk;

    DINGO_RETURN_NOT_OK(LogAndSendRpc(stub_, *rpc, region));

    const auto* response = rpc->Response();
    CheckAndLogTxnBatchRollbackResponse(response);
    if (response->has_txn_result()) {
      // TODO: which state should we transfer to ?
      const auto& txn_result = response->txn_result();
      if (txn_result.has_locked()) {
        return Status::TxnLockConflict(txn_result.locked().DebugString());
      }
    }
  }
  state_ = kRollbackted;

  {
    // we rollback primary key is success, and then we try best to rollback other keys, if fail we ignore
    auto meta_cache = stub_.GetMetaCache();
    std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
    std::unordered_map<int64_t, std::vector<std::string>> region_rollback_keys;

    std::string pk = buffer_->GetPrimaryKey();
    for (const auto& mutaion_entry : buffer_->Mutations()) {
      if (mutaion_entry.first == pk) {
        continue;
      }

      std::shared_ptr<Region> tmp;
      Status got = meta_cache->LookupRegionByKey(mutaion_entry.first, tmp);
      if (!got.IsOK()) {
        continue;
      }

      auto iter = region_id_to_region.find(tmp->RegionId());
      if (iter == region_id_to_region.end()) {
        region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
      }

      region_rollback_keys[tmp->RegionId()].push_back(mutaion_entry.second.key);
    }

    std::vector<TxnSubTask> sub_tasks;
    std::vector<std::unique_ptr<TxnBatchRollbackRpc>> rpcs;
    for (const auto& entry : region_rollback_keys) {
      auto region_id = entry.first;
      auto iter = region_id_to_region.find(region_id);
      CHECK(iter != region_id_to_region.end());
      auto region = iter->second;

      std::unique_ptr<TxnBatchRollbackRpc> rpc = PrepareTxnBatchRollbackRpc(region);
      for (const auto& key : entry.second) {
        auto* fill = rpc->MutableRequest()->add_keys();
        *fill = key;
      }
      sub_tasks.emplace_back(rpc.get(), region);
      rpcs.push_back(std::move(rpc));
    }

    DCHECK_EQ(rpcs.size(), region_rollback_keys.size());
    DCHECK_EQ(rpcs.size(), sub_tasks.size());

    std::vector<std::thread> thread_pool;
    for (auto i = 1; i < sub_tasks.size(); i++) {
      thread_pool.emplace_back(&Transaction::TxnImpl::ProcessBatchRollbackSubTask, this, &sub_tasks[i]);
    }

    ProcessBatchRollbackSubTask(sub_tasks.data());

    for (auto& thread : thread_pool) {
      thread.join();
    }

    for (auto& state : sub_tasks) {
      // ignore
      if (!state.status.IsOK()) {
        DINGO_LOG(INFO) << "Fail txn_batch_rollback_sub_task, but ignore, rpc: " << state.rpc->Method()
                        << " send to region: " << state.region->RegionId() << " status: " << state.status.ToString();
      }
    }
  }

  return Status::OK();
}

bool Transaction::TxnImpl::NeedRetryAndInc(int& times) {
  bool retry = times < FLAGS_txn_op_max_retry;
  times++;
  return retry;
}

void Transaction::TxnImpl::DelayRetry(int64_t delay_ms) { (void)usleep(delay_ms); }

}  // namespace sdk
}  // namespace dingodb