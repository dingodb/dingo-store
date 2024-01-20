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

#include "sdk/transaction/txn_lock_resolver.h"

#include <cstdint>

#include "common/logging.h"
#include "glog/logging.h"
#include "proto/store.pb.h"
#include "sdk/client_stub.h"
#include "sdk/common/common.h"
#include "sdk/region.h"
#include "sdk/status.h"
#include "sdk/store/store_rpc.h"
#include "sdk/store/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

TxnLockResolver::TxnLockResolver(const ClientStub& stub) : stub_(stub) {}

// TODO: maybe support retry
Status TxnLockResolver::ResolveLock(const pb::store::LockInfo& lock_info, int64_t caller_start_ts) {
  DINGO_LOG(DEBUG) << "lock_info:" << lock_info.DebugString();
  TxnStatus txn_status;
  Status ret = CheckTxnStatus(lock_info.lock_ts(), lock_info.primary_lock(), caller_start_ts, txn_status);
  if (!ret.ok()) {
    if (ret.IsNotFound()) {
      DINGO_LOG(DEBUG) << "txn not exist when check txn status, status:" << ret.ToString()
                       << ", lock_info:" << lock_info.DebugString();
      return Status::OK();
    } else {
      return ret;
    }
  }

  if (txn_status.IsLocked()) {
    return Status::TxnLockConflict(ret.ToString());
  }

  CHECK(txn_status.IsCommitted() || txn_status.IsRollbacked()) << "unexpected txn_status:" << txn_status.ToString();

  // resolve primary key
  ret = ResolveLockKey(lock_info.lock_ts(), lock_info.primary_lock(), txn_status.commit_ts);
  if (!ret.IsOK()) {
    DINGO_LOG(WARNING) << "resolve txn:" << lock_info.lock_ts() << " primary_key:" << lock_info.primary_lock()
                       << " txn_status:" << txn_status.ToString() << " fail, status:" << ret.ToString();
    return ret;
  }

  // resolve conflict key
  ret = ResolveLockKey(lock_info.lock_ts(), lock_info.key(), txn_status.commit_ts);
  if (!ret.IsOK()) {
    DINGO_LOG(WARNING) << "resolve txn:" << lock_info.lock_ts() << " key:" << lock_info.key()
                       << " txn_status:" << txn_status.ToString() << " fail, status:" << ret.ToString();
    return ret;
  }

  return Status::OK();
}

// TODO: use txn status cache
Status TxnLockResolver::CheckTxnStatus(int64_t txn_start_ts, const std::string& txn_primary_key,
                                       int64_t caller_start_ts, TxnStatus& txn_status) {
  std::shared_ptr<Region> region;
  DINGO_RETURN_NOT_OK(stub_.GetMetaCache()->LookupRegionByKey(txn_primary_key, region));

  int64_t current_ts;
  DINGO_RETURN_NOT_OK(stub_.GetAdminTool()->GetCurrentTimeStamp(current_ts));

  TxnCheckTxnStatusRpc rpc;

  // NOTE: use randome isolation is ok?
  FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 pb::store::IsolationLevel::SnapshotIsolation);
  rpc.MutableRequest()->set_primary_key(txn_primary_key);
  rpc.MutableRequest()->set_lock_ts(txn_start_ts);
  rpc.MutableRequest()->set_caller_start_ts(caller_start_ts);
  rpc.MutableRequest()->set_current_ts(current_ts);

  StoreRpcController controller(stub_, rpc, region);
  DINGO_RETURN_NOT_OK(controller.Call());

  const auto& response = *rpc.Response();
  return ProcessTxnCheckStatusResponse(response, txn_status);
}

Status TxnLockResolver::ProcessTxnCheckStatusResponse(const pb::store::TxnCheckTxnStatusResponse& response,
                                                      TxnStatus& txn_status) {
  if (response.has_txn_result()) {
    const auto& txn_result = response.txn_result();
    if (txn_result.has_txn_not_found()) {
      DINGO_LOG(INFO) << "NotFound txn, response" << response.DebugString();
      const auto& not_found = txn_result.txn_not_found();
      return Status::NotFound(fmt::format("start_ts:{},primary_key:{},key:{}", not_found.start_ts(),
                                          not_found.primary_key(), not_found.key()));
    } else if (txn_result.has_primary_mismatch()) {
      DINGO_LOG(ERROR) << "Mismatch txn primary key, response" << response.DebugString();
      return Status::IllegalState("");
    } else {
      DINGO_LOG(DEBUG) << "Ignore txn check status response:" << response.DebugString();
    }
  }

  txn_status = TxnStatus(response.lock_ttl(), response.commit_ts());
  return Status::OK();
}

Status TxnLockResolver::ResolveLockKey(int64_t txn_start_ts, const std::string& key, int64_t commit_ts) {
  std::shared_ptr<Region> region;
  Status ret = stub_.GetMetaCache()->LookupRegionByKey(key, region);
  if (!ret.IsOK()) {
    return ret;
  }

  TxnResolveLockRpc rpc;
  // NOTE: use randome isolation is ok?
  FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 pb::store::IsolationLevel::SnapshotIsolation);
  rpc.MutableRequest()->set_start_ts(txn_start_ts);
  rpc.MutableRequest()->set_commit_ts(commit_ts);
  auto* fill = rpc.MutableRequest()->add_keys();
  *fill = key;

  StoreRpcController controller(stub_, rpc, region);
  DINGO_RETURN_NOT_OK(controller.Call());

  const auto& response = *rpc.Response();
  return ProcessTxnResolveLockResponse(response);
}

Status TxnLockResolver::ProcessTxnResolveLockResponse(const pb::store::TxnResolveLockResponse& response) {
  // TODO: need to process lockinfo when support permissive txn
  DINGO_LOG(INFO) << "txn_resolve_lock_response:" << response.DebugString();
  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb