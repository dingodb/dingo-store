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
#include "sdk/rpc/store_rpc_controller.h"

#include <string>
#include <utility>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "sdk/client_stub.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/port/common.pb.h"
#include "sdk/status.h"
#include "sdk/utils/async_util.h"

namespace dingodb {
namespace sdk {

StoreRpcController::StoreRpcController(const ClientStub& stub, Rpc& rpc, std::shared_ptr<Region> region)
    : stub_(stub), rpc_(rpc), region_(std::move(region)), rpc_retry_times_(0), next_replica_index_(0) {}

StoreRpcController::StoreRpcController(const ClientStub& stub, Rpc& rpc)
    : stub_(stub), rpc_(rpc), region_(nullptr), rpc_retry_times_(0), next_replica_index_(0) {}

StoreRpcController::~StoreRpcController() = default;

Status StoreRpcController::Call() {
  Status ret;
  Synchronizer sync;
  AsyncCall(sync.AsStatusCallBack(ret));
  sync.Wait();

  return ret;
}

void StoreRpcController::AsyncCall(StatusCallback cb) {
  call_back_.swap(cb);
  DoAsyncCall();
}

void StoreRpcController::DoAsyncCall() {
  if (!PreCheck()) {
    FireCallback();
    return;
  }

  if (!PrepareRpc()) {
    FireCallback();
    return;
  }

  SendStoreRpc();
}

bool StoreRpcController::PreCheck() {
  if (region_->IsStale()) {
    std::string msg = fmt::format("region:{} is stale", region_->RegionId());
    DINGO_LOG(INFO) << "store rpc fail, " << msg;
    status_ = Status::Incomplete(msg);
    return false;
  }
  return true;
}

bool StoreRpcController::PrepareRpc() {
  if (NeedPickLeader()) {
    EndPoint next_leader;
    if (!PickNextLeader(next_leader)) {
      std::string msg = fmt::format("rpc:{} no valid endpoint, region:{}", rpc_.Method(), region_->RegionId());
      status_ = Status::Aborted(msg);
      return false;
    }

    CHECK(next_leader.IsValid());
    rpc_.SetEndPoint(next_leader);
  }

  rpc_.Reset();

  return true;
}

void StoreRpcController::SendStoreRpc() {
  CHECK(region_.get() != nullptr) << "region should not nullptr, please check";
  MaybeDelay();
  stub_.GetStoreRpcClient()->SendRpc(rpc_, [this] { SendStoreRpcCallBack(); });
}

void StoreRpcController::MaybeDelay() {
  if (NeedDelay()) {
    auto delay = FLAGS_store_rpc_retry_delay_ms * rpc_retry_times_;
    DINGO_LOG(INFO) << "try to delay:" << delay << "ms, rpr_retry_times:" << rpc_retry_times_;
    (void)usleep(delay*1000);
  }
}

void StoreRpcController::SendStoreRpcCallBack() {
  Status sent = rpc_.GetStatus();
  if (!sent.ok()) {
    region_->MarkFollower(rpc_.GetEndPoint());
    DINGO_LOG(WARNING) << "Fail connect to store server, status:" << sent.ToString();
    status_ = sent;
  } else {
    auto error = GetRpcResponseError(rpc_);
    if (error.errcode() == pb::error::Errno::OK) {
      status_ = Status::OK();
    } else {
      std::string base_msg =
          fmt::format("log_id:{} region:{} method:{} endpoint:{}, error_code:{}, error_msg:{}", rpc_.LogId(),
                      region_->RegionId(), rpc_.Method(), rpc_.GetEndPoint().ToString(),
                      dingodb::pb::error::Errno_Name(error.errcode()), error.errmsg());

      if (error.errcode() == pb::error::Errno::ERAFT_NOTLEADER) {
        region_->MarkFollower(rpc_.GetEndPoint());
        if (error.has_leader_location()) {
          auto endpoint = LocationToEndPoint(error.leader_location());
          if (!endpoint.IsValid()) {
            DINGO_LOG(WARNING) << base_msg << " not leader, but leader hint:" << error.leader_location().DebugString()
                               << ", endpoint: " << endpoint.ToString() << " is invalid";
            status_ = Status::NoLeader(error.errcode(), error.errmsg());
          } else {
            region_->MarkLeader(endpoint);
            DINGO_LOG(WARNING) << base_msg << " not leader, leader hint:" << endpoint.ToString();
            status_ = Status::NotLeader(error.errcode(), error.errmsg());
          }
        } else {
          DINGO_LOG(WARNING) << base_msg << " not leader, no leader hint";
          status_ = Status::NoLeader(error.errcode(), error.errmsg());
        }
      } else if (error.errcode() == pb::error::EREGION_VERSION) {
        stub_.GetMetaCache()->ClearRange(region_);
        if (error.has_store_region_info()) {
          auto region = ProcessStoreRegionInfo(error.store_region_info());
          stub_.GetMetaCache()->MaybeAddRegion(region);
          DINGO_LOG(WARNING) << base_msg << ", recive new version region:" << region->ToString();
        } else {
          DINGO_LOG(WARNING) << base_msg;
        }
        status_ = Status::Incomplete(error.errcode(), error.errmsg());
      } else if (error.errcode() == pb::error::Errno::EREGION_NOT_FOUND) {
        stub_.GetMetaCache()->ClearRange(region_);
        status_ = Status::Incomplete(error.errcode(), error.errmsg());
        DINGO_LOG(WARNING) << base_msg;
      } else if (error.errcode() == pb::error::Errno::EKEY_OUT_OF_RANGE) {
        stub_.GetMetaCache()->ClearRange(region_);
        status_ = Status::Incomplete(error.errcode(), error.errmsg());
        DINGO_LOG(WARNING) << base_msg;
      } else if (error.errcode() == pb::error::Errno::EREQUEST_FULL) {
        status_ = Status::RemoteError(error.errcode(), error.errmsg());
        DINGO_LOG(WARNING) << base_msg;
      } else {
        // NOTE: other error we not clean cache, caller decide how to process
        status_ = Status::Incomplete(error.errcode(), error.errmsg());
        DINGO_LOG(WARNING) << base_msg;
      }
    }
  }

  RetrySendRpcOrFireCallback();
}

void StoreRpcController::RetrySendRpcOrFireCallback() {
  if (status_.IsOK()) {
    FireCallback();
    return;
  }

  if (status_.IsNetworkError() || status_.IsRemoteError() || status_.IsNotLeader() || status_.IsNoLeader()) {
    if (NeedRetry()) {
      rpc_retry_times_++;
      DoAsyncCall();
    } else {
      status_ = Status::Aborted("rpc retry times exceed");
      FireCallback();
      return;
    }
  } else {
    FireCallback();
  }
}

void StoreRpcController::FireCallback() {
  if (!status_.ok()) {
    DINGO_LOG(WARNING) << "Fail send store rpc status:" << status_.ToString() << ", region:" << region_->RegionId()
                       << ", retry_times:" << rpc_retry_times_ << ", max_retry_limit:" << FLAGS_store_rpc_max_retry;
  }

  if (call_back_) {
    StatusCallback cb;
    call_back_.swap(cb);
    cb(status_);
  }
}

bool StoreRpcController::PickNextLeader(EndPoint& leader) {
  EndPoint tmp_leader;
  Status got = region_->GetLeader(tmp_leader);
  if (got.IsOK()) {
    leader = tmp_leader;
    return true;
  }

  // TODO: filter old leader
  auto endpoints = region_->ReplicaEndPoint();
  auto endpoint = endpoints[next_replica_index_ % endpoints.size()];
  next_replica_index_++;
  leader = endpoint;
  std::string msg =
      fmt::format("region:{} get leader fail, pick replica:{} as leader", region_->RegionId(), endpoint.ToString());
  DINGO_LOG(INFO) << msg;
  return true;
}

void StoreRpcController::ResetRegion(std::shared_ptr<Region> region) {
  if (region_) {
    if (!(EpochCompare(region_->Epoch(), region->Epoch()) > 0)) {
      DINGO_LOG(WARNING) << "reset region:" << region->ToString()
                         << " expect newer than old region: " << region_->ToString();
    }
  }
  region_.reset();
  region_ = std::move(region);
}

std::shared_ptr<Region> StoreRpcController::ProcessStoreRegionInfo(
    const dingodb::pb::error::StoreRegionInfo& store_region_info) {
  CHECK_NOTNULL(region_);
  CHECK(store_region_info.has_current_region_epoch());
  CHECK(store_region_info.has_current_range());
  auto id = store_region_info.region_id();
  CHECK(id == region_->RegionId());

  std::vector<Replica> replicas;
  for (const auto& peer : store_region_info.peers()) {
    CHECK(peer.has_server_location());
    auto end_point = LocationToEndPoint(peer.server_location());
    CHECK(end_point.IsValid()) << "end_point should valid, end_point:" << end_point.ToString()
                               << " peer server_location:" << peer.DebugString();
    replicas.push_back({end_point, kFollower});
  }

  std::shared_ptr<Region> region = std::make_shared<Region>(
      id, store_region_info.current_range(), store_region_info.current_region_epoch(), region_->RegionType(), replicas);

  EndPoint leader;
  if (region_->GetLeader(leader).IsOK()) {
    region->MarkLeader(leader);
  }

  return region;
}

bool StoreRpcController::NeedRetry() const { return this->rpc_retry_times_ < FLAGS_store_rpc_max_retry; }

bool StoreRpcController::NeedDelay() const { return status_.IsRemoteError() || status_.IsNoLeader(); }

bool StoreRpcController::NeedPickLeader() const { return !status_.IsRemoteError(); }

}  // namespace sdk
}  // namespace dingodb