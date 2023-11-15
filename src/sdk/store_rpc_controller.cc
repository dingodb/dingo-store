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
#include "sdk/store_rpc_controller.h"

#include <cstddef>
#include <string>
#include <utility>

#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "sdk/client_stub.h"
#include "sdk/common.h"
#include "sdk/meta_cache.h"
#include "sdk/param_config.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

using google::protobuf::DynamicCastToGenerated;

StoreRpcController::StoreRpcController(const ClientStub& stub, Rpc& rpc, std::shared_ptr<Region> region)
    : stub_(stub), rpc_(rpc), region_(std::move(region)), rpc_retry_times_(0), next_replica_index_(0) {}

StoreRpcController::~StoreRpcController() = default;

// TODO: refact
Status StoreRpcController::Call(google::protobuf::Closure* done) {
  CHECK(region_.get() != nullptr) << "region should not nullptr, please check";

  do {
    if (region_->IsStale()) {
      std::string msg = fmt::format("region:{} is stale", region_->RegionId());
      DINGO_LOG(INFO) << "store rpc fail, " << msg;
      return Status::Incomplete(msg);
    }

    butil::EndPoint next_leader;
    if (!PickNextLeader(next_leader)) {
      std::string msg = fmt::format("rpc:{} no valid endpoint, region:{}", rpc_.Method(), region_->RegionId());
      return Status::Aborted(msg);
    }

    CHECK(next_leader.ip != butil::IP_ANY);
    CHECK(next_leader.port != 0);
    rpc_.SetEndPoint(next_leader);

    Status sent = PrepareAndSendRpc(done);

    if (!sent.IsOK()) {
      SetFailed(rpc_.GetEndPoint());
      rpc_retry_times_++;
      DINGO_LOG(WARNING) << "store rpc send fail, " << sent.ToString()
                         << " endpoint:" << butil::endpoint2str(rpc_.GetEndPoint()).c_str()
                         << " region:" << region_->RegionId();
      continue;
    }

    CHECK(sent.IsOK());

    const brpc::Controller* cntl = rpc_.Controller();
    if (IsRpcFailed(cntl)) {
      SetFailed(rpc_.GetEndPoint());
      rpc_retry_times_++;

      std::string msg = fmt::format(
          "log_id:{} connect with store server fail. region:{} method:{} endpoint:{}, error_text:{}", cntl->log_id(),
          region_->RegionId(), rpc_.Method(), butil::endpoint2str(cntl->remote_side()).c_str(), cntl->ErrorText());

      DINGO_LOG(WARNING) << msg;
      continue;
    } else {
      auto error = GetResponseError(rpc_);
      if (error.errcode() == pb::error::Errno::OK) {
        return Status::OK();
      } else if (error.errcode() == pb::error::Errno::ERAFT_NOTLEADER) {
        region_->MarkFollower(rpc_.GetEndPoint());
        rpc_retry_times_++;

        if (error.has_leader_location()) {
          auto endpoint = Helper::LocationToEndPoint(error.leader_location());
          if (endpoint.ip == butil::IP_ANY || endpoint.port == 0) {
            std::string msg =
                fmt::format("log_id:{} region:{} method:{} endpoint:{} not leader, but leader hint:{} is invalid",
                            cntl->log_id(), region_->RegionId(), rpc_.Method(),
                            butil::endpoint2str(cntl->remote_side()).c_str(), butil::endpoint2str(endpoint).c_str());
            DINGO_LOG(WARNING) << msg;
          } else {
            region_->MarkLeader(endpoint);

            std::string msg =
                fmt::format("log_id:{} region:{} method:{} endpoint:{} not leader, leader hint:{}", cntl->log_id(),
                            region_->RegionId(), rpc_.Method(), butil::endpoint2str(cntl->remote_side()).c_str(),
                            butil::endpoint2str(endpoint).c_str());

            DINGO_LOG(WARNING) << msg;
          }
        } else {
          std::string msg =
              fmt::format("log_id:{} region:{} method:{} endpoint:{}, not leader", cntl->log_id(), region_->RegionId(),
                          rpc_.Method(), butil::endpoint2str(cntl->remote_side()).c_str());

          DINGO_LOG(WARNING) << msg;
        }

        continue;
      } else if (error.errcode() == pb::error::EREGION_VERSION) {
        stub_.GetMetaCache()->ClearRange(region_);

        std::string error_msg =
            fmt::format("error:{}, {}", dingodb::pb::error::Errno_Name(error.errcode()), error.errmsg());

        if (error.has_store_region_info()) {
          auto region = ProcessStoreRegionInfo(error.store_region_info());
          stub_.GetMetaCache()->MaybeAddRegion(region);

          std::string msg = fmt::format("log_id:{} region:{} method:{} endpoint:{}, recive new version region:{}",
                                        cntl->log_id(), region_->RegionId(), rpc_.Method(),
                                        butil::endpoint2str(cntl->remote_side()).c_str(), region->ToString());

          DINGO_LOG(WARNING) << msg << ", " << error_msg;
        } else {
          std::string msg =
              fmt::format("log_id:{} region:{} method:{} endpoint:{}", cntl->log_id(), region_->RegionId(),
                          rpc_.Method(), butil::endpoint2str(cntl->remote_side()).c_str());

          DINGO_LOG(WARNING) << msg << ", " << error_msg;
        }

        return Status::Incomplete(error_msg);
      } else if (error.errcode() == pb::error::Errno::EREGION_NOT_FOUND) {
        stub_.GetMetaCache()->ClearRange(region_);

        std::string error_msg =
            fmt::format("error:{}, {}", dingodb::pb::error::Errno_Name(error.errcode()), error.errmsg());

        std::string msg = fmt::format("log_id:{} region:{} method:{} endpoint:{}", cntl->log_id(), region_->RegionId(),
                                      rpc_.Method(), butil::endpoint2str(cntl->remote_side()).c_str());

        DINGO_LOG(WARNING) << msg << ", " << error_msg;

        return Status::Incomplete(error_msg);
      } else if (error.errcode() == pb::error::Errno::EKEY_OUT_OF_RANGE) {
        stub_.GetMetaCache()->ClearRange(region_);

        std::string error_msg =
            fmt::format("error:{}, {}", dingodb::pb::error::Errno_Name(error.errcode()), error.errmsg());

        std::string msg = fmt::format("log_id:{} region:{} method:{} endpoint:{}", cntl->log_id(), region_->RegionId(),
                                      rpc_.Method(), butil::endpoint2str(cntl->remote_side()).c_str());

        DINGO_LOG(WARNING) << msg << ", " << error_msg;

        return Status::Incomplete(error_msg);
      } else {
        // NOTE: other error we not clean cache, caller decide how to process
        std::string error_msg =
            fmt::format("error:{}, {}", dingodb::pb::error::Errno_Name(error.errcode()), error.errmsg());

        std::string msg = fmt::format("log_id:{} region:{} method:{} endpoint:{} ", cntl->log_id(), region_->RegionId(),
                                      rpc_.Method(), butil::endpoint2str(cntl->remote_side()).c_str());

        DINGO_LOG(WARNING) << msg << ", " << error_msg;

        return Status::Incomplete(error_msg);
      }
    }
  } while (NeedRetry());

  std::string msg = fmt::format("region:{} retry times:{} exceed RpcMaxRetry:{}", region_->RegionId(), rpc_retry_times_,
                                kRpcMaxRetry);
  DINGO_LOG(INFO) << "store rpc fail, " << msg;

  return Status::Aborted(msg);
}

Status StoreRpcController::PrepareAndSendRpc(google::protobuf::Closure* done) {
  rpc_.RawMutableResponse()->Clear();

  rpc_.MutableController()->Reset();
  rpc_.MutableController()->set_log_id(butil::fast_rand());
  rpc_.MutableController()->set_timeout_ms(kRpcTimeOutMs);
  rpc_.MutableController()->set_max_retry(kRpcCallMaxRetry);

  return stub_.GetStoreRpcInteraction()->SendRpc(rpc_, done);
}

bool StoreRpcController::IsRpcFailed(const brpc::Controller* cntl) { return cntl->Failed(); }

bool StoreRpcController::PickNextLeader(butil::EndPoint& leader) {
  butil::EndPoint tmp_leader;
  Status got = region_->GetLeader(tmp_leader);
  if (got.IsOK()) {
    if (Failed(tmp_leader)) {
      std::string msg = fmt::format("region:{} get leader success, but leader endpoint is already failed",
                                    region_->RegionId(), butil::endpoint2str(tmp_leader).c_str());
      DINGO_LOG(INFO) << msg;
    } else {
      leader = tmp_leader;
      return true;
    }
  }

  // TODO: filter old leader
  auto endpoints = region_->ReplicaEndPoint();
  int before = next_replica_index_ % endpoints.size();
  int current = before;
  do {
    auto endpoint = endpoints[current];
    next_replica_index_++;

    if (!Failed(endpoint)) {
      leader = endpoint;
      std::string msg = fmt::format("region:{} get leader fail, pick replica:{} as leader", region_->RegionId(),
                                    butil::endpoint2str(endpoint).c_str());
      DINGO_LOG(INFO) << msg;
      return true;
    }

    current = next_replica_index_ % endpoints.size();
  } while (current != before);

  DINGO_LOG(WARNING) << "region:" << region_->RegionId() << " pick leader fail, all replica is set failed";
  return false;
}

void StoreRpcController::ResetRegion(std::shared_ptr<Region> region) {
  CHECK(EpochCompare(region_->Epoch(), region->Epoch()) > 0)
      << "reset region:" << region->ToString() << " expect newer than old region: " << region_->ToString();
  region_.reset();
  region_ = std::move(region);
}

bool StoreRpcController::Failed(const butil::EndPoint& addr) { return failed_addrs_.find(addr) != failed_addrs_.end(); }

void StoreRpcController::SetFailed(butil::EndPoint addr) {
  failed_addrs_.emplace(std::move(addr));

  std::string msg =
      fmt::format("region:{} replica:{} is set failed", region_->RegionId(), butil::endpoint2str(addr).c_str());
  DINGO_LOG(INFO) << msg;
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
    auto end_point = Helper::LocationToEndPoint(peer.server_location());
    CHECK(end_point.ip != butil::IP_ANY) << "ip should not any, end_point:" << butil::endpoint2str(end_point);
    CHECK(end_point.port != 0) << "port should not 0 end_point:" << butil::endpoint2str(end_point);
    replicas.push_back({end_point, kFollower});
  }

  std::shared_ptr<Region> region = std::make_shared<Region>(
      id, store_region_info.current_range(), store_region_info.current_region_epoch(), region_->RegionType(), replicas);

  butil::EndPoint leader;
  if (region_->GetLeader(leader).IsOK()) {
    region->MarkLeader(leader);
  }

  return region;
}

bool StoreRpcController::NeedRetry() const { return this->rpc_retry_times_ < kRpcMaxRetry; }

const pb::error::Error& StoreRpcController::GetResponseError(Rpc& rpc) {
  const auto* response = rpc.RawResponse();
  const auto* descriptor = response->GetDescriptor();
  const auto* reflection = response->GetReflection();

  const auto* error_field = descriptor->FindFieldByName("error");
  CHECK(error_field) << "no error field";

  auto* msg = reflection->MutableMessage(rpc.RawMutableResponse(), error_field);
  CHECK(msg) << "get error mutable message fail";

  auto* error = DynamicCastToGenerated<pb::error::Error>(msg);
  CHECK(error) << "dynamic cast msg to error fail";
  return *error;
}

}  // namespace sdk
}  // namespace dingodb