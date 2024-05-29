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

#include "sdk/rpc/coordinator_rpc_controller.h"

#include <utility>

#include "sdk/client_stub.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/status.h"
#include "sdk/utils/async_util.h"
#include "sdk/utils/net_util.h"

namespace dingodb {
namespace sdk {

Status CoordinatorRpcController::Open(const std::vector<EndPoint>& endpoints) {
  if (endpoints.empty()) {
    return Status::InvalidArgument("endpoints is empty");
  };

  meta_member_info_.SetMembers(endpoints);

  return Status::OK();
}

Status CoordinatorRpcController::SyncCall(Rpc& rpc) {
  Status ret;

  Synchronizer sync;
  AsyncCall(rpc, sync.AsStatusCallBack(ret));
  sync.Wait();

  return ret;
}

void CoordinatorRpcController::AsyncCall(Rpc& rpc, StatusCallback cb) {
  rpc.call_back = std::move(cb);
  DoAsyncCall(rpc);
}

void CoordinatorRpcController::DoAsyncCall(Rpc& rpc) {
  PrepareRpc(rpc);
  SendCoordinatorRpc(rpc);
}

static bool NeedPickLeader(Rpc& rpc) { return !rpc.GetStatus().IsRemoteError(); }

void CoordinatorRpcController::PrepareRpc(Rpc& rpc) {
  if (NeedPickLeader(rpc)) {
    EndPoint next_leader = meta_member_info_.PickNextLeader();

    CHECK(next_leader.IsValid());
    rpc.SetEndPoint(next_leader);
  }

  rpc.Reset();
}

static bool NeedDelay(Rpc& rpc) { return rpc.GetStatus().IsRemoteError(); }

void CoordinatorRpcController::SendCoordinatorRpc(Rpc& rpc) {
  // TODO: what error should be delay
  if (NeedDelay(rpc)) {
    DINGO_LOG(INFO) << "try to delay:" << FLAGS_coordinator_interaction_delay_ms << "ms";
    (void)usleep(FLAGS_coordinator_interaction_delay_ms * 1000);
  }

  stub_.GetStoreRpcClient()->SendRpc(rpc, [this, &rpc] { SendCoordinatorRpcCallBack(rpc); });
}

void CoordinatorRpcController::SendCoordinatorRpcCallBack(Rpc& rpc) {
  Status sent = rpc.GetStatus();
  if (!sent.ok()) {
    meta_member_info_.MarkFollower(rpc.GetEndPoint());
    DINGO_LOG(WARNING) << "Fail connect to meta server: " << rpc.GetEndPoint().ToString()
                       << ", status:" << sent.ToString();
  } else {
    auto error = GetRpcResponseError(rpc);
    if (error.errcode() == pb::error::Errno::OK) {
      VLOG(kSdkVlogLevel) << "Success connect with meta server leader_addr: " << rpc.GetEndPoint().ToString();
      Status s = Status::OK();
      rpc.SetStatus(s);
    } else {
      DINGO_LOG(INFO) << fmt::format("log_id:{} method:{} endpoint:{}, error_code:{}, error_msg:{}", rpc.LogId(),
                                     rpc.Method(), rpc.GetEndPoint().ToString(),
                                     dingodb::pb::error::Errno_Name(error.errcode()), error.errmsg());

      if (error.errcode() == pb::error::Errno::ERAFT_NOTLEADER) {
        meta_member_info_.MarkFollower(rpc.GetEndPoint());
        if (error.has_leader_location()) {
          auto endpoint = LocationToEndPoint(error.leader_location());
          if (!endpoint.IsValid()) {
            DINGO_LOG(WARNING) << "endpoint: " << endpoint.ToString() << " is invalid";
          } else {
            meta_member_info_.MarkLeader(endpoint);
          }
        }

        Status s = Status::NotLeader(error.errcode(), error.errmsg());
        rpc.SetStatus(s);
      } else if (error.errcode() == pb::error::Errno::EREGION_NOT_FOUND) {
        Status s = Status::NotFound(error.errcode(), error.errmsg());
        rpc.SetStatus(s);
      } else if (error.errcode() == pb::error::Errno::EINDEX_NOT_FOUND) {
        Status s = Status::NotFound(error.errcode(), error.errmsg());
        rpc.SetStatus(s);
      } else {
        Status s = Status::Incomplete(error.errcode(), error.errmsg());
        rpc.SetStatus(s);
      }
    }
  }

  RetrySendRpcOrFireCallback(rpc);
}

static bool NeedRetry(Rpc& rpc) { return rpc.GetRetryTimes() < FLAGS_coordinator_interaction_max_retry; }

void CoordinatorRpcController::RetrySendRpcOrFireCallback(Rpc& rpc) {
  Status status = rpc.GetStatus();
  if (status.IsOK()) {
    FireCallback(rpc);
    return;
  }

  if (status.IsNetworkError() || status.IsNotLeader()) {
    if (NeedRetry(rpc)) {
      rpc.IncRetryTimes();
      DoAsyncCall(rpc);
    } else {
      rpc.SetStatus(Status::Aborted("rpc retry times exceed"));
      FireCallback(rpc);
      return;
    }
  } else {
    FireCallback(rpc);
  }
}

void CoordinatorRpcController::FireCallback(Rpc& rpc) {
  Status status = rpc.GetStatus();

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "Fail send rpc: " << rpc.Method() << ", status: " << status.ToString()
                       << ", retry_times:" << rpc.GetRetryTimes()
                       << ", max_retry_limit:" << FLAGS_coordinator_interaction_max_retry;
  }

  if (rpc.call_back) {
    StatusCallback cb;
    rpc.call_back.swap(cb);
    cb(rpc.GetStatus());
  }
}

}  // namespace sdk
}  // namespace dingodb