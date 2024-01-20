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

#ifndef DINGODB_COMMON_COORDINATOR_INTERACTION_H_
#define DINGODB_COMMON_COORDINATOR_INTERACTION_H_

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <memory>

#include "brpc/channel.h"
#include "bthread/mutex.h"
#include "bthread/types.h"
#include "butil/endpoint.h"
#include "butil/fast_rand.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/safe_map.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

const int kMaxRetry = 5;

// For store interact with coordinator.
class CoordinatorInteraction {
 public:
  CoordinatorInteraction() : leader_index_(0) {
    bthread_mutex_init(&leader_mutex_, nullptr);
    channel_map_.Init(100);
  }
  ~CoordinatorInteraction() = default;

  CoordinatorInteraction(const CoordinatorInteraction&) = delete;
  const CoordinatorInteraction& operator=(const CoordinatorInteraction&) = delete;

  bool Init(const std::string& addr, uint32_t service_type);
  bool InitByNameService(const std::string& service_name, uint32_t service_type);

  int GetLeader();
  void NextLeader(int leader_index);

  template <typename Request, typename Response>
  butil::Status SendRequest(const std::string& api_name, const Request& request, Response& response,
                            int64_t time_out_ms = 60000);

  const ::google::protobuf::ServiceDescriptor* GetServiceDescriptor() const;

  void SetLeaderAddress(const butil::EndPoint& addr);

 private:
  std::atomic<int> leader_index_;
  std::vector<butil::EndPoint> endpoints_;
  std::vector<std::shared_ptr<brpc::Channel>> channels_;

  DingoSafeMap<butil::EndPoint, std::shared_ptr<brpc::Channel>> channel_map_;
  brpc::Channel name_service_channel_;
  butil::EndPoint leader_addr_;
  bthread_mutex_t leader_mutex_;
  uint32_t service_type_ = 0;
  bool use_service_name_ = false;

  template <typename Request, typename Response>
  butil::Status SendRequestByList(const std::string& api_name, const Request& request, Response& response,
                                  int64_t time_out_ms);
  template <typename Request, typename Response>
  butil::Status SendRequestByService(const std::string& api_name, const Request& request, Response& response,
                                     int64_t time_out_ms);
};

template <typename Request, typename Response>
butil::Status CoordinatorInteraction::SendRequest(const std::string& api_name, const Request& request,
                                                  Response& response, int64_t time_out_ms) {
  if (use_service_name_) {
    return SendRequestByService(api_name, request, response, time_out_ms);
  } else {
    return SendRequestByList(api_name, request, response, time_out_ms);
  }
}

template <typename Request, typename Response>
butil::Status CoordinatorInteraction::SendRequestByService(const std::string& api_name, const Request& request,
                                                           Response& response, int64_t time_out_ms) {
  const ::google::protobuf::ServiceDescriptor* service_desc = GetServiceDescriptor();
  if (service_desc == nullptr) {
    return butil::Status(pb::error::ENOT_SUPPORT, "Service type not found");
  }

  const ::google::protobuf::MethodDescriptor* method = service_desc->FindMethodByName(api_name);
  if (method == nullptr) {
    return butil::Status(pb::error::ENOT_SUPPORT, "Service method not found");
  }

  // DINGO_LOG(DEBUG) << "send request api " << api_name << " request: " << request.ShortDebugString();
  int retry_count = 0;
  do {
    brpc::Controller cntl;
    cntl.set_log_id(butil::fast_rand());
    cntl.set_timeout_ms(time_out_ms);

    butil::EndPoint leader_addr;
    {
      BAIDU_SCOPED_LOCK(leader_mutex_);
      leader_addr = leader_addr_;
    }

    // if no leader is set, use bns channel to get leader
    if (leader_addr.ip != butil::IP_ANY) {
      std::shared_ptr<brpc::Channel> channel;
      int ret = channel_map_.Get(leader_addr, channel);
      if (ret < 0) {
        // create new channel
        brpc::ChannelOptions channel_opt;
        // ChannelOptions should set "timeout_ms > connect_timeout_ms" for circuit breaker
        channel_opt.timeout_ms = 4000;
        channel_opt.connect_timeout_ms = 3000;
        std::shared_ptr<brpc::Channel> short_channel = std::make_shared<brpc::Channel>();
        if (short_channel->Init(leader_addr, &channel_opt) != 0) {
          DINGO_LOG(WARNING) << "connect with meta server fail. channel Init fail, leader_addr: "
                             << butil::endpoint2str(leader_addr).c_str();
          ++retry_count;
          continue;
        }
        channel_map_.Put(leader_addr, short_channel);
        channel = short_channel;
      } else if (!channel) {
        channel_map_.Erase(leader_addr);
        DINGO_LOG(WARNING) << "connect with meta server fail. channel is nullptr, leader_addr: "
                           << butil::endpoint2str(leader_addr).c_str();
        SetLeaderAddress(butil::EndPoint());
        ++retry_count;
        continue;
      }

      channel->CallMethod(method, &cntl, &request, &response, nullptr);

      if (cntl.Failed()) {
        DINGO_LOG(WARNING) << "connect with meta server fail. channel CallMethod " << api_name
                           << " fail, leader_addr: " << butil::endpoint2str(leader_addr).c_str()
                           << " errorcode:" << cntl.ErrorCode() << " error text:" << cntl.ErrorText();
        SetLeaderAddress(butil::EndPoint());
        ++retry_count;
        continue;
      } else if (response.error().errcode() == pb::error::Errno::OK) {
        DINGO_LOG(DEBUG) << "connect with meta server success. return OK, leader_addr: "
                         << butil::endpoint2str(leader_addr).c_str();
        return butil::Status();
      } else if (response.error().errcode() == pb::error::Errno::ERAFT_NOTLEADER) {
        if (response.error().has_leader_location()) {
          pb::common::Location leader_location = response.error().leader_location();
          auto leader_endpoint = Helper::LocationToEndPoint(leader_location);

          SetLeaderAddress(leader_endpoint);

          DINGO_LOG(WARNING) << "connect with meta server success, connected to:"
                             << butil::endpoint2str(cntl.remote_side()).c_str()
                             << " but not leader, leader_addr: " << butil::endpoint2str(leader_addr).c_str();
        } else {
          DINGO_LOG(WARNING) << "connect with meta server success, but no leader found: "
                             << butil::endpoint2str(cntl.remote_side()).c_str();
          SetLeaderAddress(butil::EndPoint());
        }
        response.mutable_error()->set_errcode(pb::error::Errno::OK);
        ++retry_count;
        continue;
      } else {
        DINGO_LOG(DEBUG) << "connect with meta server success. return response errcode: " << response.error().errcode()
                         << ", leader_addr: " << butil::endpoint2str(leader_addr).c_str();
        return butil::Status(response.error().errcode(), response.error().errmsg());
      }
    } else {
      name_service_channel_.CallMethod(method, &cntl, &request, &response, nullptr);
      if (cntl.Failed()) {
        DINGO_LOG(WARNING) << "name_service_channel_ connect with meta server fail. channel CallMethod " << api_name
                           << " fail, remote_side: " << butil::endpoint2str(cntl.remote_side()).c_str()
                           << " error_code=" << cntl.ErrorCode() << " error_text=" << cntl.ErrorText();
        ++retry_count;
        continue;
      } else if (response.error().errcode() == pb::error::Errno::ERAFT_NOTLEADER) {
        if (response.error().has_leader_location()) {
          pb::common::Location leader_location = response.error().leader_location();
          auto leader_endpoint = Helper::LocationToEndPoint(leader_location);

          SetLeaderAddress(leader_endpoint);

          DINGO_LOG(WARNING) << "name_service_channel_ connect with meta server success by service name, connected to: "
                             << butil::endpoint2str(cntl.remote_side()).c_str()
                             << " found new leader: " << leader_endpoint.ip << ":" << leader_endpoint.port;
        }
        response.mutable_error()->set_errcode(pb::error::Errno::OK);
        ++retry_count;
        continue;
      } else {
        // call success, set leader
        DINGO_LOG(INFO) << "name_service_channel_ connect with meta server finished. response errcode: "
                        << response.error().errcode()
                        << ", leader_addr: " << butil::endpoint2str(cntl.remote_side()).c_str();
        SetLeaderAddress(cntl.remote_side());
        return butil::Status(response.error().errcode(), response.error().errmsg());
      }
    }
  } while (retry_count < kMaxRetry);

  return butil::Status(pb::error::EINTERNAL,
                       "connect with meta server fail, no leader found or connect timeout, retry count: %d",
                       retry_count);
}

template <typename Request, typename Response>
butil::Status CoordinatorInteraction::SendRequestByList(const std::string& api_name, const Request& request,
                                                        Response& response, int64_t time_out_ms) {
  const ::google::protobuf::ServiceDescriptor* service_desc = GetServiceDescriptor();
  if (service_desc == nullptr) {
    return butil::Status(pb::error::ENOT_SUPPORT, "Service type not found");
  }

  const ::google::protobuf::MethodDescriptor* method = service_desc->FindMethodByName(api_name);
  if (method == nullptr) {
    return butil::Status(pb::error::ENOT_SUPPORT, "Service method not found");
  }

  // DINGO_LOG(DEBUG) << "send request api " << api_name << " request: " << request.ShortDebugString();
  int retry_count = 0;
  do {
    brpc::Controller cntl;
    cntl.set_log_id(butil::fast_rand());
    cntl.set_timeout_ms(time_out_ms);

    const int leader_index = GetLeader();
    channels_[leader_index]->CallMethod(method, &cntl, &request, &response, nullptr);
    // DINGO_LOG(DEBUG) << "send request api " << api_name << " response: " <<
    // response.ShortDebugString();
    if (cntl.Failed()) {
      DINGO_LOG(ERROR) << fmt::format("{} response failed, {} {} {}", api_name, cntl.log_id(), cntl.ErrorCode(),
                                      cntl.ErrorText());
      return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    if (response.error().errcode() != pb::error::OK) {
      if (response.error().errcode() == pb::error::ERAFT_NOTLEADER) {
        ++retry_count;
        NextLeader(leader_index);
      } else {
        return butil::Status(response.error().errcode(), response.error().errmsg());
      }
    } else {
      return butil::Status();
    }

  } while (retry_count < kMaxRetry);

  return butil::Status(pb::error::EINTERNAL,
                       "connect with meta server fail, no leader found or connect timeout, retry count: %d",
                       retry_count);
}

}  // namespace dingodb

#endif  // DINGODB_COMMON_COORDINATOR_INTERACTION_H_
