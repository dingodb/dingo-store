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

#ifndef DINGODB_COMMON_CONTEXT_H_
#define DINGODB_COMMON_CONTEXT_H_

#include <memory>
#include <string>

#include "brpc/controller.h"
#include "common/synchronization.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

class Context;

using WriteCbFunc = std::function<void(std::shared_ptr<Context>, butil::Status)>;

class Context {
 public:
  Context()
      : cntl_(nullptr),
        done_(nullptr),
        request_(nullptr),
        response_(nullptr),
        region_id_(0),
        delete_files_in_range_(false),
        flush_(false),
        // role_(pb::common::ClusterRole::STORE),
        enable_sync_(false) {}
  Context(brpc::Controller* cntl, google::protobuf::Closure* done)
      : cntl_(cntl),
        done_(done),
        request_(nullptr),
        response_(nullptr),
        region_id_(0),
        delete_files_in_range_(false),
        flush_(false),
        // role_(pb::common::ClusterRole::STORE),
        enable_sync_(false) {}
  Context(brpc::Controller* cntl, google::protobuf::Closure* done, google::protobuf::Message* response)
      : cntl_(cntl),
        done_(done),
        request_(nullptr),
        response_(response),
        region_id_(0),
        delete_files_in_range_(false),
        flush_(false),
        // role_(pb::common::ClusterRole::STORE),
        enable_sync_(false) {}
  Context(brpc::Controller* cntl, google::protobuf::Closure* done, const google::protobuf::Message* request,
          google::protobuf::Message* response)
      : cntl_(cntl),
        done_(done),
        request_(request),
        response_(response),
        region_id_(0),
        delete_files_in_range_(false),
        flush_(false),
        // role_(pb::common::ClusterRole::STORE),
        enable_sync_(false) {}
  ~Context() = default;

  brpc::Controller* Cntl() { return cntl_; }
  Context& SetCntl(brpc::Controller* cntl) {
    cntl_ = cntl;
    return *this;
  }

  google::protobuf::Closure* Done() { return done_; }
  Context& SetDone(google::protobuf::Closure* done) {
    done_ = done;
    return *this;
  }

  const google::protobuf::Message* Request() { return request_; }

  google::protobuf::Message* Response() { return response_; }
  Context& SetResponse(google::protobuf::Message* response) {
    response_ = response;
    return *this;
  }

  int64_t RegionId() const { return region_id_; }
  Context& SetRegionId(int64_t region_id) {
    region_id_ = region_id;
    return *this;
  }

  pb::common::RegionEpoch RegionEpoch() const { return region_epoch_; }
  Context& SetRegionEpoch(const pb::common::RegionEpoch& region_epoch) {
    region_epoch_ = region_epoch;
    return *this;
  }

  pb::store::IsolationLevel IsolationLevel() const { return isolation_level_; }
  Context& SetIsolationLevel(const pb::store::IsolationLevel& isolation_level) {
    isolation_level_ = isolation_level;
    return *this;
  }

  void SetCfName(const std::string& cf_name) { cf_name_ = cf_name; }
  const std::string& CfName() const { return cf_name_; }

  bool DeleteFilesInRange() const { return delete_files_in_range_; }
  void SetDeleteFilesInRange(bool delete_files_in_range) { delete_files_in_range_ = delete_files_in_range; }

  bool Flush() const { return flush_; }
  void SetFlush(bool flush) { flush_ = flush; }

  // pb::common::ClusterRole ClusterRole() { return role_; }
  // void SetClusterRole(pb::common::ClusterRole role) { role_ = role; }

  void EnableSyncMode() {
    enable_sync_ = true;
    cond_ = std::make_shared<BthreadCond>();
  }

  bool IsSyncMode() const { return enable_sync_; }

  std::shared_ptr<BthreadCond> Cond() { return cond_; }
  butil::Status Status() { return status_; }
  void SetStatus(butil::Status& status) { status_ = status; }
  void SetStatus(butil::Status&& status) { status_ = status; }  // NOLINT

  WriteCbFunc WriteCb() { return write_cb_; }
  void SetWriteCb(WriteCbFunc write_cb) { write_cb_ = write_cb; }

 private:
  // brpc framework free resource
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  const google::protobuf::Message* request_;
  google::protobuf::Message* response_;

  int64_t region_id_;
  // Column family name
  std::string cf_name_;
  // Rocksdb delete range in files
  bool delete_files_in_range_;
  // Flush data to persistence.
  bool flush_;
  // role
  // pb::common::ClusterRole role_;

  // For sync mode
  bool enable_sync_;
  butil::Status status_;
  std::shared_ptr<BthreadCond> cond_;

  pb::common::RegionEpoch region_epoch_;
  pb::store::IsolationLevel isolation_level_;

  WriteCbFunc write_cb_;
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_CONTEXT_H_
