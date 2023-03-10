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

#include <string>

#include "brpc/controller.h"
#include "common/meta_control.h"
#include "common/synchronization.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

class Context {
 public:
  Context()
      : cntl_(nullptr),
        done_(nullptr),
        response_(nullptr),
        region_id_(0),
        cf_name_(""),
        directly_delete_(false),
        delete_files_in_range_(false),
        flush_(false),
        role_(pb::common::ClusterRole::STORE),
        enable_sync_(false),
        meta_ctl_(nullptr) {}
  Context(brpc::Controller* cntl, google::protobuf::Closure* done)
      : cntl_(cntl),
        done_(done),
        response_(nullptr),
        region_id_(0),
        cf_name_(""),
        directly_delete_(false),
        delete_files_in_range_(false),
        flush_(false),
        role_(pb::common::ClusterRole::STORE),
        enable_sync_(false),
        meta_ctl_(nullptr) {}
  Context(brpc::Controller* cntl, google::protobuf::Closure* done, google::protobuf::Message* response)
      : cntl_(cntl),
        done_(done),
        response_(response),
        region_id_(0),
        cf_name_(""),
        directly_delete_(false),
        delete_files_in_range_(false),
        flush_(false),
        role_(pb::common::ClusterRole::STORE),
        enable_sync_(false),
        meta_ctl_(nullptr) {}
  ~Context() = default;

  brpc::Controller* cntl() { return cntl_; }
  Context& set_cntl(brpc::Controller* cntl) {
    cntl_ = cntl;
    return *this;
  }

  google::protobuf::Closure* done() { return done_; }
  Context& set_done(google::protobuf::Closure* done) {
    done_ = done;
    return *this;
  }

  google::protobuf::Message* response() { return response_; }
  Context& set_response(google::protobuf::Message* response) {
    response_ = response;
    return *this;
  }

  uint64_t region_id() { return region_id_; }
  Context& set_region_id(uint64_t region_id) {
    region_id_ = region_id;
    return *this;
  }

  void set_cf_name(const std::string& cf_name) { cf_name_ = cf_name; }
  const std::string& cf_name() const { return cf_name_; }

  bool directly_delete() { return directly_delete_; }
  void set_directly_delete(bool directly_delete) { directly_delete_ = directly_delete; }

  bool delete_files_in_range() { return delete_files_in_range_; }
  void set_delete_files_in_range(bool delete_files_in_range) { delete_files_in_range_ = delete_files_in_range; }

  bool flush() { return flush_; }
  void set_flush(bool flush) { flush_ = flush; }

  pb::common::ClusterRole ClusterRole() { return role_; }
  void SetClusterRole(pb::common::ClusterRole role) { role_ = role; }

  void EnableSyncMode() {
    enable_sync_ = true;
    cond_ = std::make_shared<BthreadCond>();
  }

  bool IsSyncMode() { return enable_sync_; }

  std::shared_ptr<BthreadCond> Cond() { return cond_; }
  butil::Status Status() { return status_; }
  void SetStatus(butil::Status& status) { status_ = status; }
  void SetMetaController(MetaControl* meta_ctl) { meta_ctl_ = meta_ctl; }

  MetaControl* GetMetaControl() { return meta_ctl_; }

 private:
  // brpc framework free resource
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
  google::protobuf::Message* response_;

  uint64_t region_id_;
  // Column family name
  std::string cf_name_;
  // Directly delete data, not through raft.
  bool directly_delete_;
  // Rocksdb delete range in files
  bool delete_files_in_range_;
  // Flush data to persistence.
  bool flush_;
  // role
  pb::common::ClusterRole role_;

  // For sync mode
  bool enable_sync_;
  butil::Status status_;
  std::shared_ptr<BthreadCond> cond_;
  MetaControl* meta_ctl_;
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_CONTEXT_H_
