
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

#include <iostream>

#include "client_v2/client_helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "subcommand_coordinator.h"

#ifndef DINGODB_SUBCOMMAND_HELPER_H_
#define DINGODB_SUBCOMMAND_HELPER_H_

namespace client_v2 {

class Bthread {
 public:
  template <class Fn, class... Args>
  Bthread(const bthread_attr_t* attr, Fn&& fn, Args&&... args) {  // NOLINT
    auto p_wrap_fn = new auto([=] { fn(args...); });
    auto call_back = [](void* ar) -> void* {
      auto f = reinterpret_cast<decltype(p_wrap_fn)>(ar);
      (*f)();
      delete f;
      return nullptr;
    };

    bthread_start_background(&th_, attr, call_back, (void*)p_wrap_fn);
    joinable_ = true;
  }

  void Join() {
    if (joinable_) {
      bthread_join(th_, nullptr);
      joinable_ = false;
    }
  }

  bool Joinable() const noexcept { return joinable_; }

  bthread_t GetId() const { return th_; }

 private:
  bthread_t th_;
  bool joinable_ = false;
};

class SubcommandHelper {
 public:
  static dingodb::pb::common::Engine GetEngine(const std::string& engine_name) {
    if (engine_name == "rocksdb") {
      return dingodb::pb::common::Engine::ENG_ROCKSDB;
    } else if (engine_name == "bdb") {
      return dingodb::pb::common::Engine::ENG_BDB;
    } else {
      DINGO_LOG(FATAL) << "engine_name is illegal, please input -engine=[rocksdb, bdb]";
    }
  }

  static int GetCreateTableIds(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, int64_t count,
                               std::vector<int64_t>& table_ids) {
    dingodb::pb::meta::CreateTableIdsRequest request;
    dingodb::pb::meta::CreateTableIdsResponse response;

    auto* schema_id = request.mutable_schema_id();
    schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
    schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
    schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

    request.set_count(count);

    auto status = coordinator_interaction->SendRequest("CreateTableIds", request, response);
    DINGO_LOG(INFO) << "SendRequest status=" << status;
    DINGO_LOG(INFO) << response.DebugString();

    if (response.table_ids_size() > 0) {
      for (const auto& id : response.table_ids()) {
        table_ids.push_back(id.entity_id());
      }
      return 0;
    } else {
      return -1;
    }
  }

  static dingodb::pb::common::RawEngine GetRawEngine(const std::string& engine_name) {
    if (engine_name == "rocksdb") {
      return dingodb::pb::common::RawEngine::RAW_ENG_ROCKSDB;
    } else if (engine_name == "bdb") {
      return dingodb::pb::common::RawEngine::RAW_ENG_BDB;
    } else if (engine_name == "xdp") {
      return dingodb::pb::common::RawEngine::RAW_ENG_XDPROCKS;
    } else {
      DINGO_LOG(FATAL) << "raw_engine_name is illegal, please input -raw-engine=[rocksdb, bdb]";
    }

    return dingodb::pb::common::RawEngine::RAW_ENG_ROCKSDB;
  }

  static int GetCreateTableId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction,
                              int64_t& table_id) {
    dingodb::pb::meta::CreateTableIdRequest request;
    dingodb::pb::meta::CreateTableIdResponse response;

    auto* schema_id = request.mutable_schema_id();
    schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
    schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
    schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

    auto status = coordinator_interaction->SendRequest("CreateTableId", request, response);
    DINGO_LOG(INFO) << "SendRequest status=" << status;
    DINGO_LOG(INFO) << response.DebugString();

    if (response.has_table_id()) {
      table_id = response.table_id().entity_id();
      return 0;
    } else {
      return -1;
    }
  }
};

}  // namespace client_v2
#endif  // DINGODB_SUBCOMMAND_HELPER_H_