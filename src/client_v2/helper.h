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

#ifndef DINGODB_CLIENT_HELPER_H_
#define DINGODB_CLIENT_HELPER_H_

#include <cstdint>
#include <fstream>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "butil/endpoint.h"
#include "butil/strings/string_split.h"
#include "client_v2/interation.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "document/codec.h"
#include "fmt/core.h"
#include "proto/debug.pb.h"
#include "serial/buf.h"
#include "vector/codec.h"

namespace client_v2 {

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

const char kAlphabetV2[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y'};

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

class CoordinatorInteraction {
 private:
  CoordinatorInteraction() = default;
  ~CoordinatorInteraction() = default;

  dingodb::CoordinatorInteractionPtr coordinator_interaction_;
  dingodb::CoordinatorInteractionPtr coordinator_interaction_meta_;
  dingodb::CoordinatorInteractionPtr coordinator_interaction_version_;

 public:
  static CoordinatorInteraction& GetInstance() {
    static CoordinatorInteraction instance;
    return instance;
  }
  CoordinatorInteraction(const CoordinatorInteraction&) = delete;
  CoordinatorInteraction& operator=(const CoordinatorInteraction) = delete;
  void SetCoorinatorInteraction(dingodb::CoordinatorInteractionPtr interaction) {
    coordinator_interaction_ = interaction;
  }
  void SetCoorinatorInteractionMeta(dingodb::CoordinatorInteractionPtr interaction) {
    coordinator_interaction_meta_ = interaction;
  }
  void SetCoorinatorInteractionVersion(dingodb::CoordinatorInteractionPtr interaction) {
    coordinator_interaction_version_ = interaction;
  }
  dingodb::CoordinatorInteractionPtr GetCoorinatorInteraction() { return coordinator_interaction_; }
  dingodb::CoordinatorInteractionPtr GetCoorinatorInteractionMeta() { return coordinator_interaction_meta_; }
  dingodb::CoordinatorInteractionPtr GetCoorinatorInteractionVersion() { return coordinator_interaction_version_; }
};
class Helper {
 public:
  static std::string Ltrim(const std::string& s, const std::string& delete_str) {
    size_t start = s.find_first_not_of(delete_str);
    return (start == std::string::npos) ? "" : s.substr(start);
  }

  static std::string Rtrim(const std::string& s, const std::string& delete_str) {
    size_t end = s.find_last_not_of(delete_str);
    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
  }

  static std::string Trim(const std::string& s, const std::string& delete_str) {
    return Rtrim(Ltrim(s, delete_str), delete_str);
  }

  static int GetRandInt() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
    return distrib(gen);
  }

  // rand string
  static std::string GenRandomString(int len) {
    std::string result;
    int alphabet_len = sizeof(kAlphabet);

    std::mt19937 rng;
    rng.seed(std::random_device()());
    std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
    for (int i = 0; i < len; ++i) {
      result.append(1, kAlphabet[distrib(rng) % alphabet_len]);
    }

    return result;
  }

  static std::string GenRandomStringV2(int len) {
    std::string result;
    int alphabet_len = sizeof(kAlphabetV2);

    std::mt19937 rng;
    rng.seed(std::random_device()());
    std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
    for (int i = 0; i < len; ++i) {
      result.append(1, kAlphabetV2[distrib(rng) % alphabet_len]);
    }

    return result;
  }

  static std::vector<std::string> GenKeys(int nums) {
    std::vector<std::string> vec;
    vec.reserve(nums);
    for (int i = 0; i < nums; ++i) {
      vec.push_back(GenRandomString(4));
    }

    return vec;
  }

  static std::map<std::string, std::string> GenDataset(const std::string& prefix, int n) {
    std::map<std::string, std::string> dataset;

    for (int i = 0; i < n; ++i) {
      std::string key = prefix + GenRandomStringV2(32);
      dataset[key] = GenRandomString(256);
    }

    return dataset;
  }

  static std::vector<butil::EndPoint> StringToEndpoints(const std::string& str) {
    std::vector<std::string> addrs;
    butil::SplitString(str, ',', &addrs);

    std::vector<butil::EndPoint> endpoints;
    for (const auto& addr : addrs) {
      butil::EndPoint endpoint;
      if (butil::hostname2endpoint(addr.c_str(), &endpoint) != 0 && str2endpoint(addr.c_str(), &endpoint) != 0) {
        continue;
      }

      endpoints.push_back(endpoint);
    }

    return endpoints;
  }

  static std::vector<butil::EndPoint> VectorToEndpoints(std::vector<std::string> addrs) {
    std::vector<butil::EndPoint> endpoints;
    for (const auto& addr : addrs) {
      butil::EndPoint endpoint;
      if (butil::hostname2endpoint(addr.c_str(), &endpoint) != 0 && str2endpoint(addr.c_str(), &endpoint) != 0) {
        continue;
      }

      endpoints.push_back(endpoint);
    }

    return endpoints;
  }

  static bool RandomChoice() { return GetRandInt() % 2 == 0; }

  static std::vector<std::string> GetAddrsFromFile(const std::string& path) {
    std::vector<std::string> addrs;

    std::ifstream input(path);
    for (std::string line; getline(input, line);) {
      if (line.find('#') != std::string::npos) {
        continue;
      }

      addrs.push_back(Trim(line, " "));
    }

    return addrs;
  }

  static std::string EncodeRegionRange(int64_t partition_id) {
    dingodb::Buf buf(9);
    buf.Write(dingodb::Constant::kClientRaw);
    buf.WriteLong(partition_id);

    return buf.GetString();
  }

  static std::string CalculateVectorMiddleKey(const std::string& start_key, const std::string& end_key) {
    int64_t partition_id = dingodb::VectorCodec::UnPackagePartitionId(start_key);
    int64_t min_vector_id = dingodb::VectorCodec::UnPackageVectorId(start_key);
    int64_t max_vector_id = dingodb::VectorCodec::UnPackageVectorId(end_key);
    max_vector_id = max_vector_id > 0 ? max_vector_id : INT64_MAX;
    int64_t mid_vector_id = min_vector_id + (max_vector_id - min_vector_id) / 2;

    DINGO_LOG(INFO) << "mid_vector_id: " << mid_vector_id;
    std::string result = dingodb::VectorCodec::PackageVectorKey(start_key[0], partition_id, mid_vector_id);
    return result;
  }

  static std::string CalculateDocumentMiddleKey(const std::string& start_key, const std::string& end_key) {
    int64_t partition_id = dingodb::DocumentCodec::UnPackagePartitionId(start_key);
    int64_t min_document_id = dingodb::DocumentCodec::UnPackageDocumentId(start_key);
    int64_t max_document_id = dingodb::DocumentCodec::UnPackageDocumentId(end_key);
    max_document_id = max_document_id > 0 ? max_document_id : INT64_MAX;
    int64_t mid_document_id = min_document_id + (max_document_id - min_document_id) / 2;

    DINGO_LOG(INFO) << "mid_document_id: " << mid_document_id;
    std::string result = dingodb::DocumentCodec::PackageDocumentKey(start_key[0], partition_id, mid_document_id);
    return result;
  }

  // format and print
  static std::string FormatVectorData(const dingodb::pb::common::Vector& vector) {
    std::string value_type = dingodb::pb::common::ValueType_Name(vector.value_type());
    if (vector.float_values_size() > 0) {
      return fmt::format("{}/{}/[{} {}...]", vector.float_values_size(), value_type, vector.float_values().at(0),
                         vector.float_values().at(1));
    } else if (vector.binary_values_size() > 0) {
      return fmt::format("{}/{}/[{} {}...]", vector.binary_values_size(), value_type, vector.binary_values().at(0),
                         vector.binary_values().at(1));
    } else {
      return "no data";
    }
  }

  static std::string FormatVectorScalar(const dingodb::pb::common::VectorScalardata& scalar) {
    std::string result;
    for (const auto& [key, value] : scalar.scalar_data()) {
      result += fmt::format("{}/{}", key, value.ShortDebugString());
      result += ";";
    }

    return result;
  }

  static std::string FormatVectorTable(const dingodb::pb::common::VectorTableData& table) {
    return fmt::format("{}/{}", dingodb::Helper::StringToHex(table.table_key()),
                       dingodb::Helper::StringToHex(table.table_value()));
  }

  static std::string FormatDocument(const dingodb::pb::common::Document& document) {
    std::string result;
    for (const auto& [key, value] : document.document_data()) {
      result += fmt::format("{}/{}", key, value.ShortDebugString());
      result += ";";
    }

    return result;
  }

  static void PrintRegionData(dingodb::pb::debug::DumpRegionResponse& response, bool show_detail) {
    std::cout << "==================== show data ====================" << std::endl;
    for (const auto& kv : response.data().kvs()) {
      auto flag = dingodb::pb::debug::DumpRegionResponse::ValueFlag_Name(kv.flag());
      std::cout << fmt::format("key({}) ts({}) flag({}) ttl({}) value({})", dingodb::Helper::StringToHex(kv.key()),
                               kv.ts(), flag, kv.ttl(), kv.value().substr(0, 32))
                << std::endl;
    }

    for (const auto& vector : response.data().vectors()) {
      auto flag = dingodb::pb::debug::DumpRegionResponse::ValueFlag_Name(vector.flag());
      if (show_detail) {
        std::cout << fmt::format("vector_id({}) ts({}) flag({}) ttl({}) vector({}) scalar({}) table({})",
                                 vector.vector_id(), vector.ts(), flag, vector.ttl(), FormatVectorData(vector.vector()),
                                 FormatVectorScalar(vector.scalar_data()), FormatVectorTable(vector.table_data()))
                  << std::endl;
      } else {
        std::cout << fmt::format("vector_id({}) ts({}) flag({}) ttl({}) vector({})", vector.vector_id(), vector.ts(),
                                 flag, vector.ttl(), FormatVectorData(vector.vector()))
                  << std::endl;
      }
    }

    for (const auto& document : response.data().documents()) {
      auto flag = dingodb::pb::debug::DumpRegionResponse::ValueFlag_Name(document.flag());
      std::cout << fmt::format("doc_id({}) ts({}) flag({}) ttl({}) data({})", document.document_id(), document.ts(),
                               flag, document.ttl(), FormatDocument(document.document()))
                << std::endl;
    }

    int size = std::max(response.data().kvs_size(), response.data().vectors_size());
    size = std::max(size, response.data().documents_size());

    std::cout << fmt::format("==================== size({}) ====================", size) << std::endl;
  }

  static void PrintVectorWithId(const dingodb::pb::common::VectorWithId& vector_with_id) {
    std::cout << fmt::format("vector_id({}) vector({}) scalar({}) table({})", vector_with_id.id(),
                             FormatVectorData(vector_with_id.vector()),
                             FormatVectorScalar(vector_with_id.scalar_data()),
                             FormatVectorTable(vector_with_id.table_data()))
              << std::endl;
  }
  static dingodb::pb::common::Engine GetEngine(const std::string& engine_name) {
    if (engine_name == "rocksdb") {
      return dingodb::pb::common::Engine::ENG_ROCKSDB;
    } else if (engine_name == "bdb") {
      return dingodb::pb::common::Engine::ENG_BDB;
    } else {
      DINGO_LOG(FATAL) << "engine_name is illegal, please input -engine=[rocksdb, bdb]";
    }
  }

  static int GetCreateTableIds(dingodb::CoordinatorInteractionPtr coordinator_interaction, int64_t count,
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

  static int SetUp(std::string url) {
    if (url.empty()) {
      url = "file://./coor_list";
    }

    if (!url.empty()) {
      std::string path = url;
      path = path.replace(path.find("file://"), 7, "");
      auto addrs = Helper::GetAddrsFromFile(path);
      if (addrs.empty()) {
        std::cout << "coor_url not find addr, path=" << path << std::endl;
        return -1;
      }

      auto coordinator_interaction = std::make_shared<ServerInteraction>();
      if (!coordinator_interaction->Init(addrs)) {
        std::cout << "Fail to init coordinator_interaction, please check parameter --url=" << url << std::endl;
        return -1;
      }

      InteractionManager::GetInstance().SetCoorinatorInteraction(coordinator_interaction);
    }

    // this is for legacy coordinator_client use, will be removed in the future
    if (!url.empty()) {
      auto coordinator_interaction = std::make_shared<dingodb::CoordinatorInteraction>();
      if (!coordinator_interaction->InitByNameService(
              url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeCoordinator)) {
        std::cout << "Fail to init coordinator_interaction, please check parameter --coor_url=" << url << std::endl;
        return -1;
      }
      CoordinatorInteraction::GetInstance().SetCoorinatorInteraction(coordinator_interaction);
      auto coordinator_interaction_meta = std::make_shared<dingodb::CoordinatorInteraction>();
      if (!coordinator_interaction_meta->InitByNameService(
              url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeMeta)) {
        std::cout << "Fail to init coordinator_interaction_meta, please check parameter --coor_url=" << url
                  << std::endl;
        return -1;
      }
      CoordinatorInteraction::GetInstance().SetCoorinatorInteractionMeta(coordinator_interaction_meta);
      auto coordinator_interaction_version = std::make_shared<dingodb::CoordinatorInteraction>();
      if (!coordinator_interaction_version->InitByNameService(
              url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeVersion)) {
        std::cout << "Fail to init coordinator_interaction_version, please check parameter --coor_url=" << url
                  << std::endl;
        return -1;
      }
      CoordinatorInteraction::GetInstance().SetCoorinatorInteractionVersion(coordinator_interaction_version);
    }
    return 0;
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

  static int GetCreateTableId(dingodb::CoordinatorInteractionPtr coordinator_interaction, int64_t& table_id) {
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

#endif  // DINGODB_CLIENT_HELPER_H_