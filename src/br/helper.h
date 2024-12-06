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

#ifndef DINGODB_BR_HELPER_H_
#define DINGODB_BR_HELPER_H_

#include <time.h>

#include <cstdint>
#include <ctime>
#include <fstream>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "butil/endpoint.h"
#include "butil/strings/string_split.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "document/codec.h"
#include "fmt/core.h"
#include "proto/debug.pb.h"
#include "serial/buf.h"
#include "vector/codec.h"

namespace br {

#if 0

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

const char kAlphabetV2[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y'};

const int kGroupSize = 8;
const int kPadGroupSize = 9;
const uint8_t kMarker = 255;

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

#endif

class Helper {
 public:
#if 0
  static std::string ToUpperCase(const std::string& input) {
    std::string output = input;
    std::transform(output.begin(), output.end(), output.begin(), [](unsigned char c) { return std::toupper(c); });
    return output;
  }
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

#endif

  static std::vector<butil::EndPoint> StringToEndpoints(const std::string& str);

  static std::vector<butil::EndPoint> VectorToEndpoints(std::vector<std::string> addrs);
#if 0

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
    if (vector.float_values_size() > 0) {
      return fmt::format("{}/[{} {}...]", vector.float_values_size(), vector.float_values().at(0),
                         vector.float_values().at(1));
    } else if (vector.binary_values_size() > 0) {
      return fmt::format("{}/[{} {}...]", vector.binary_values_size(), vector.binary_values().at(0),
                         vector.binary_values().at(1));
    } else {
      return "no data";
    }
  }

  static std::vector<std::string> FormatVectorScalar(const dingodb::pb::common::VectorScalardata& scalar) {
    auto format_scalar_func = [](const dingodb::pb::common::ScalarValue& value) -> std::string {
      std::string str = "[";
      switch (value.field_type()) {
        case dingodb::pb::common::ScalarFieldType::BOOL:
          for (const auto& field : value.fields()) {
            str.append(fmt::format("{},", field.bool_data()));
          }
          str.pop_back();
          break;
        case dingodb::pb::common::ScalarFieldType::INT8:
          for (const auto& field : value.fields()) {
            str.append(fmt::format("{},", field.int_data()));
          }
          str.pop_back();
          break;
        case dingodb::pb::common::ScalarFieldType::INT16:
          for (const auto& field : value.fields()) {
            str.append(fmt::format("{},", field.int_data()));
          }
          str.pop_back();
          break;
        case dingodb::pb::common::ScalarFieldType::INT32:
          for (const auto& field : value.fields()) {
            str.append(fmt::format("{},", field.int_data()));
          }
          str.pop_back();
          break;
        case dingodb::pb::common::ScalarFieldType::INT64:
          for (const auto& field : value.fields()) {
            str.append(fmt::format("{},", field.long_data()));
          }
          str.pop_back();
          break;
        case dingodb::pb::common::ScalarFieldType::FLOAT32:
          for (const auto& field : value.fields()) {
            str.append(fmt::format("{},", field.float_data()));
          }
          str.pop_back();
          break;
        case dingodb::pb::common::ScalarFieldType::DOUBLE:
          for (const auto& field : value.fields()) {
            str.append(fmt::format("{},", field.double_data()));
          }
          str.pop_back();
          break;
        case dingodb::pb::common::ScalarFieldType::STRING:
          for (const auto& field : value.fields()) {
            str.append(fmt::format("{},", field.string_data()));
          }
          str.pop_back();
          break;
        case dingodb::pb::common::ScalarFieldType::BYTES:
          for (const auto& field : value.fields()) {
            str.append(field.bytes_data());
            str.push_back(',');
          }
          str.pop_back();
          break;
        default:
          break;
      }

      if (str.size() > 32) {
        str = str.substr(0, 32);
      }

      str.push_back(']');
      return str;
    };

    std::vector<std::string> result;
    for (const auto& [key, value] : scalar.scalar_data()) {
      result.push_back(fmt::format("{}: {}", key, format_scalar_func(value)));
    }

    return result;
  }

  static std::string FormatVectorTable(const dingodb::pb::common::VectorTableData& table) {
    return fmt::format("{}:{}", dingodb::Helper::StringToHex(table.table_key()),
                       dingodb::Helper::StringToHex(table.table_value()));
  }

  static std::vector<std::string> FormatDocument(const dingodb::pb::common::Document& document) {
    auto format_scalar_func = [](const dingodb::pb::common::DocumentValue& value) -> std::string {
      switch (value.field_type()) {
        case dingodb::pb::common::ScalarFieldType::BOOL:
          return fmt::format("{}", value.field_value().bool_data());
        case dingodb::pb::common::ScalarFieldType::INT8:
          return fmt::format("{}", value.field_value().int_data());
        case dingodb::pb::common::ScalarFieldType::INT16:
          return fmt::format("{}", value.field_value().int_data());
        case dingodb::pb::common::ScalarFieldType::INT32:
          return fmt::format("{}", value.field_value().int_data());
        case dingodb::pb::common::ScalarFieldType::INT64:
          return fmt::format("{}", value.field_value().long_data());
        case dingodb::pb::common::ScalarFieldType::FLOAT32:
          return fmt::format("{}", value.field_value().float_data());
        case dingodb::pb::common::ScalarFieldType::DOUBLE:
          return fmt::format("{}", value.field_value().double_data());
        case dingodb::pb::common::ScalarFieldType::STRING:
          return fmt::format("{}", value.field_value().string_data());
        case dingodb::pb::common::ScalarFieldType::BYTES:
          return fmt::format("{}", value.field_value().bytes_data());
        default:
          return "Unknown Type";
      }
      return "";
    };

    std::vector<std::string> result;
    for (const auto& [key, value] : document.document_data()) {
      result.push_back(fmt::format("{}: {}", key, format_scalar_func(value)));
    }

    return result;
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

  static int DecodeBytes(const std::string_view& encode_key, std::string& output) {
    output.reserve(256);

    const auto* data = encode_key.data();
    for (int i = 0; i < encode_key.size(); i++) {
      uint8_t marker = encode_key.at(i + 8);

      int pad_count = kMarker - marker;
      for (int j = 0; j < kGroupSize - pad_count; ++j) {
        output.push_back(encode_key.at(i++));
      }

      if (pad_count != 0) {
        for (int j = 0; j < pad_count; ++j) {
          if (encode_key.at(i++) != 0) {
            return -1;
          }
        }

        return i;
      }
    }

    return -1;
  }

  static void PrintIndexRange(dingodb::pb::meta::IndexRange& index_range) {  // NOLINT
    DINGO_LOG(INFO) << "refresh route...";
    for (const auto& item : index_range.range_distribution()) {
      DINGO_LOG(INFO) << fmt::format("region {} range [{}-{})", item.id().entity_id(),
                                     dingodb::Helper::StringToHex(item.range().start_key()),
                                     dingodb::Helper::StringToHex(item.range().end_key()));
    }
  }

  static dingodb::pb::common::Range DecodeRangeToPlaintext(const dingodb::pb::common::Region& region) {
    const dingodb::pb::common::RegionDefinition& region_definition = region.definition();
    const auto& origin_range = region_definition.range();

    dingodb::pb::common::Range plaintext_range;
    if (region.region_type() == dingodb::pb::common::INDEX_REGION &&
        region.definition().index_parameter().has_vector_index_parameter()) {
      int64_t min_vector_id = 0, max_vector_id = 0;
      dingodb::VectorCodec::DecodeRangeToVectorId(false, origin_range, min_vector_id, max_vector_id);

      plaintext_range.set_start_key(fmt::format("{}/{}/{}", dingodb::Helper::GetKeyPrefix(origin_range.start_key()),
                                                dingodb::VectorCodec::UnPackagePartitionId(origin_range.start_key()),
                                                min_vector_id));

      plaintext_range.set_end_key(fmt::format("{}/{}/{}", dingodb::Helper::GetKeyPrefix(origin_range.end_key()),
                                              dingodb::VectorCodec::UnPackagePartitionId(origin_range.end_key()),
                                              max_vector_id));

    } else if (region.region_type() == dingodb::pb::common::DOCUMENT_REGION &&
               region.definition().index_parameter().has_document_index_parameter()) {
      int64_t min_document_id = 0, max_document_id = 0;
      dingodb::DocumentCodec::DecodeRangeToDocumentId(false, origin_range, min_document_id, max_document_id);

      plaintext_range.set_start_key(fmt::format("{}/{}/{}", dingodb::Helper::GetKeyPrefix(origin_range.start_key()),
                                                dingodb::DocumentCodec::UnPackagePartitionId(origin_range.start_key()),
                                                min_document_id));

      plaintext_range.set_end_key(fmt::format("{}/{}/{}", dingodb::Helper::GetKeyPrefix(origin_range.end_key()),
                                              dingodb::DocumentCodec::UnPackagePartitionId(origin_range.end_key()),
                                              max_document_id));

    } else if (dingodb::Helper::IsExecutorRaw(origin_range.start_key()) ||
               dingodb::Helper::IsExecutorTxn(origin_range.start_key())) {
    } else {
      std::string start_key = origin_range.start_key().substr(1, origin_range.start_key().size());
      plaintext_range.set_start_key(fmt::format("{}/{}", dingodb::Helper::GetKeyPrefix(origin_range.start_key()),
                                                dingodb::Helper::StringToHex(start_key)));

      std::string end_key = origin_range.end_key().substr(1, origin_range.end_key().size());
      plaintext_range.set_end_key(fmt::format("{}/{}", dingodb::Helper::GetKeyPrefix(origin_range.end_key()),
                                              dingodb::Helper::StringToHex(end_key)));
    }
    return plaintext_range;
  }

  static bool IsDateString(const std::string& date_str) {
    struct tm time_struct = {0};
    return strptime(date_str.c_str(), "%Y-%m-%d", &time_struct) != nullptr;
  }

  static int64_t StringToTimestamp(const std::string& date_str) {
    struct tm time_struct = {0};
    if (!strptime(date_str.c_str(), "%Y-%m-%d", &time_struct)) {
      DINGO_LOG(ERROR) << "Error converting date string to tm structure!" << std::endl;
      return -1;
    }

    time_t timestamp = std::mktime(&time_struct);
    if (timestamp <= 0) {
      DINGO_LOG(ERROR) << fmt::format("mktime failed, timestamp: {}", timestamp);
      return -1;
    }

    // add 8 hour
    return static_cast<int64_t>(timestamp + 28800);
  }
#endif
};

}  // namespace br

#endif  // DINGODB_BR_HELPER_H_