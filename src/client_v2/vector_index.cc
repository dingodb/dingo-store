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

#include "client_v2/vector_index.h"

#include "client_v2/helper.h"
#include "client_v2/meta.h"
#include "client_v2/pretty.h"

namespace client_v2 {

void SetUpVectorIndexSubCommands(CLI::App& app) {
  SetUpCreateIndex(app);
  SetUpVectorAdd(app);
  SetUpVectorSearch(app);
  SetUpVectorDelete(app);
  SetUpVectorGetMaxId(app);
  SetUpVectorGetMinId(app);
  SetUpVectorAddBatch(app);
  SetUpVectorCount(app);
  SetUpVectorCalcDistance(app);
  SetUpCountVectorTable(app);

  SetUpVectorSearchDebug(app);
  // SetUpVectorRangeSearch(app);
  // SetUpVectorRangeSearchDebug(app);
  // SetUpVectorBatchSearch(app);
  // SetUpVectorBatchQuery(app);
  // SetUpVectorScanQuery(app);
  // SetUpVectorScanDump(app);
  SetUpVectorGetRegionMetrics(app);

  // diskann
  SetUpVectorImport(app);
  SetUpVectorBuild(app);
  SetUpVectorLoad(app);
  SetUpVectorStatus(app);
  SetUpVectorReset(app);
  SetUpVectorDump(app);
  SetUpVectorCountMemory(app);
}

static bool SetUpStore(const std::string& url, const std::vector<std::string>& addrs, int64_t region_id) {
  if (Helper::SetUp(url) < 0) {
    exit(-1);
  }
  if (!addrs.empty()) {
    return client_v2::InteractionManager::GetInstance().CreateStoreInteraction(addrs);
  } else {
    // Get store addr from coordinator
    auto status = client_v2::InteractionManager::GetInstance().CreateStoreInteraction(region_id);
    if (!status.ok()) {
      std::cout << "Create store interaction failed, error: " << status.error_cstr() << std::endl;
      return false;
    }
  }
  return true;
}

bool QueryRegionIdByVectorId(dingodb::pb::meta::IndexRange& index_range, int64_t vector_id,  // NOLINT
                             int64_t& region_id) {                                           // NOLINT
  for (const auto& item : index_range.range_distribution()) {
    const auto& range = item.range();
    int64_t min_vector_id = dingodb::VectorCodec::UnPackageVectorId(range.start_key());
    int64_t max_vector_id = dingodb::VectorCodec::UnPackageVectorId(range.end_key());
    max_vector_id = max_vector_id == 0 ? INT64_MAX : max_vector_id;
    if (vector_id >= min_vector_id && vector_id < max_vector_id) {
      region_id = item.id().entity_id();
      return true;
    }
  }

  std::cout << fmt::format("query region id by key failed, vector_id {}", vector_id);
  return false;
}

int SendBatchVectorAdd(int64_t region_id, uint32_t dimension, std::vector<int64_t> vector_ids,
                       std::vector<std::vector<float>> vector_datas, uint32_t vector_datas_offset, bool with_scalar,
                       bool with_table, std::string scalar_filter_key, std::string scalar_filter_value,
                       std::string scalar_filter_key2, std::string scalar_filter_value2) {
  dingodb::pb::index::VectorAddRequest request;
  dingodb::pb::index::VectorAddResponse response;

  uint32_t max_size = 0;
  if (vector_datas.empty()) {
    max_size = vector_ids.size();
  } else {
    if (vector_datas.size() > vector_datas_offset + vector_ids.size()) {
      max_size = vector_ids.size();
    } else {
      max_size = vector_datas.size() > vector_datas_offset ? vector_datas.size() - vector_datas_offset : 0;
    }
  }

  if (max_size == 0) {
    std::cout << "vector_datas.size() - vector_datas_offset <= vector_ids.size(), max_size: " << max_size
              << ", vector_datas.size: " << vector_datas.size() << ", vector_datas_offset: " << vector_datas_offset
              << ", vector_ids.size: " << vector_ids.size() << std::endl;
    return 0;
  }

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(region_id);

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  for (int i = 0; i < max_size; ++i) {
    const auto& vector_id = vector_ids[i];

    auto* vector_with_id = request.add_vectors();
    vector_with_id->set_id(vector_id);
    vector_with_id->mutable_vector()->set_dimension(dimension);
    vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);

    if (vector_datas.empty()) {
      for (int j = 0; j < dimension; j++) {
        vector_with_id->mutable_vector()->add_float_values(distrib(rng));
      }

    } else {
      const auto& vector_data = vector_datas[i + vector_datas_offset];
      CHECK(vector_data.size() == dimension);

      for (int j = 0; j < dimension; j++) {
        vector_with_id->mutable_vector()->add_float_values(vector_data[j]);
      }
    }

    if (with_scalar) {
      if (scalar_filter_key.empty() || scalar_filter_value.empty()) {
        for (int k = 0; k < 2; ++k) {
          auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
          dingodb::pb::common::ScalarValue scalar_value;
          scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
          scalar_value.add_fields()->set_string_data(fmt::format("scalar_value{}", k));
          (*scalar_data)[fmt::format("scalar_key{}", k)] = scalar_value;
        }
        for (int k = 2; k < 4; ++k) {
          auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
          dingodb::pb::common::ScalarValue scalar_value;
          scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
          scalar_value.add_fields()->set_long_data(k);
          (*scalar_data)[fmt::format("scalar_key{}", k)] = scalar_value;
        }
      } else {
        if (scalar_filter_key == "enable_scalar_schema" || scalar_filter_key2 == "enable_scalar_schema") {
          auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
          // bool speedup key
          {
            std::string key = "speedup_key_bool";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            bool value = i % 2;
            field->set_bool_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // int speedup key
          {
            std::string key = "speedup_key_int";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            int value = i;
            field->set_int_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // long speedup key
          {
            std::string key = "speedup_key_long";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            int64_t value = i + 1000;
            field->set_long_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // float speedup key
          {
            std::string key = "speedup_key_float";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            float value = 0.23 + i;
            field->set_float_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // double speedup key
          {
            std::string key = "speedup_key_double";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            double value = 0.23 + i + 1000;
            field->set_double_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // string speedup key
          {
            std::string key = "speedup_key_string";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            std::string value(1024 * 2, 's');
            value = std::to_string(i) + value;
            field->set_string_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // bytes speedup key
          {
            std::string key = "speedup_key_bytes";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            std::string value(1024 * 2, 'b');
            value = std::to_string(i) + value;
            field->set_bytes_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          //////////////////////////////////no speedup
          /// key/////////////////////////////////////////////////////////////////////////
          // bool key
          {
            std::string key = "key_bool";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            bool value = i % 2;
            field->set_bool_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // int key
          {
            std::string key = "key_int";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            int value = i;
            field->set_int_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // long key
          {
            std::string key = "key_long";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            int64_t value = i + 1000;
            field->set_long_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // float key
          {
            std::string key = "key_float";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            float value = 0.23 + i;
            field->set_float_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // double key
          {
            std::string key = "key_double";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            double value = 0.23 + i + 1000;
            field->set_double_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // string  key
          {
            std::string key = "key_string";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            std::string value(1024 * 2, 's');
            value = std::to_string(i) + value;
            field->set_string_data(value);

            (*scalar_data)[key] = scalar_value;
          }

          // bytes key
          {
            std::string key = "key_bytes";
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
            dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            std::string value(1024 * 2, 'b');
            value = std::to_string(i) + value;
            field->set_bytes_data(value);

            (*scalar_data)[key] = scalar_value;
          }

        } else {
          if (!scalar_filter_key.empty()) {
            auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            scalar_value.add_fields()->set_string_data(scalar_filter_value);
            (*scalar_data)[scalar_filter_key] = scalar_value;
          }

          if (!scalar_filter_key2.empty()) {
            auto* scalar_data = vector_with_id->mutable_scalar_data()->mutable_scalar_data();
            dingodb::pb::common::ScalarValue scalar_value;
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            scalar_value.add_fields()->set_string_data(scalar_filter_value2);
            (*scalar_data)[scalar_filter_key2] = scalar_value;
          }
        }
      }
    }

    if (with_table) {
      auto* table_data = vector_with_id->mutable_table_data();
      table_data->set_table_key(fmt::format("table_key{}", vector_id));
      table_data->set_table_value(fmt::format("table_value{}", vector_id));
    }
  }

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorAdd", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }
  if (Pretty::ShowError(status)) {
    response.error().errcode();
  }

  std::cout << fmt::format("VectorAdd response success region: {} count: {} fail count: {} vector count: {}", region_id,
                           success_count, response.key_states().size() - success_count, request.vectors().size())
            << std::endl;

  return response.error().errcode();
}

// We can use a csv file to import vector_data
// The csv file format is:
// 1.0, 2.0, 3.0, 4.0
// 1.1, 2.1, 3.1, 4.1
// the line count must equal to the vector count, no new line at the end of the file
void SendVectorAddRetry(VectorAddOptions const& opt) {  // NOLINT
  auto index_range = SendGetIndexRange(opt.table_id);
  if (index_range.range_distribution().empty()) {
    std::cout << fmt::format("Not found range of table {}", opt.table_id) << std::endl;
    return;
  }
  Helper::PrintIndexRange(index_range);

  std::vector<std::vector<float>> vector_datas;

  if (!opt.csv_data.empty()) {
    if (!dingodb::Helper::IsExistPath(opt.csv_data)) {
      std::cout << fmt::format("csv data file {} not exist", opt.csv_data) << std::endl;
      return;
    }

    std::ifstream file(opt.csv_data);

    if (file.is_open()) {
      std::string line;
      while (std::getline(file, line)) {
        std::vector<float> row;
        std::stringstream ss(line);
        std::string value;
        while (std::getline(ss, value, ',')) {
          row.push_back(std::stof(value));
        }
        vector_datas.push_back(row);
      }
      file.close();
    }
  }

  uint32_t total_count = 0;

  int64_t end_id = 0;
  if (vector_datas.empty()) {
    end_id = opt.start_id + opt.count;
  } else {
    end_id = opt.start_id + vector_datas.size();
  }

  std::vector<int64_t> vector_ids;
  vector_ids.reserve(opt.step_count);
  for (int64_t i = opt.start_id; i < end_id; i += opt.step_count) {
    for (int64_t j = i; j < i + opt.step_count; ++j) {
      vector_ids.push_back(j);
    }

    int64_t region_id = 0;
    if (!QueryRegionIdByVectorId(index_range, i, region_id)) {
      std::cout << fmt::format("query region id by vector id failed, vector id {}", i) << std::endl;
      return;
    }

    int ret = SendBatchVectorAdd(region_id, opt.dimension, vector_ids, vector_datas, total_count, !opt.without_scalar,
                                 !opt.without_table, opt.scalar_filter_key, opt.scalar_filter_value,
                                 opt.scalar_filter_key2, opt.scalar_filter_value2);
    if (ret == dingodb::pb::error::EKEY_OUT_OF_RANGE || ret == dingodb::pb::error::EREGION_REDIRECT) {
      bthread_usleep(1000 * 500);  // 500ms
      index_range = SendGetIndexRange(opt.table_id);
      Helper::PrintIndexRange(index_range);
    }

    total_count += vector_ids.size();

    vector_ids.clear();
  }
}

// We can use a csv file to import vector_data
// The csv file format is:
// 1.0, 2.0, 3.0, 4.0
// 1.1, 2.1, 3.1, 4.1
// the line count must equal to the vector count, no new line at the end of the file
void SendVectorAdd(VectorAddOptions const& opt) {
  std::vector<int64_t> vector_ids;
  vector_ids.reserve(opt.step_count);

  std::vector<std::vector<float>> vector_datas;

  if (!opt.csv_data.empty()) {
    if (!dingodb::Helper::IsExistPath(opt.csv_data)) {
      std::cout << fmt::format("csv data file {} not exist", opt.csv_data) << std::endl;
      return;
    }

    std::ifstream file(opt.csv_data);

    if (file.is_open()) {
      std::string line;
      while (std::getline(file, line)) {
        std::vector<float> row;
        std::stringstream ss(line);
        std::string value;
        while (std::getline(ss, value, ',')) {
          row.push_back(std::stof(value));
        }
        vector_datas.push_back(row);
      }
      file.close();
    }
  }

  uint32_t total_count = 0;

  int64_t end_id = 0;
  if (vector_datas.empty()) {
    end_id = opt.start_id + opt.count;
  } else {
    end_id = opt.start_id + vector_datas.size();
  }

  for (int i = opt.start_id; i < end_id; i += opt.step_count) {
    for (int j = i; j < i + opt.step_count; ++j) {
      vector_ids.push_back(j);
    }

    SendBatchVectorAdd(opt.region_id, opt.dimension, vector_ids, vector_datas, total_count, !opt.without_scalar,
                       !opt.without_table, opt.scalar_filter_key, opt.scalar_filter_value, opt.scalar_filter_key2,
                       opt.scalar_filter_value2);

    total_count += vector_ids.size();

    vector_ids.clear();
  }
}

void SendVectorDelete(VectorDeleteOptions const& opt) {  // NOLINT
  dingodb::pb::index::VectorDeleteRequest request;
  dingodb::pb::index::VectorDeleteResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (int i = 0; i < opt.count; ++i) {
    request.add_ids(i + opt.start_id);
  }

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorDelete", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }
  if (Pretty::ShowError(response.error())) {
    return;
  }
  std::cout << fmt::format("VectorDelete response success count: {} fail count: {}", success_count,
                           response.key_states().size() - success_count)
            << std::endl;
}

void SendVectorGetMaxId(VectorGetMaxIdOptions const& opt) {  // NOLINT
  dingodb::pb::index::VectorGetBorderIdRequest request;
  dingodb::pb::index::VectorGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorGetBorderId", request, response);

  Pretty::Show(response);
}

void SendVectorGetMinId(VectorGetMinIdOptions const& opt) {  // NOLINT
  dingodb::pb::index::VectorGetBorderIdRequest request;
  dingodb::pb::index::VectorGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_get_min(true);
  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorGetBorderId", request, response);

  Pretty::Show(response);
}

void SendVectorAddBatch(VectorAddBatchOptions const& opt) {
  if (opt.step_count == 0) {
    std::cout << "step_count must be greater than 0" << std::endl;
    return;
  }
  if (opt.region_id == 0) {
    std::cout << "region_id must be greater than 0" << std::endl;
    return;
  }
  if (opt.dimension == 0) {
    std::cout << "dimension must be greater than 0" << std::endl;
    return;
  }
  if (opt.count == 0) {
    std::cout << "count must be greater than 0" << std::endl;
    return;
  }
  if (opt.start_id < 0) {
    std::cout << "start_id must be greater than 0" << std::endl;
    return;
  }
  if (opt.vector_index_add_cost_file.empty()) {
    std::cout << "vector_index_add_cost_file must not be empty" << std::endl;
    return;
  }

  std::filesystem::path url(opt.vector_index_add_cost_file);
  std::fstream out;
  if (!std::filesystem::exists(url)) {
    // not exist
    out.open(opt.vector_index_add_cost_file, std::ios::out | std::ios::binary);
  } else {
    out.open(opt.vector_index_add_cost_file, std::ios::out | std::ios::binary | std::ios::trunc);
  }

  if (!out.is_open()) {
    std::cout << fmt::format("{} open failed", opt.vector_index_add_cost_file) << std::endl;
    out.close();
    return;
  }

  out << "index,cost(us)\n";
  int64_t total = 0;

  if (opt.count % opt.step_count != 0) {
    std::cout << fmt::format("count {} must be divisible by step_count {}", opt.count, opt.step_count);
    return;
  }

  uint32_t cnt = opt.count / opt.step_count;

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  std::vector<float> random_seeds;
  random_seeds.resize(opt.count * opt.dimension);
  for (uint32_t i = 0; i < opt.count; ++i) {
    for (uint32_t j = 0; j < opt.dimension; ++j) {
      random_seeds[i * opt.dimension + j] = distrib(rng);
    }

    if (i % 10000 == 0) {
      std::cout << fmt::format("generate random seeds: {}/{}", i, opt.count) << std::endl;
    }
  }

  // Add data to index
  // uint32_t num_threads = std::thread::hardware_concurrency();
  // try {
  //   ParallelFor(0, count, num_threads, [&](size_t row, size_t /*thread_id*/) {
  //     std::mt19937 rng;
  //     std::uniform_real_distribution<> distrib(0.0, 10.0);
  //     for (uint32_t i = 0; i < dimension; ++i) {
  //       random_seeds[row * dimension + i] = distrib(rng);
  //     }
  //   });
  // } catch (std::runtime_error& e) {
  //   std::cout << "generate random data failed, error=" << e.what();
  //   return;
  // }

  std::cout << fmt::format("generate random seeds: {}/{}", opt.count, opt.count) << std::endl;

  for (uint32_t x = 0; x < cnt; x++) {
    auto start = std::chrono::steady_clock::now();
    {
      dingodb::pb::index::VectorAddRequest request;
      dingodb::pb::index::VectorAddResponse response;

      *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

      int64_t real_start_id = opt.start_id + x * opt.step_count;
      for (int i = real_start_id; i < real_start_id + opt.step_count; ++i) {
        auto* vector_with_id = request.add_vectors();
        vector_with_id->set_id(i);
        vector_with_id->mutable_vector()->set_dimension(opt.dimension);
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        for (int j = 0; j < opt.dimension; j++) {
          vector_with_id->mutable_vector()->add_float_values(random_seeds[(i - opt.start_id) * opt.dimension + j]);
        }

        if (!opt.without_scalar) {
          for (int k = 0; k < 3; k++) {
            ::dingodb::pb::common::ScalarValue scalar_value;
            int index = k + (i < 30 ? 0 : 1);
            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            field->set_string_data("value" + std::to_string(index));

            vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert(
                {"key" + std::to_string(index), scalar_value});
          }
        }
      }

      butil::Status status =
          InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorAdd", request, response);
      int success_count = 0;
      for (auto key_state : response.key_states()) {
        if (key_state) {
          ++success_count;
        }
      }
    }

    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    out << x << " , " << diff << "\n";

    std::cout << "index : " << x << " : " << diff << " us, avg : " << static_cast<long double>(diff) / opt.step_count
              << " us";

    total += diff;
  }

  std::cout << fmt::format("total : {} cost : {} (us) avg : {} us", opt.count, total,
                           static_cast<long double>(total) / opt.count)
            << std::endl;

  out.close();
}

void SendVectorAddBatchDebug(VectorAddBatchDebugOptions const& opt) {
  if (opt.step_count == 0) {
    std::cout << "step_count must be greater than 0" << std::endl;
    return;
  }
  if (opt.region_id == 0) {
    std::cout << "region_id must be greater than 0" << std::endl;
    return;
  }
  if (opt.dimension == 0) {
    std::cout << "dimension must be greater than 0" << std::endl;
    return;
  }
  if (opt.count == 0) {
    std::cout << "count must be greater than 0" << std::endl;
    return;
  }
  if (opt.start_id < 0) {
    std::cout << "start_id must be greater than 0" << std::endl;
    return;
  }
  if (opt.vector_index_add_cost_file.empty()) {
    std::cout << "vector_index_add_cost_file must not be empty" << std::endl;
    return;
  }

  std::filesystem::path url(opt.vector_index_add_cost_file);
  std::fstream out;
  if (!std::filesystem::exists(url)) {
    // not exist
    out.open(opt.vector_index_add_cost_file, std::ios::out | std::ios::binary);
  } else {
    out.open(opt.vector_index_add_cost_file, std::ios::out | std::ios::binary | std::ios::trunc);
  }

  if (!out.is_open()) {
    std::cout << fmt::format("{} open failed", opt.vector_index_add_cost_file) << std::endl;
    out.close();
    return;
  }

  out << "index,cost(us)\n";
  int64_t total = 0;

  if (opt.count % opt.step_count != 0) {
    std::cout << fmt::format("count {} must be divisible by step_count {}", opt.count, opt.step_count) << std::endl;
    return;
  }

  uint32_t cnt = opt.count / opt.step_count;

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  std::vector<float> random_seeds;
  random_seeds.resize(opt.count * opt.dimension);
  for (uint32_t i = 0; i < opt.count; ++i) {
    for (uint32_t j = 0; j < opt.dimension; ++j) {
      random_seeds[i * opt.dimension + j] = distrib(rng);
    }

    if (i % 10000 == 0) {
      DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", i, opt.count) << std::endl;
    }
  }

  // Add data to index
  // uint32_t num_threads = std::thread::hardware_concurrency();
  // try {
  //   ParallelFor(0, count, num_threads, [&](size_t row, size_t /*thread_id*/) {
  //     std::mt19937 rng;
  //     std::uniform_real_distribution<> distrib(0.0, 10.0);
  //     for (uint32_t i = 0; i < dimension; ++i) {
  //       random_seeds[row * dimension + i] = distrib(rng);
  //     }
  //   });
  // } catch (std::runtime_error& e) {
  //   std::cout << "generate random data failed, error=" << e.what();
  //   return;
  // }

  for (uint32_t x = 0; x < cnt; x++) {
    auto start = std::chrono::steady_clock::now();
    {
      dingodb::pb::index::VectorAddRequest request;
      dingodb::pb::index::VectorAddResponse response;

      *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

      int64_t real_start_id = opt.start_id + x * opt.step_count;
      for (int i = real_start_id; i < real_start_id + opt.step_count; ++i) {
        auto* vector_with_id = request.add_vectors();
        vector_with_id->set_id(i);
        vector_with_id->mutable_vector()->set_dimension(opt.dimension);
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        for (int j = 0; j < opt.dimension; j++) {
          vector_with_id->mutable_vector()->add_float_values(random_seeds[(i - opt.start_id) * opt.dimension + j]);
        }

        if (!opt.without_scalar) {
          auto index = (i - opt.start_id);
          auto left = index % 200;

          auto lambda_insert_scalar_data_function = [&vector_with_id](int tag) {
            ::dingodb::pb::common::ScalarValue scalar_value;

            scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
            ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
            field->set_string_data("tag" + std::to_string(tag));

            vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert(
                {"tag" + std::to_string(tag), scalar_value});
          };

          if (left >= 0 && left < 99) {  // tag1 [0, 99] total 100
            lambda_insert_scalar_data_function(1);
          } else if (left >= 100 && left <= 139) {  // tag2 [100, 139]  total 40
            lambda_insert_scalar_data_function(2);
          } else if (left >= 140 && left <= 149) {  // tag3 [140, 149]  total 10
            lambda_insert_scalar_data_function(3);
          } else if (left >= 150 && left <= 154) {  // tag4 [150, 154]  total 5
            lambda_insert_scalar_data_function(4);
          } else if (left >= 155 && left <= 156) {  // tag5 [155, 156]  total 2
            lambda_insert_scalar_data_function(5);
          } else if (left >= 157 && left <= 157) {  // tag6 [157, 157]  total 1
            lambda_insert_scalar_data_function(6);
          } else {
            // do nothing
          }
        }
      }

      butil::Status status =
          InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorAdd", request, response);
      int success_count = 0;
      for (auto key_state : response.key_states()) {
        if (key_state) {
          ++success_count;
        }
      }
    }

    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    out << x << " , " << diff << "\n";

    DINGO_LOG(INFO) << "index : " << x << " : " << diff
                    << " us, avg : " << static_cast<long double>(diff) / opt.step_count << " us" << std::endl;

    total += diff;
  }

  DINGO_LOG(INFO) << fmt::format("total : {} cost : {} (us) avg : {} us", opt.count, total,
                                 static_cast<long double>(total) / opt.count)
                  << std::endl;

  out.close();
}

// vector_data1: "1.0, 2.0, 3.0, 4.0"
// vector_data2: "1.1, 2.0, 3.0, 4.1"
void SendCalcDistance(CalcDistanceOptions const& opt) {
  if (opt.vector_data1.empty() || opt.vector_data2.empty()) {
    std::cout << "vector_data1 or vector_data2 is empty";
    return;
  }

  std::vector<float> x_i = dingodb::Helper::StringToVector(opt.vector_data1);
  std::vector<float> y_j = dingodb::Helper::StringToVector(opt.vector_data2);

  if (x_i.size() != y_j.size() || x_i.empty() || y_j.empty()) {
    std::cout << "vector_data1 size must be equal to vector_data2 size";
    return;
  }

  auto dimension = x_i.size();

  auto faiss_l2 = dingodb::Helper::DingoFaissL2sqr(x_i.data(), y_j.data(), dimension);
  auto faiss_ip = dingodb::Helper::DingoFaissInnerProduct(x_i.data(), y_j.data(), dimension);
  auto hnsw_l2 = dingodb::Helper::DingoHnswL2Sqr(x_i.data(), y_j.data(), dimension);
  auto hnsw_ip = dingodb::Helper::DingoHnswInnerProduct(x_i.data(), y_j.data(), dimension);
  auto hnsw_ip_dist = dingodb::Helper::DingoHnswInnerProductDistance(x_i.data(), y_j.data(), dimension);

  DINGO_LOG(INFO) << "vector_data1: " << opt.vector_data1;
  DINGO_LOG(INFO) << " vector_data2: " << opt.vector_data2;

  DINGO_LOG(INFO) << "[faiss_l2: " << faiss_l2 << ", faiss_ip: " << faiss_ip << ", hnsw_l2: " << hnsw_l2
                  << ", hnsw_ip: " << hnsw_ip << ", hnsw_ip_dist: " << hnsw_ip_dist << "]";
}

void SendVectorCalcDistance(VectorCalcDistanceOptions const& opt) {
  ::dingodb::pb::index::VectorCalcDistanceRequest request;
  ::dingodb::pb::index::VectorCalcDistanceResponse response;

  if (opt.dimension == 0) {
    std::cout << "step_count must be greater than 0" << std::endl;
    return;
  }

  std::string real_alg_type = opt.alg_type;
  std::string real_metric_type = opt.metric_type;

  std::transform(real_alg_type.begin(), real_alg_type.end(), real_alg_type.begin(), ::tolower);
  std::transform(real_metric_type.begin(), real_metric_type.end(), real_metric_type.begin(), ::tolower);

  bool is_faiss = ("faiss" == real_alg_type);
  bool is_hnsw = ("hnsw" == real_alg_type);

  // if (!is_faiss && !is_hnsw) {
  //   std::cout << "invalid alg_type :  use faiss or hnsw!!!";
  //   return;
  // }

  bool is_l2 = ("l2" == real_metric_type);
  bool is_ip = ("ip" == real_metric_type);
  bool is_cosine = ("cosine" == real_metric_type);
  // if (!is_l2 && !is_ip && !is_cosine) {
  //   std::cout << "invalid metric_type :  use L2 or IP or cosine !!!";
  //   return;
  // }

  // if (left_vector_size <= 0) {
  //   std::cout << "left_vector_size <=0 : " << left_vector_size;
  //   return;
  // }

  // if (right_vector_size <= 0) {
  //   std::cout << "right_vector_size <=0 : " << left_vector_size;
  //   return;
  // }

  dingodb::pb::index::AlgorithmType algorithm_type = dingodb::pb::index::AlgorithmType::ALGORITHM_NONE;
  if (is_faiss) {
    algorithm_type = dingodb::pb::index::AlgorithmType::ALGORITHM_FAISS;
  }
  if (is_hnsw) {
    algorithm_type = dingodb::pb::index::AlgorithmType::ALGORITHM_HNSWLIB;
  }

  dingodb::pb::common::MetricType my_metric_type = dingodb::pb::common::MetricType::METRIC_TYPE_NONE;
  if (is_l2) {
    my_metric_type = dingodb::pb::common::MetricType::METRIC_TYPE_L2;
  }
  if (is_ip) {
    my_metric_type = dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
  }
  if (is_cosine) {
    my_metric_type = dingodb::pb::common::MetricType::METRIC_TYPE_COSINE;
  }
  google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_left_vectors;
  google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector> op_right_vectors;

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib;

  // op left assignment
  for (size_t i = 0; i < opt.left_vector_size; i++) {
    ::dingodb::pb::common::Vector op_left_vector;
    for (uint32_t i = 0; i < opt.dimension; i++) {
      op_left_vector.add_float_values(distrib(rng));
    }
    op_left_vectors.Add(std::move(op_left_vector));
  }

  // op right assignment
  for (size_t i = 0; i < opt.right_vector_size; i++) {
    ::dingodb::pb::common::Vector op_right_vector;
    for (uint32_t i = 0; i < opt.dimension; i++) {
      op_right_vector.add_float_values(distrib(rng));
    }
    op_right_vectors.Add(std::move(op_right_vector));  // NOLINT
  }

  request.set_algorithm_type(algorithm_type);
  request.set_metric_type(my_metric_type);
  request.set_is_return_normlize(opt.is_return_normlize);
  request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
  request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

  InteractionManager::GetInstance().SendRequestWithoutContext("UtilService", "VectorCalcDistance", request, response);
  Pretty::Show(response);
}

int64_t SendVectorCount(VectorCountOptions const& opt, bool show) {
  ::dingodb::pb::index::VectorCountRequest request;
  ::dingodb::pb::index::VectorCountResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  if (opt.start_id > 0) {
    request.set_vector_id_start(opt.start_id);
  }
  if (opt.end_id > 0) {
    request.set_vector_id_end(opt.end_id);
  }

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorCount", request, response);
  if (show) {
    Pretty::Show(response);
  }
  return response.count();
}
void CountVectorTable(CountVectorTableOptions const& opt) {
  auto index_range = SendGetIndexRange(opt.table_id);

  int64_t total_count = 0;
  std::map<std::string, dingodb::pb::meta::RangeDistribution> region_map;
  for (const auto& region_range : index_range.range_distribution()) {
    if (region_range.range().start_key() >= region_range.range().end_key()) {
      std::cout << fmt::format("Invalid region {} range [{}-{})", region_range.id().entity_id(),
                               dingodb::Helper::StringToHex(region_range.range().start_key()),
                               dingodb::Helper::StringToHex(region_range.range().end_key()));
      continue;
    }
    VectorCountOptions opt;
    opt.region_id = region_range.id().entity_id();
    opt.end_id = 0;
    opt.start_id = 0;
    int64_t count = SendVectorCount(opt, false);
    total_count += count;
  }
  Pretty::ShowTotalCount(total_count);
}

// We cant use FLAGS_vector_data to define the vector we want to search, the format is:
// 1.0, 2.0, 3.0, 4.0
// only one vector data, no new line at the end of the file, and only float value and , is allowed

void SendVectorSearch(VectorSearchOptions const& opt) {
  dingodb::pb::index::VectorSearchRequest request;
  dingodb::pb::index::VectorSearchResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  auto* vector = request.add_vector_with_ids();

  if (opt.region_id == 0) {
    std::cout << "region_id is 0" << std::endl;
    return;
  }

  if (opt.dimension == 0) {
    std::cout << "dimension is 0" << std::endl;
    return;
  }

  if (opt.topn == 0) {
    std::cout << "topn is 0" << std::endl;
    return;
  }

  if (opt.vector_data.empty()) {
    for (int i = 0; i < opt.dimension; i++) {
      vector->mutable_vector()->add_float_values(1.0 * i);
    }
  } else {
    std::vector<float> row = dingodb::Helper::StringToVector(opt.vector_data);

    CHECK(opt.dimension == row.size()) << "dimension not match" << std::endl;

    for (auto v : row) {
      vector->mutable_vector()->add_float_values(v);
    }
  }

  request.mutable_parameter()->set_top_n(opt.topn);

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < 20; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 100000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    // std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    // for (auto id : request.parameter().vector_ids()) {
    //   std::cout << id << " ";
    // }
    // std::cout << "]";
    // std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    if (opt.scalar_filter_key.empty() || opt.scalar_filter_value.empty()) {
      std::cout << "scalar_filter_key or scalar_filter_value is empty" << std::endl;
      return;
    }

    auto lambda_cmd_set_key_value_function = [](dingodb::pb::common::VectorScalardata& scalar_data,
                                                const std::string& cmd_key, const std::string& cmd_value) {
      // bool
      if ("speedup_key_bool" == cmd_key || "key_bool" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        bool value = (cmd_value == "true");
        field->set_bool_data(value);
        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // int
      if ("speedup_key_int" == cmd_key || "key_int" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        int value = std::stoi(cmd_value);
        field->set_int_data(value);
        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // long
      if ("speedup_key_long" == cmd_key || "key_long" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        int64_t value = std::stoll(cmd_value);
        field->set_long_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // float
      if ("speedup_key_float" == cmd_key || "key_float" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        float value = std::stof(cmd_value);
        field->set_float_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // double
      if ("speedup_key_double" == cmd_key || "key_double" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        double value = std::stod(cmd_value);
        field->set_double_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // string
      if ("speedup_key_string" == cmd_key || "key_string" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        std::string value = cmd_value;
        field->set_string_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }

      // bytes
      if ("speedup_key_bytes" == cmd_key || "key_bytes" == cmd_key) {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        std::string value = cmd_value;
        field->set_bytes_data(value);

        scalar_data.mutable_scalar_data()->insert({cmd_key, scalar_value});
      }
    };

    if (!opt.scalar_filter_key.empty()) {
      if (std::string::size_type idx = opt.scalar_filter_key.find("key"); idx != std::string::npos) {
        lambda_cmd_set_key_value_function(*vector->mutable_scalar_data(), opt.scalar_filter_key,
                                          opt.scalar_filter_value);

      } else {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        field->set_string_data(opt.scalar_filter_value);

        vector->mutable_scalar_data()->mutable_scalar_data()->insert({opt.scalar_filter_key, scalar_value});
      }
    }

    if (!opt.scalar_filter_key2.empty()) {
      if (std::string::size_type idx = opt.scalar_filter_key2.find("key"); idx != std::string::npos) {
        lambda_cmd_set_key_value_function(*vector->mutable_scalar_data(), opt.scalar_filter_key2,
                                          opt.scalar_filter_value2);
      } else {
        dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
        dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        field->set_string_data(opt.scalar_filter_value2);

        vector->mutable_scalar_data()->mutable_scalar_data()->insert({opt.scalar_filter_key2, scalar_value});
      }
    }
  }

  if (opt.with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);
    if (opt.scalar_filter_key.empty() || opt.scalar_filter_value.empty()) {
      std::cout << "scalar_filter_key or scalar_filter_value is empty" << std::endl;
      return;
    }

    if (!opt.scalar_filter_key.empty()) {
      dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.scalar_filter_value);

      vector->mutable_scalar_data()->mutable_scalar_data()->insert({opt.scalar_filter_key, scalar_value});
    }

    if (!opt.scalar_filter_key2.empty()) {
      dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
      dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.scalar_filter_value2);

      vector->mutable_scalar_data()->mutable_scalar_data()->insert({opt.scalar_filter_key2, scalar_value});
    }
  }

  if (opt.with_table_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::TABLE_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);
  }

  if (opt.ef_search > 0) {
    request.mutable_parameter()->mutable_hnsw()->set_efsearch(opt.ef_search);
  }

  if (opt.bruteforce) {
    request.mutable_parameter()->set_use_brute_force(opt.bruteforce);
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
  }

  std::cout << "VectorSearch response: " << response.DebugString();

  std::ofstream file;
  if (!opt.csv_output.empty()) {
    file = std::ofstream(opt.csv_output, std::ios::out | std::ios::app);
    if (!file.is_open()) {
      std::cout << "open file failed" << std::endl;
      return;
    }
  }

  for (const auto& batch_result : response.batch_results()) {
    DINGO_LOG(INFO) << "VectorSearch response, batch_result_dist_size: " << batch_result.vector_with_distances_size();

    for (const auto& vector_with_distance : batch_result.vector_with_distances()) {
      std::string vector_string = dingodb::Helper::VectorToString(
          dingodb::Helper::PbRepeatedToVector(vector_with_distance.vector_with_id().vector().float_values()));

      DINGO_LOG(INFO) << "vector_id: " << vector_with_distance.vector_with_id().id() << ", vector: [" << vector_string
                      << "]";

      if (!opt.csv_output.empty()) {
        file << vector_string << '\n';
      }
    }

    if (!opt.without_vector) {
      for (const auto& vector_with_distance : batch_result.vector_with_distances()) {
        std::vector<float> x_i = dingodb::Helper::PbRepeatedToVector(vector->vector().float_values());
        std::vector<float> y_j =
            dingodb::Helper::PbRepeatedToVector(vector_with_distance.vector_with_id().vector().float_values());

        auto faiss_l2 = dingodb::Helper::DingoFaissL2sqr(x_i.data(), y_j.data(), opt.dimension);
        auto faiss_ip = dingodb::Helper::DingoFaissInnerProduct(x_i.data(), y_j.data(), opt.dimension);
        auto hnsw_l2 = dingodb::Helper::DingoHnswL2Sqr(x_i.data(), y_j.data(), opt.dimension);
        auto hnsw_ip = dingodb::Helper::DingoHnswInnerProduct(x_i.data(), y_j.data(), opt.dimension);
        auto hnsw_ip_dist = dingodb::Helper::DingoHnswInnerProductDistance(x_i.data(), y_j.data(), opt.dimension);

        DINGO_LOG(INFO) << "vector_id: " << vector_with_distance.vector_with_id().id()
                        << ", distance: " << vector_with_distance.distance() << ", [faiss_l2: " << faiss_l2
                        << ", faiss_ip: " << faiss_ip << ", hnsw_l2: " << hnsw_l2 << ", hnsw_ip: " << hnsw_ip
                        << ", hnsw_ip_dist: " << hnsw_ip_dist << "]";
      }
    }
  }

  if (file.is_open()) {
    file.close();
  }

  // match compare
  if (opt.with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';

    std::vector<int64_t> result_vt_ids;
    for (const auto& vector_with_distance_result : response.batch_results()) {
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }
    }

    if (result_vt_ids.empty()) {
      std::cout << "result_vt_ids : empty" << '\n';
    } else {
      std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
      for (auto id : result_vt_ids) {
        std::cout << id << " ";
      }
      std::cout << "]";
      std::cout << '\n';
    }

    for (auto id : result_vt_ids) {
      auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
      if (iter == vt_ids.end()) {
        std::cout << "result_vector_ids not all in vector_ids" << '\n';
        return;
      }
    }
    std::cout << "result_vector_ids  all in vector_ids" << '\n';
  }

  if (opt.with_scalar_pre_filter || opt.with_scalar_post_filter) {
    std::vector<int64_t> result_vt_ids;
    for (const auto& vector_with_distance_result : response.batch_results()) {
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }
    }

    auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
      std::cout << name << ": " << result_vt_ids.size() << " [ ";
      for (auto id : result_vt_ids) {
        std::cout << id << " ";
      }
      std::cout << "]";
      std::cout << '\n';
    };

    lambda_print_result_vector_function("before sort result_vt_ids");

    std::sort(result_vt_ids.begin(), result_vt_ids.end());

    lambda_print_result_vector_function("after  sort result_vt_ids");
  }
}

void SendVectorSearchDebug(VectorSearchDebugOptions const& opt) {
  dingodb::pb::index::VectorSearchDebugRequest request;
  dingodb::pb::index::VectorSearchDebugResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  if (opt.region_id == 0) {
    std::cout << "region_id is 0" << std::endl;
    return;
  }

  if (opt.dimension == 0) {
    std::cout << "dimension is 0" << std::endl;
    return;
  }

  if (opt.topn == 0) {
    std::cout << "topn is 0" << std::endl;
    return;
  }

  if (opt.batch_count == 0) {
    std::cout << "batch_count is 0" << std::endl;
    return;
  }

  if (opt.start_vector_id > 0) {
    for (int count = 0; count < opt.batch_count; count++) {
      auto* add_vector_with_id = request.add_vector_with_ids();
      add_vector_with_id->set_id(opt.start_vector_id + count);
    }
  } else {
    std::random_device seed;
    std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(0, 100);

    for (int count = 0; count < opt.batch_count; count++) {
      auto* vector = request.add_vector_with_ids()->mutable_vector();
      for (int i = 0; i < opt.dimension; i++) {
        auto random = static_cast<double>(distrib(engine)) / 10.123;
        vector->add_float_values(random);
      }
    }

    request.mutable_parameter()->set_top_n(opt.topn);
  }

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < opt.vector_ids_count; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 1000000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({opt.key, scalar_value});
    }
  }

  if (opt.with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({opt.key, scalar_value});
    }
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearchDebug", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearchDebug  span: {} (us)", diff);

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearchDebug", request, response);
  }

  std::cout << "VectorSearchDebug response: " << response.DebugString() << std::endl;

  std::cout << "VectorSearchDebug response, batch_result_size: " << response.batch_results_size() << std::endl;
  for (const auto& batch_result : response.batch_results()) {
    std::cout << "VectorSearchDebug response, batch_result_dist_size: " << batch_result.vector_with_distances_size()
              << std::endl;
  }

#if 0  // NOLINT
  // match compare
  if (FLAGS_with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << std::endl;

    std::cout << "response.batch_results() size : " << response.batch_results().size() << std::endl;

    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      if (result_vt_ids.empty()) {
        std::cout << "result_vt_ids : empty" << std::endl;
      } else {
        std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << std::endl;
      }

      for (auto id : result_vt_ids) {
        auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
        if (iter == vt_ids.end()) {
          std::cout << "result_vector_ids not all in vector_ids" << std::endl;
          return;
        }
      }
      std::cout << "result_vector_ids  all in vector_ids" << std::endl;
    }
  }

  if (FLAGS_with_scalar_pre_filter) {
    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
        std::cout << name << ": " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << std::endl;
      };

      lambda_print_result_vector_function("before sort result_vt_ids");

      std::sort(result_vt_ids.begin(), result_vt_ids.end());

      lambda_print_result_vector_function("after  sort result_vt_ids");

      std::cout << std::endl;
    }
  }
#endif
}

void SendVectorRangeSearch(VectorRangeSearchOptions const& opt) {
  dingodb::pb::index::VectorSearchRequest request;
  dingodb::pb::index::VectorSearchResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  auto* vector = request.add_vector_with_ids();

  if (opt.region_id == 0) {
    std::cout << "region_id is 0" << std::endl;
    return;
  }

  if (opt.dimension == 0) {
    std::cout << "dimension is 0" << std::endl;
    return;
  }

  for (int i = 0; i < opt.dimension; i++) {
    vector->mutable_vector()->add_float_values(1.0 * i);
  }

  request.mutable_parameter()->set_top_n(0);
  request.mutable_parameter()->set_enable_range_search(true);
  request.mutable_parameter()->set_radius(opt.radius);

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < 20; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 100000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int k = 0; k < 2; k++) {
      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data("value" + std::to_string(k));

      vector->mutable_scalar_data()->mutable_scalar_data()->insert({"key" + std::to_string(k), scalar_value});
    }
  }

  if (opt.with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);

    for (int k = 0; k < 2; k++) {
      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data("value" + std::to_string(k));

      vector->mutable_scalar_data()->mutable_scalar_data()->insert({"key" + std::to_string(k), scalar_value});
    }
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearch  span: {} (us)", diff);

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
  }

  std::cout << "VectorSearch response: " << response.DebugString() << std::endl;

  // match compare
  if (opt.with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';

    std::vector<int64_t> result_vt_ids;
    for (const auto& vector_with_distance_result : response.batch_results()) {
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }
    }

    if (result_vt_ids.empty()) {
      std::cout << "result_vt_ids : empty" << '\n';
    } else {
      std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
      for (auto id : result_vt_ids) {
        std::cout << id << " ";
      }
      std::cout << "]";
      std::cout << '\n';
    }

    for (auto id : result_vt_ids) {
      auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
      if (iter == vt_ids.end()) {
        std::cout << "result_vector_ids not all in vector_ids" << '\n';
        return;
      }
    }
    std::cout << "result_vector_ids  all in vector_ids" << '\n';
  }

  if (opt.with_scalar_pre_filter || opt.with_scalar_post_filter) {
    std::vector<int64_t> result_vt_ids;
    for (const auto& vector_with_distance_result : response.batch_results()) {
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }
    }

    auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
      std::cout << name << ": " << result_vt_ids.size() << " [ ";
      for (auto id : result_vt_ids) {
        std::cout << id << " ";
      }
      std::cout << "]";
      std::cout << '\n';
    };

    lambda_print_result_vector_function("before sort result_vt_ids");

    std::sort(result_vt_ids.begin(), result_vt_ids.end());

    lambda_print_result_vector_function("after  sort result_vt_ids");
  }
}
void SendVectorRangeSearchDebug(VectorRangeSearchDebugOptions const& opt) {
  dingodb::pb::index::VectorSearchDebugRequest request;
  dingodb::pb::index::VectorSearchDebugResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  if (opt.region_id == 0) {
    std::cout << "region_id is 0" << std::endl;
    return;
  }

  if (opt.dimension == 0) {
    std::cout << "dimension is 0" << std::endl;
    return;
  }

  if (opt.batch_count == 0) {
    std::cout << "batch_count is 0" << std::endl;
    return;
  }

  if (opt.start_vector_id > 0) {
    for (int count = 0; count < opt.batch_count; count++) {
      auto* add_vector_with_id = request.add_vector_with_ids();
      add_vector_with_id->set_id(opt.start_vector_id + count);
    }
  } else {
    std::random_device seed;
    std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(0, 100);

    for (int count = 0; count < opt.batch_count; count++) {
      auto* vector = request.add_vector_with_ids()->mutable_vector();
      for (int i = 0; i < opt.dimension; i++) {
        auto random = static_cast<double>(distrib(engine)) / 10.123;
        vector->add_float_values(random);
      }
    }

    request.mutable_parameter()->set_top_n(0);
  }

  request.mutable_parameter()->set_enable_range_search(true);
  request.mutable_parameter()->set_radius(opt.radius);

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < opt.vector_ids_count; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 1000000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({opt.key, scalar_value});
    }
  }

  if (opt.with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(opt.value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({opt.key, scalar_value});
    }
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearchDebug", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearchDebug  span: {} (us)", diff);

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearchDebug", request, response);
  }

  std::cout << "VectorSearchDebug response: " << response.DebugString() << std::endl;

  std::cout << "VectorSearchDebug response, batch_result_size: " << response.batch_results_size() << std::endl;
  for (const auto& batch_result : response.batch_results()) {
    std::cout << "VectorSearchDebug response, batch_result_dist_size: " << batch_result.vector_with_distances_size()
              << std::endl;
  }

#if 0  // NOLINT
  // match compare
  if (FLAGS_with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << std::endl;

    std::cout << "response.batch_results() size : " << response.batch_results().size() << std::endl;

    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      if (result_vt_ids.empty()) {
        std::cout << "result_vt_ids : empty" << std::endl;
      } else {
        std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << std::endl;
      }

      for (auto id : result_vt_ids) {
        auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
        if (iter == vt_ids.end()) {
          std::cout << "result_vector_ids not all in vector_ids" << std::endl;
          return;
        }
      }
      std::cout << "result_vector_ids  all in vector_ids" << std::endl;
    }
  }

  if (FLAGS_with_scalar_pre_filter) {
    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
        std::cout << name << ": " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << std::endl;
      };

      lambda_print_result_vector_function("before sort result_vt_ids");

      std::sort(result_vt_ids.begin(), result_vt_ids.end());

      lambda_print_result_vector_function("after  sort result_vt_ids");

      std::cout << std::endl;
    }
  }
#endif
}

void SendVectorBatchSearch(VectorBatchSearchOptions const& opt) {
  dingodb::pb::index::VectorSearchRequest request;
  dingodb::pb::index::VectorSearchResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  if (opt.region_id == 0) {
    std::cout << "region_id is 0" << std::endl;
    return;
  }

  if (opt.dimension == 0) {
    std::cout << "dimension is 0" << std::endl;
    return;
  }

  if (opt.topn == 0) {
    std::cout << "topn is 0" << std::endl;
    return;
  }

  if (opt.batch_count == 0) {
    std::cout << "batch_count is 0" << std::endl;
    return;
  }

  std::random_device seed;
  std::ranlux48 engine(seed());
  std::uniform_int_distribution<> distrib(0, 100);

  for (int count = 0; count < opt.batch_count; count++) {
    auto* vector = request.add_vector_with_ids()->mutable_vector();
    for (int i = 0; i < opt.dimension; i++) {
      auto random = static_cast<double>(distrib(engine)) / 10.123;
      vector->add_float_values(random);
    }
  }

  request.mutable_parameter()->set_top_n(opt.topn);

  if (opt.without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.mutable_parameter()->set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.mutable_parameter()->set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_parameter()->mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  std::vector<int64_t> vt_ids;
  if (opt.with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < 200; i++) {
      std::random_device seed;
      std::ranlux48 engine(seed());

      std::uniform_int_distribution<> distrib(1, 10000ULL);
      auto random = distrib(engine);

      vt_ids.push_back(random);
    }

    sort(vt_ids.begin(), vt_ids.end());
    vt_ids.erase(std::unique(vt_ids.begin(), vt_ids.end()), vt_ids.end());

    for (auto id : vt_ids) {
      request.mutable_parameter()->add_vector_ids(id);
    }

    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';
  }

  if (opt.with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (uint32_t m = 0; m < opt.batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      for (int k = 0; k < 2; k++) {
        ::dingodb::pb::common::ScalarValue scalar_value;
        scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
        ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
        field->set_string_data("value" + std::to_string(k));

        vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({"key" + std::to_string(k), scalar_value});
      }
    }
  }

  if (opt.print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearch  span: {} (us)", diff);

  } else {
    InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorSearch", request, response);
  }

  std::cout << "VectorSearch response: " << response.DebugString() << std::endl;

  std::cout << "VectorSearch response, batch_result_size: " << response.batch_results_size();
  for (const auto& batch_result : response.batch_results()) {
    std::cout << "VectorSearch response, batch_result_dist_size: " << batch_result.vector_with_distances_size()
              << std::endl;
  }

  // match compare
  if (opt.with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';

    std::cout << "response.batch_results() size : " << response.batch_results().size() << '\n';

    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      if (result_vt_ids.empty()) {
        std::cout << "result_vt_ids : empty" << '\n';
      } else {
        std::cout << "result_vt_ids : " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << '\n';
      }

      for (auto id : result_vt_ids) {
        auto iter = std::find(vt_ids.begin(), vt_ids.end(), id);
        if (iter == vt_ids.end()) {
          std::cout << "result_vector_ids not all in vector_ids" << '\n';
          return;
        }
      }
      std::cout << "result_vector_ids  all in vector_ids" << '\n';
    }
  }

  if (opt.with_scalar_pre_filter) {
    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<int64_t> result_vt_ids;
      for (const auto& vector_with_distance : vector_with_distance_result.vector_with_distances()) {
        auto id = vector_with_distance.vector_with_id().id();
        result_vt_ids.push_back(id);
      }

      auto lambda_print_result_vector_function = [&result_vt_ids](const std::string& name) {
        std::cout << name << ": " << result_vt_ids.size() << " [ ";
        for (auto id : result_vt_ids) {
          std::cout << id << " ";
        }
        std::cout << "]";
        std::cout << '\n';
      };

      lambda_print_result_vector_function("before sort result_vt_ids");

      std::sort(result_vt_ids.begin(), result_vt_ids.end());

      lambda_print_result_vector_function("after  sort result_vt_ids");

      std::cout << '\n';
    }
  }
}

void SendVectorBatchQuery(VectorBatchQueryOptions const& opt) {
  dingodb::pb::index::VectorBatchQueryRequest request;
  dingodb::pb::index::VectorBatchQueryResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (auto vector_id : opt.vector_ids) {
    request.add_vector_ids(vector_id);
  }

  if (opt.without_vector) {
    request.set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorBatchQuery", request, response);

  std::cout << "VectorBatchQuery response: " << response.DebugString() << std::endl;
}

void SendVectorScanQuery(VectorScanQueryOptions const& opt) {
  dingodb::pb::index::VectorScanQueryRequest request;
  dingodb::pb::index::VectorScanQueryResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_vector_id_start(opt.start_id);
  request.set_vector_id_end(opt.end_id);

  if (opt.limit > 0) {
    request.set_max_scan_count(opt.limit);
  } else {
    request.set_max_scan_count(10);
  }

  request.set_is_reverse_scan(opt.is_reverse);

  if (opt.without_vector) {
    request.set_without_vector_data(true);
  }

  if (opt.without_scalar) {
    request.set_without_scalar_data(true);
  }

  if (opt.without_table) {
    request.set_without_table_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  if (!opt.scalar_filter_key.empty()) {
    auto* scalar_data = request.mutable_scalar_for_filter()->mutable_scalar_data();
    dingodb::pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    scalar_value.add_fields()->set_string_data(opt.scalar_filter_value);
    (*scalar_data)[opt.scalar_filter_key] = scalar_value;

    request.set_use_scalar_filter(true);

    DINGO_LOG(INFO) << "scalar_filter_key: " << opt.scalar_filter_key
                    << " scalar_filter_value: " << opt.scalar_filter_value;
  }

  if (!opt.scalar_filter_key2.empty()) {
    auto* scalar_data = request.mutable_scalar_for_filter()->mutable_scalar_data();
    dingodb::pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    scalar_value.add_fields()->set_string_data(opt.scalar_filter_value2);
    (*scalar_data)[opt.scalar_filter_key2] = scalar_value;

    request.set_use_scalar_filter(true);

    DINGO_LOG(INFO) << "scalar_filter_key2: " << opt.scalar_filter_key2
                    << " scalar_filter_value2: " << opt.scalar_filter_value2;
  }

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorScanQuery", request, response);

  std::cout << "VectorScanQuery response: " << response.DebugString() << " vector count: " << response.vectors().size()
            << std::endl;
}

butil::Status ScanVectorData(int64_t region_id, int64_t start_id, int64_t end_id, int64_t limit, bool is_reverse,
                             std::vector<std::vector<float>>& vector_datas, int64_t& last_vector_id) {
  dingodb::pb::index::VectorScanQueryRequest request;
  dingodb::pb::index::VectorScanQueryResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(region_id);
  request.set_vector_id_start(start_id);
  request.set_vector_id_end(end_id);
  request.set_max_scan_count(limit);
  request.set_is_reverse_scan(is_reverse);
  request.set_without_vector_data(false);
  request.set_without_scalar_data(true);
  request.set_without_table_data(true);

  auto ret =
      InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorScanQuery", request, response);
  if (!ret.ok()) {
    std::cout << "VectorScanQuery failed: " << ret.error_str();
    return ret;
  }

  if (response.error().errcode() != 0) {
    std::cout << "VectorScanQuery failed: " << response.error().errcode() << " " << response.error().errmsg();
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG(DEBUG) << "VectorScanQuery response: " << response.DebugString()
                   << " vector count: " << response.vectors().size();

  if (response.vectors_size() > 0) {
    for (const auto& vector : response.vectors()) {
      std::vector<float> vector_data;
      vector_data.reserve(vector.vector().float_values_size());
      for (int i = 0; i < vector.vector().float_values_size(); i++) {
        vector_data.push_back(vector.vector().float_values(i));
        if (vector.id() > last_vector_id) {
          last_vector_id = vector.id();
        }
      }
      vector_datas.push_back(vector_data);
    }
  }

  return butil::Status::OK();
}

void SendVectorScanDump(VectorScanDumpOptions const& opt) {
  if (opt.csv_output.empty()) {
    std::cout << "csv_output is empty" << std::endl;
    return;
  }

  std::ofstream file(opt.csv_output, std::ios::out);

  if (!file.is_open()) {
    std::cout << "open file failed" << std::endl;
    return;
  }

  int64_t batch_count = opt.limit > 1000 || opt.limit == 0 ? 1000 : opt.limit;

  int64_t new_start_id = opt.start_id - 1;

  for (;;) {
    std::vector<std::vector<float>> vector_datas;
    int64_t last_vector_id = 0;

    if (new_start_id >= opt.end_id) {
      DINGO_LOG(INFO) << "new_start_id: " << new_start_id << " end_id: " << opt.end_id << ", will break";
      break;
    }

    auto ret = ScanVectorData(opt.region_id, new_start_id + 1, opt.end_id, batch_count, opt.is_reverse, vector_datas,
                              new_start_id);
    if (!ret.ok()) {
      std::cout << "ScanVectorData failed: " << ret.error_str();
      return;
    }

    if (vector_datas.empty()) {
      DINGO_LOG(INFO) << "vector_datas is empty, finish";
      break;
    }

    for (const auto& vector_data : vector_datas) {
      std::string vector_string = dingodb::Helper::VectorToString(vector_data);
      file << vector_string << '\n';
    }

    DINGO_LOG(INFO) << "new_start_id: " << new_start_id << " end_id: " << opt.end_id
                    << " vector_datas.size(): " << vector_datas.size();
  }
}

void SendVectorGetRegionMetrics(VectorGetRegionMetricsOptions const& opt) {
  dingodb::pb::index::VectorGetRegionMetricsRequest request;
  dingodb::pb::index::VectorGetRegionMetricsResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorGetRegionMetrics", request, response);
  Pretty::Show(response);
}

void SetUpCreateIndex(CLI::App& app) {
  auto opt = std::make_shared<CreateIndexOptions>();
  auto* cmd = app.add_subcommand("CreateIndex", "Create index ")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--name", opt->name, "Request parameter region name")->required();
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")->default_val(1);
  cmd->add_option("--with_auto_increment", opt->with_auto_increment, "Request parameter with_auto_increment")
      ->default_val(true)
      ->default_str("true");
  cmd->add_option("--with_scalar_schema", opt->with_scalar_schema, "Request parameter with_scalar_schema")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--replica", opt->replica, "Request parameter replica num, must greater than 0")->default_val(3);
  cmd->add_option("--vector_index_type", opt->vector_index_type,
                  "Request parameter vector_index_type, hnsw|flat|ivf_flat|ivf_pq|diskann");
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension");
  cmd->add_option("--metrics_type", opt->metrics_type, "Request parameter metrics_type, L2|IP|COSINE")->ignore_case();
  cmd->add_option("--max_elements", opt->max_elements, "Request parameter max_elements");
  cmd->add_option("--efconstruction", opt->efconstruction, "Request parameter efconstruction");
  cmd->add_option("--nlinks", opt->nlinks, "Request parameter nlinks");
  cmd->add_option("--ncentroids", opt->ncentroids, "Request parameter ncentroids, ncentroids default 10")
      ->default_val(10);
  cmd->add_option("--nsubvector", opt->nsubvector, "Request parameter nsubvector, ivf pq default subvector nums 8")
      ->default_val(8);
  cmd->add_option("--nbits_per_idx", opt->nbits_per_idx,
                  "Request parameter nbits_per_idx, ivf pq default nbits_per_idx 8")
      ->default_val(8);

  cmd->add_option("--max_degree", opt->max_degree,
                  "diskann the degree of the graph index, typically between 60 and 150.")
      ->default_val(64);
  cmd->add_option("--search_list_size", opt->search_list_size,
                  "diskann the size of search list during index build. Typical values are between 75 to 200.")
      ->default_val(100);
  cmd->callback([opt]() { RunCreateIndex(*opt); });
}

void RunCreateIndex(CreateIndexOptions const& opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateIndexRequest request;
  dingodb::pb::meta::CreateIndexResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  if (opt.schema_id > 0) {
    schema_id->set_entity_id(opt.schema_id);
  }

  uint32_t part_count = opt.part_count;

  std::vector<int64_t> new_ids;
  int ret = client_v2::Helper::GetCreateTableIds(CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta(),
                                                 1 + opt.part_count, new_ids);
  if (ret < 0) {
    std::cout << "GetCreateTableIds failed" << std::endl;
    return;
  }
  if (new_ids.empty()) {
    std::cout << "GetCreateTableIds failed" << std::endl;
    return;
  }
  if (new_ids.size() != 1 + opt.part_count) {
    std::cout << "GetCreateTableIds failed" << std::endl;
    return;
  }

  int64_t new_index_id = new_ids.at(0);

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = new_ids.at(1 + i);
    part_ids.push_back(new_part_id);
  }

  // setup index_id
  auto* index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(schema_id->entity_id());
  index_id->set_entity_id(new_index_id);

  // string name = 1;
  auto* index_definition = request.mutable_index_definition();
  index_definition->set_name(opt.name);

  if (opt.replica > 0) {
    index_definition->set_replica(opt.replica);
  }

  if (opt.with_auto_increment) {
    index_definition->set_with_auto_incrment(true);
    index_definition->set_auto_increment(1024);
  }

  // vector index parameter
  index_definition->mutable_index_parameter()->set_index_type(dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
  auto* vector_index_parameter = index_definition->mutable_index_parameter()->mutable_vector_index_parameter();

  if (opt.vector_index_type == "hnsw") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
  } else if (opt.vector_index_type == "flat") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
  } else if (opt.vector_index_type == "bruteforce") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BRUTEFORCE);
  } else if (opt.vector_index_type == "ivf_flat") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT);
  } else if (opt.vector_index_type == "ivf_pq") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
  } else if (opt.vector_index_type == "diskann") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
  } else {
    std::cout << "vector_index_type is invalid, now only support hnsw and flat" << std::endl;
    return;
  }

  if (opt.dimension == 0) {
    std::cout << "dimension is empty" << std::endl;
    return;
  }

  dingodb::pb::common::MetricType metric_type;

  if (opt.metrics_type == "L2" || opt.metrics_type == "l2") {
    metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_L2;
  } else if (opt.metrics_type == "IP" || opt.metrics_type == "ip") {
    metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
  } else if (opt.metrics_type == "COSINE" || opt.metrics_type == "cosine") {
    metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE;
  } else {
    std::cout << "metrics_type is invalid, now only support L2, IP and COSINE" << std::endl;
    return;
  }

  if (opt.vector_index_type == "hnsw") {
    if (opt.max_elements < 0) {
      std::cout << "max_elements is negative" << std::endl;
      return;
    }
    if (opt.efconstruction == 0) {
      std::cout << "efconstruction is empty" << std::endl;
      return;
    }
    if (opt.nlinks == 0) {
      std::cout << "nlinks is empty" << std::endl;
      return;
    }

    // DINGO_LOG(INFO) << "max_elements=" << opt.max_elements << ", dimension=" << opt.dimension;

    auto* hsnw_index_parameter = vector_index_parameter->mutable_hnsw_parameter();

    hsnw_index_parameter->set_dimension(opt.dimension);
    hsnw_index_parameter->set_metric_type(metric_type);
    hsnw_index_parameter->set_efconstruction(opt.efconstruction);
    hsnw_index_parameter->set_nlinks(opt.nlinks);
    hsnw_index_parameter->set_max_elements(opt.max_elements);
  } else if (opt.vector_index_type == "flat") {
    auto* flat_index_parameter = vector_index_parameter->mutable_flat_parameter();
    flat_index_parameter->set_dimension(opt.dimension);
    flat_index_parameter->set_metric_type(metric_type);
  } else if (opt.vector_index_type == "bruteforce") {
    auto* bruteforce_index_parameter = vector_index_parameter->mutable_bruteforce_parameter();
    bruteforce_index_parameter->set_dimension(opt.dimension);
    bruteforce_index_parameter->set_metric_type(metric_type);
  } else if (opt.vector_index_type == "ivf_flat") {
    auto* ivf_flat_index_parameter = vector_index_parameter->mutable_ivf_flat_parameter();
    ivf_flat_index_parameter->set_dimension(opt.dimension);
    ivf_flat_index_parameter->set_metric_type(metric_type);
    ivf_flat_index_parameter->set_ncentroids(opt.ncentroids);
  } else if (opt.vector_index_type == "ivf_pq") {
    auto* ivf_pq_index_parameter = vector_index_parameter->mutable_ivf_pq_parameter();
    ivf_pq_index_parameter->set_dimension(opt.dimension);
    ivf_pq_index_parameter->set_metric_type(metric_type);
    ivf_pq_index_parameter->set_ncentroids(opt.ncentroids);
    ivf_pq_index_parameter->set_nsubvector(opt.nsubvector);
    ivf_pq_index_parameter->set_nbits_per_idx(opt.nbits_per_idx);
  } else if (opt.vector_index_type == "diskann") {
    auto* diskann_parameter = vector_index_parameter->mutable_diskann_parameter();
    diskann_parameter->set_dimension(opt.dimension);
    diskann_parameter->set_metric_type(metric_type);
    diskann_parameter->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    diskann_parameter->set_max_degree(opt.max_degree);
    diskann_parameter->set_search_list_size(opt.search_list_size);
  }

  index_definition->set_version(1);

  auto* partition_rule = index_definition->mutable_index_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");

  for (int i = 0; i < part_count; i++) {
    auto* part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_index_id);
    part->mutable_range()->set_start_key(client_v2::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client_v2::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  if (opt.with_auto_increment) {
    DINGO_LOG(INFO) << "with_auto_increment" << std::endl;
    index_definition->set_auto_increment(100);
  }

  // scalar key speed up
  if (opt.with_scalar_schema) {
    auto* scalar_parameter = vector_index_parameter->mutable_scalar_schema();

    auto* field = scalar_parameter->add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(false);
  }

  // DINGO_LOG(INFO) << "Request: " << request.DebugString();

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateIndex",
                                                                                                  request, response);
  if (Pretty::ShowError(status)) {
    return;
  }
  std::cout << "create index success, index_id==" << response.index_id().entity_id() << std::endl;
  DINGO_LOG(INFO) << response.DebugString();
}

int64_t SendVectorCountMemory(VectorCountMemoryOptions const& opt) {
  if (opt.region_id <= 0) {
    std::cout << "region_id must be greater than 0" << std::endl;
    return -1;
  }
  dingodb::pb::index::VectorCountMemoryRequest request;
  dingodb::pb::index::VectorCountMemoryResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorCountMemory", request, response);

  DINGO_LOG(INFO) << "VectorCountMemory request: " << request.DebugString();
  DINGO_LOG(INFO) << "VectorCountMemory response: " << response.DebugString();
  return response.error().errcode() != 0 ? 0 : response.count();
}

void SendVectorDump(VectorDumpOptions const& opt) {
  if (opt.region_id <= 0) {
    std::cout << "region_id must be greater than 0" << std::endl;
    return;
  }
  dingodb::pb::index::VectorDumpRequest request;
  dingodb::pb::index::VectorDumpResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  request.set_dump_all(opt.dump_all);

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorDump", request, response);

  std::cout << "VectorDump response: " << std::endl;
}

void SendVectorReset(VectorResetOptions const& opt) {
  if (opt.region_id <= 0) {
    DINGO_LOG(ERROR) << "region_id must be greater than 0" << std::endl;
    return;
  }
  dingodb::pb::index::VectorResetRequest request;
  dingodb::pb::index::VectorResetResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  request.set_delete_data_file(opt.delete_data_file);

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorReset", request, response);

  std::cout << "VectorReset response: " << response.DebugString() << std::endl;
}

void SendVectorStatus(VectorStatusOptions const& opt) {
  if (opt.region_id <= 0) {
    std::cout << "region_id must be greater than 0" << std::endl;
    return;
  }
  dingodb::pb::index::VectorStatusRequest request;
  dingodb::pb::index::VectorStatusResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorStatus", request, response);

  DINGO_LOG(INFO) << "VectorStatus request: " << request.DebugString();
  std::cout << "VectorStatus response: " << response.DebugString() << std::endl;
}

void SendVectorLoad(VectorLoadOptions const& opt) {
  if (opt.region_id <= 0) {
    std::cout << "region_id must be greater than 0" << std::endl;
    return;
  }
  dingodb::pb::index::VectorLoadRequest request;
  dingodb::pb::index::VectorLoadResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  request.mutable_parameter()->mutable_diskann()->set_direct_load_without_build(opt.direct_load_without_build);
  request.mutable_parameter()->mutable_diskann()->set_num_nodes_to_cache(opt.num_nodes_to_cache);
  request.mutable_parameter()->mutable_diskann()->set_warmup(opt.warmup);

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorLoad", request, response);

  std::cout << "VectorLoad response: " << response.DebugString() << std::endl;
}

void SendVectorBuild(VectorBuildOptions const& opt) {
  if (opt.region_id <= 0) {
    std::cout << "region_id must be greater than 0" << std::endl;
    return;
  }
  dingodb::pb::index::VectorBuildRequest request;
  dingodb::pb::index::VectorBuildResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorBuild", request, response);

  DINGO_LOG(INFO) << "VectorBuild request: " << request.DebugString();
  std::cout << "VectorBuild response: " << response.DebugString() << std::endl;
}

void SendVectorImport(VectorImportOptions const& opt) {
  if (opt.step_count == 0) {
    std::cout << "step_count must be greater than 0" << std::endl;
    return;
  }
  if (opt.region_id == 0) {
    std::cout << "region_id must be greater than 0" << std::endl;
    return;
  }
  if (opt.dimension == 0) {
    std::cout << "dimension must be greater than 0" << std::endl;
    return;
  }
  if (opt.count == 0) {
    std::cout << "count must be greater than 0" << std::endl;
    return;
  }
  if (opt.start_id < 0) {
    std::cout << "start_id must be greater than 0" << std::endl;
    return;
  }

  int64_t total = 0;

  if (opt.count % opt.step_count != 0) {
    std::cout << fmt::format("count {} must be divisible by step_count {}", opt.count, opt.step_count);
    return;
  }

  uint32_t cnt = opt.count / opt.step_count;

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  std::vector<float> random_seeds;
  random_seeds.resize(opt.count * opt.dimension);
  for (uint32_t i = 0; i < opt.count; ++i) {
    for (uint32_t j = 0; j < opt.dimension; ++j) {
      random_seeds[i * opt.dimension + j] = distrib(rng);
    }

    if (i % 10000 == 0) {
      DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", i, opt.count);
    }
  }

  DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", opt.count, opt.count);

  for (uint32_t x = 0; x < cnt; x++) {
    auto start = std::chrono::steady_clock::now();
    {
      dingodb::pb::index::VectorImportRequest request;
      dingodb::pb::index::VectorImportResponse response;

      *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

      int64_t real_start_id = opt.start_id + x * opt.step_count;
      for (int64_t i = real_start_id; i < real_start_id + opt.step_count; ++i) {
        if (opt.import_for_add) {  // add
          auto* vector_with_id = request.add_vectors();
          vector_with_id->set_id(i);
          vector_with_id->mutable_vector()->set_dimension(opt.dimension);
          vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
          for (int j = 0; j < opt.dimension; j++) {
            vector_with_id->mutable_vector()->add_float_values(random_seeds[(i - opt.start_id) * opt.dimension + j]);
          }

          if (!opt.without_scalar) {
            for (int k = 0; k < 3; k++) {
              ::dingodb::pb::common::ScalarValue scalar_value;
              int index = k + (i < 30 ? 0 : 1);
              scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
              ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
              field->set_string_data("value" + std::to_string(index));

              vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert(
                  {"key" + std::to_string(index), scalar_value});
            }
          }
        } else {  // delete
          request.add_delete_ids(i);
        }
      }

      butil::Status status =
          InteractionManager::GetInstance().SendRequestWithContext("IndexService", "VectorImport", request, response);
    }

    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << "index : " << x << " : " << diff
                    << " us, avg : " << static_cast<long double>(diff) / opt.step_count << " us";

    total += diff;
  }

  std::cout << fmt::format("total : {} cost : {} (us) avg : {} us", opt.count, total,
                           static_cast<long double>(total) / opt.count)
            << std::endl;
}

void SetUpVectorSearch(CLI::App& app) {
  auto opt = std::make_shared<VectorSearchOptions>();
  auto* cmd = app.add_subcommand("VectorSearch", "Vector search ")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->required();
  cmd->add_option("--topn", opt->topn, "Request parameter topn")->required();
  cmd->add_option("--vector_data", opt->vector_data, "Request parameter vector data");
  cmd->add_option("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--key", opt->key, "Request parameter key");
  cmd->add_option("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--with_table_pre_filter", opt->with_table_pre_filter, "Search vector with table data pre filter")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--scalar_filter_key", opt->scalar_filter_key, "Request scalar_filter_key");
  cmd->add_option("--scalar_filter_value", opt->scalar_filter_value, "Request parameter scalar_filter_value");
  cmd->add_option("--scalar_filter_key2", opt->scalar_filter_key2, "Request parameter scalar_filter_key2");
  cmd->add_option("--scalar_filter_value2", opt->scalar_filter_value2, "Request parameter scalar_filter_value2");
  cmd->add_option("--with_scalar_post_filter", opt->with_scalar_post_filter,
                  "Search vector with scalar data post filter")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--bruteforce", opt->bruteforce, "Use bruteforce search")->default_val(false)->default_str("false");
  cmd->add_option("--print_vector_search_delay", opt->print_vector_search_delay, "print vector search delay")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--ef_search", opt->ef_search, "hnsw index search ef")->default_val(0);
  cmd->add_option("--csv_output", opt->csv_output, "csv output");
  cmd->callback([opt]() { RunVectorSearch(*opt); });
}

void RunVectorSearch(VectorSearchOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendVectorSearch(opt);
}

void SetUpVectorSearchDebug(CLI::App& app) {
  auto opt = std::make_shared<VectorSearchDebugOptions>();
  auto* cmd = app.add_subcommand("VectorSearchDebug", "Vector search debug")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->required();
  cmd->add_option("--topn", opt->topn, "Request parameter topn")->required();
  cmd->add_option("--start_vector_id", opt->start_vector_id, "Request parameter start_vector_id");
  cmd->add_option("--batch_count", opt->batch_count, "Request parameter batch count")->required();
  cmd->add_option("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_flag("--without_scalar", opt->without_scalar, "Search vector without scalar data")->default_val(false);
  cmd->add_flag("--without_table", opt->without_table, "Search vector without table data")->default_val(false);
  cmd->add_option("--key", opt->key, "Request parameter key");
  cmd->add_option("--value", opt->value, "Request parameter value");
  cmd->add_option("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--vector_ids_count", opt->vector_ids_count, "Request parameter vector_ids_count")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->default_str("false");

  cmd->add_option("--with_scalar_post_filter", opt->with_scalar_post_filter,
                  "Search vector with scalar data post filter")
      ->default_val(false)
      ->default_str("false");

  cmd->callback([opt]() { RunVectorSearchDebug(*opt); });
}

void RunVectorSearchDebug(VectorSearchDebugOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendVectorSearchDebug(opt);
}

void SetUpVectorRangeSearch(CLI::App& app) {
  auto opt = std::make_shared<VectorRangeSearchOptions>();
  auto* cmd = app.add_subcommand("VectorRangeSearch", "Vector range search ")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->required();
  cmd->add_option("--radius", opt->radius, "Request parameter radius")->default_val(10.1);
  cmd->add_option("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--key", opt->key, "Request parameter key");
  cmd->add_option("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--with_scalar_post_filter", opt->with_scalar_post_filter,
                  "Search vector with scalar data post filter")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--print_vector_search_delay", opt->print_vector_search_delay, "print vector search delay")
      ->default_val(false)
      ->default_str("false");
  cmd->callback([opt]() { RunVectorRangeSearch(*opt); });
}

void RunVectorRangeSearch(VectorRangeSearchOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendVectorRangeSearch(opt);
}

void SetUpVectorRangeSearchDebug(CLI::App& app) {
  auto opt = std::make_shared<VectorRangeSearchDebugOptions>();
  auto* cmd = app.add_subcommand("VectorRangeSearchDebug", "Vector range search debug")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->required();
  cmd->add_option("--start_vector_id", opt->start_vector_id, "Request parameter start_vector_id");
  cmd->add_option("--batch_count", opt->batch_count, "Request parameter batch_count")->required();
  cmd->add_option("--radius", opt->radius, "Request parameter radius")->default_val(10.1);
  cmd->add_option("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--key", opt->key, "Request parameter key");
  cmd->add_option("--value", opt->value, "Request parameter value");
  cmd->add_option("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--vector_ids_count", opt->vector_ids_count, "Search vector with vector ids count")->default_val(100);
  cmd->add_option("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--with_scalar_post_filter", opt->with_scalar_post_filter,
                  "Search vector with scalar data post filter")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--print_vector_search_delay", opt->print_vector_search_delay, "print vector search delay")
      ->default_val(false)
      ->default_str("false");
  cmd->callback([opt]() { RunVectorRangeSearchDebug(*opt); });
}

void RunVectorRangeSearchDebug(VectorRangeSearchDebugOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendVectorRangeSearchDebug(opt);
}

void SetUpVectorBatchSearch(CLI::App& app) {
  auto opt = std::make_shared<VectorBatchSearchOptions>();
  auto* cmd = app.add_subcommand("VectorBatchSearch", "Vector batch search")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->required();
  cmd->add_option("--topn", opt->topn, "Request parameter topn")->required();
  cmd->add_option("--batch_count", opt->batch_count, "Request parameter batch_count")->required();
  cmd->add_option("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--key", opt->key, "Request parameter key");
  cmd->add_option("--with_vector_ids", opt->with_vector_ids, "Search vector with vector ids list default false")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--with_scalar_pre_filter", opt->with_scalar_pre_filter, "Search vector with scalar data pre filter")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--with_scalar_post_filter", opt->with_scalar_post_filter,
                  "Search vector with scalar data post filter")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--print_vector_search_delay", opt->print_vector_search_delay, "print vector search delay")
      ->default_val(false)
      ->default_str("false");
  cmd->callback([opt]() { RunVectorBatchSearch(*opt); });
}

void RunVectorBatchSearch(VectorBatchSearchOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendVectorBatchSearch(opt);
}

void SetUpVectorBatchQuery(CLI::App& app) {
  auto opt = std::make_shared<VectorBatchQueryOptions>();
  auto* cmd = app.add_subcommand("VectorBatchQuery", "Vector batch query")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--vector_ids", opt->vector_ids, "Request parameter vector_ids");
  cmd->add_option("--key", opt->key, "Request parameter key");
  cmd->add_option("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->default_str("false");
  cmd->callback([opt]() { RunVectorBatchQuery(*opt); });
}

void RunVectorBatchQuery(VectorBatchQueryOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendVectorBatchQuery(opt);
}

void SetUpVectorScanQuery(CLI::App& app) {
  auto opt = std::make_shared<VectorScanQueryOptions>();
  auto* cmd = app.add_subcommand("VectorScanQuery", "Vector scan query ")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();

  cmd->add_option("--start_id", opt->start_id, "Request parameter start_id")->required();
  cmd->add_option("--end_id", opt->end_id, "Request parameter end_id")->required();
  cmd->add_option("--limit", opt->limit, "Request parameter limit")->default_val(50);
  cmd->add_option("--is_reverse", opt->is_reverse, "Request parameter is_reverse")->default_val(false);

  cmd->add_option("--without_vector", opt->without_vector, "Search vector without output vector data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_scalar", opt->without_scalar, "Search vector without scalar data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_table", opt->without_table, "Search vector without table data")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--key", opt->key, "Request parameter key");

  cmd->add_option("--scalar_filter_key", opt->scalar_filter_key, "Request scalar_filter_key");
  cmd->add_option("--scalar_filter_value", opt->scalar_filter_value, "Request parameter scalar_filter_value");
  cmd->add_option("--scalar_filter_key2", opt->scalar_filter_key2, "Request parameter scalar_filter_key2");
  cmd->add_option("--scalar_filter_value2", opt->scalar_filter_value2, "Request parameter scalar_filter_value2");

  cmd->callback([opt]() { RunVectorScanQuery(*opt); });
}

void RunVectorScanQuery(VectorScanQueryOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendVectorScanQuery(opt);
}

void SetUpVectorScanDump(CLI::App& app) {
  auto opt = std::make_shared<VectorScanDumpOptions>();
  auto* cmd = app.add_subcommand("VectorScanDump", "Vector scan dump ")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start_id")->required();
  cmd->add_option("--end_id", opt->end_id, "Request parameter end_id")->required();
  cmd->add_option("--limit", opt->limit, "Request parameter limit")->default_val(50);
  cmd->add_option("--is_reverse", opt->is_reverse, "Request parameter is_reverse")->default_val(false);
  cmd->add_option("--csv_output", opt->csv_output, "Request parameter is_reverse")->required();

  cmd->callback([opt]() { RunVectorScanDump(*opt); });
}

void RunVectorScanDump(VectorScanDumpOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendVectorScanDump(opt);
}

void SetUpVectorGetRegionMetrics(CLI::App& app) {
  auto opt = std::make_shared<VectorGetRegionMetricsOptions>();
  auto* cmd = app.add_subcommand("VectorGetRegionMetrics", "Vector get region metrics")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunVectorGetRegionMetricsd(*opt); });
}

void RunVectorGetRegionMetricsd(VectorGetRegionMetricsOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendVectorGetRegionMetrics(opt);
}

void SetUpVectorAdd(CLI::App& app) {
  auto opt = std::make_shared<VectorAddOptions>();
  auto* cmd = app.add_subcommand("VectorAdd", "Vector add ")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--table_id", opt->table_id, "Request parameter table_id");
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start_id")->required();
  cmd->add_option("--count", opt->count, "Request parameter count");
  cmd->add_option("--step_count", opt->step_count, "Request parameter step_count");
  cmd->add_option("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_table", opt->without_table, "Request parameter without_scalar")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--csv_data", opt->csv_data, "Request parameter csv_data");
  cmd->add_option("--json_data", opt->json_data, "Request parameter json_data");

  cmd->add_option("--scalar_filter_key", opt->scalar_filter_key, "Request parameter scalar_filter_key");
  cmd->add_option("--scalar_filter_value", opt->scalar_filter_value, "Request parameter scalar_filter_value");
  cmd->add_option("--scalar_filter_key2", opt->scalar_filter_key2, "Request parameter scalar_filter_key2");
  cmd->add_option("--scalar_filter_value2", opt->scalar_filter_value2, "Request parameter scalar_filter_value2");

  cmd->callback([opt]() { RunVectorAdd(*opt); });
}

void RunVectorAdd(VectorAddOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  if (opt.table_id > 0) {
    client_v2::SendVectorAddRetry(opt);
  } else {
    client_v2::SendVectorAdd(opt);
  }
  // client_v2::SendVectorGetRegionMetrics(opt.region_id);
}

void SetUpVectorDelete(CLI::App& app) {
  auto opt = std::make_shared<VectorDeleteOptions>();
  auto* cmd = app.add_subcommand("VectorDelete", "Vector delete")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start_id")->required();
  cmd->add_option("--count", opt->count, "Request parameter count")->required();
  cmd->callback([opt]() { RunVectorDelete(*opt); });
}

void RunVectorDelete(VectorDeleteOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendVectorDelete(opt);
}

void SetUpVectorGetMaxId(CLI::App& app) {
  auto opt = std::make_shared<VectorGetMaxIdOptions>();
  auto* cmd = app.add_subcommand("VectorGetMaxId", "Vector get max id")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunVectorGetMaxId(*opt); });
}

void RunVectorGetMaxId(VectorGetMaxIdOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendVectorGetMaxId(opt);
}

void SetUpVectorGetMinId(CLI::App& app) {
  auto opt = std::make_shared<VectorGetMinIdOptions>();
  auto* cmd = app.add_subcommand("VectorGetMinId", "Vector get min id")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunVectorGetMinId(*opt); });
}

void RunVectorGetMinId(VectorGetMinIdOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendVectorGetMinId(opt);
}

void SetUpVectorAddBatch(CLI::App& app) {
  auto opt = std::make_shared<VectorAddBatchOptions>();
  auto* cmd = app.add_subcommand("VectorAddBatch", "Vector add batch")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start_id")->required();
  cmd->add_option("--count", opt->count, "Request parameter count");
  cmd->add_option("--step_count", opt->step_count, "Request parameter step_count")->required();
  cmd->add_option("--vector_index_add_cost_file", opt->vector_index_add_cost_file, "exec batch vector add. cost time")
      ->default_val("./cost.txt");
  cmd->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")->default_val(false);

  cmd->callback([opt]() { RunVectorAddBatch(*opt); });
}

void RunVectorAddBatch(VectorAddBatchOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendVectorAddBatch(opt);
}

void SetUpVectorAddBatchDebug(CLI::App& app) {
  auto opt = std::make_shared<VectorAddBatchDebugOptions>();
  auto* cmd = app.add_subcommand("VectorAddBatchDebug", "Vector add batch debug")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start_id")->required();
  cmd->add_option("--count", opt->count, "Request parameter count");
  cmd->add_option("--step_count", opt->step_count, "Request parameter step_count")->required();
  cmd->add_option("--vector_index_add_cost_file", opt->vector_index_add_cost_file, "exec batch vector add. cost time")
      ->default_val("./cost.txt");
  cmd->add_option("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->default_str("false");
  cmd->callback([opt]() { RunVectorAddBatchDebug(*opt); });
}

void RunVectorAddBatchDebug(VectorAddBatchDebugOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  client_v2::SendVectorAddBatchDebug(opt);
}

void SetUpVectorCalcDistance(CLI::App& app) {
  auto opt = std::make_shared<VectorCalcDistanceOptions>();
  auto* cmd = app.add_subcommand("VectorCalcDistance", "Vector add batch debug")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->required();
  cmd->add_option("--alg_type", opt->alg_type, "use alg type. such as faiss or hnsw")->default_val("faiss");
  cmd->add_option("--metric_type", opt->metric_type, "metric type. such as L2 or IP or cosine")->default_val("L2");
  cmd->add_option("--left_vector_size", opt->left_vector_size, "left vector size. <= 0 error")->default_val(2);
  cmd->add_option("--right_vector_size", opt->right_vector_size, "right vector size. <= 0 error")->default_val(3);
  cmd->add_option("--is_return_normlize", opt->is_return_normlize, "is return normlize")
      ->default_val(true)
      ->default_str("true");
  cmd->callback([opt]() { RunVectorCalcDistance(*opt); });
}

void RunVectorCalcDistance(VectorCalcDistanceOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendVectorCalcDistance(opt);
}

void SetUpCalcDistance(CLI::App& app) {
  auto opt = std::make_shared<CalcDistanceOptions>();
  auto* cmd = app.add_subcommand("CalcDistance", "Calc distance")->group("Vector Command");
  cmd->add_option("--vector_data1", opt->vector_data1, "Request parameter vector_data1")->required();
  cmd->add_option("--vector_data2", opt->vector_data2, "Request parameter vector_data2")->required();
  cmd->callback([opt]() { RunCalcDistance(*opt); });
}

void RunCalcDistance(CalcDistanceOptions const& opt) { client_v2::SendCalcDistance(opt); }

void SetUpVectorCount(CLI::App& app) {
  auto opt = std::make_shared<VectorCountOptions>();
  auto* cmd = app.add_subcommand("VectorCount", "Vector count")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start_id")->required();
  cmd->add_option("--end_id", opt->end_id, "Request parameter end_id")->required();
  cmd->callback([opt]() { RunVectorCount(*opt); });
}

void RunVectorCount(VectorCountOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendVectorCount(opt, true);
}

void SetUpCountVectorTable(CLI::App& app) {
  auto opt = std::make_shared<CountVectorTableOptions>();
  auto* cmd = app.add_subcommand("CountVectorTable", "Count vector table")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--store_addrs", opt->store_addrs, "server addrs")->required();
  cmd->add_option("--table_id", opt->table_id, "Request parameter table_id")->required();
  cmd->callback([opt]() { RunCountVectorTable(*opt); });
}

void RunCountVectorTable(CountVectorTableOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {opt.store_addrs}, 0)) {
    exit(-1);
  }

  client_v2::CountVectorTable(opt);
}

void SetUpVectorImport(CLI::App& app) {
  auto opt = std::make_shared<VectorImportOptions>();
  auto* cmd = app.add_subcommand("VectorImport", "Vector import")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension ")->required();
  cmd->add_option("--count", opt->count, "Request parameter count")->default_val(50);
  cmd->add_option("--step_count", opt->step_count, "Request parameter step count")->default_val(1024);
  cmd->add_option("--start_id", opt->start_id, "Request parameter start id");
  cmd->add_option("--import_for_add", opt->import_for_add, "diskann execute import for add or delete, default is add")
      ->default_val(true)
      ->default_str("true");
  cmd->add_option("--without_scalar", opt->without_scalar, "Request parameter without scalar")
      ->default_val(false)
      ->default_str("false");
  ;
  cmd->callback([opt]() { RunVectorImport(*opt); });
}

void RunVectorImport(VectorImportOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, 0)) {
    exit(-1);
  }

  client_v2::SendVectorImport(opt);
}

void SetUpVectorBuild(CLI::App& app) {
  auto opt = std::make_shared<VectorBuildOptions>();
  auto* cmd = app.add_subcommand("VectorBuild", "Vector build")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunVectorBuild(*opt); });
}

void RunVectorBuild(VectorBuildOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, 0)) {
    exit(-1);
  }

  client_v2::SendVectorBuild(opt);
}

void SetUpVectorLoad(CLI::App& app) {
  auto opt = std::make_shared<VectorLoadOptions>();
  auto* cmd = app.add_subcommand("VectorLoad", "Vector load")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--num_nodes_to_cache", opt->num_nodes_to_cache,
                  "While serving the index, the entire graph is stored on SSD.For faster search performance, you can "
                  "cache a few frequently accessed nodes in memory.");
  cmd->add_option("--direct_load_without_build", opt->direct_load_without_build,
                  "diskann if true, direct load without build index. default is false. Note: If it is true, do not "
                  "call the import build interface, call load directly. If it is false, first import the data, then "
                  "build, and then call load")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--warmup", opt->warmup,
                  "Whether to warm up the index. If true, the index will be loaded into memory and kept in memory for "
                  "faster.")
      ->default_val(true)
      ->default_str("true");
  ;
  cmd->callback([opt]() { RunVectorLoad(*opt); });
}

void RunVectorLoad(VectorLoadOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, 0)) {
    exit(-1);
  }

  client_v2::SendVectorLoad(opt);
}

void SetUpVectorStatus(CLI::App& app) {
  auto opt = std::make_shared<VectorStatusOptions>();
  auto* cmd = app.add_subcommand("VectorStatus", "Vector status")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunVectorStatus(*opt); });
}

void RunVectorStatus(VectorStatusOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, 0)) {
    exit(-1);
  }

  client_v2::SendVectorStatus(opt);
}

void SetUpVectorReset(CLI::App& app) {
  auto opt = std::make_shared<VectorResetOptions>();
  auto* cmd = app.add_subcommand("VectorReset", "Vector reset")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--delete_data_file", opt->delete_data_file, "Diskann If true, delete data file after reset.")
      ->default_val(false)
      ->default_str("false");
  cmd->callback([opt]() { RunVectorReset(*opt); });
}

void RunVectorReset(VectorResetOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, 0)) {
    exit(-1);
  }

  client_v2::SendVectorReset(opt);
}

void SetUpVectorDump(CLI::App& app) {
  auto opt = std::make_shared<VectorDumpOptions>();
  auto* cmd = app.add_subcommand("VectorDump", "Vector dump")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--dump_all", opt->dump_all,
                  "Diskann If true, dump all vector id data from diskann. or just dump the data of this region.")
      ->default_val(false)
      ->default_str("false");
  cmd->callback([opt]() { RunVectorDump(*opt); });
}

void RunVectorDump(VectorDumpOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, 0)) {
    exit(-1);
  }

  client_v2::SendVectorDump(opt);
}

void SetUpVectorCountMemory(CLI::App& app) {
  auto opt = std::make_shared<VectorCountMemoryOptions>();
  auto* cmd = app.add_subcommand("VectorCountMemory", "Vector count memory")->group("Vector Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunVectorCountMemory(*opt); });
}

void RunVectorCountMemory(VectorCountMemoryOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, 0)) {
    exit(-1);
  }

  client_v2::SendVectorCountMemory(opt);
}

}  // namespace client_v2
