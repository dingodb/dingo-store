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

#include "client/store_client_function.h"

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "bthread/bthread.h"
#include "client/client_helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"
#include "serial/buf.h"

const int kBatchSize = 1000;

DECLARE_string(key);
DECLARE_bool(without_vector);
DECLARE_bool(with_scalar);
DECLARE_bool(with_table);
DECLARE_bool(print_vector_search_delay);
DECLARE_string(scalar_filter_key);
DECLARE_string(scalar_filter_value);
DECLARE_string(scalar_filter_key2);
DECLARE_string(scalar_filter_value2);
DECLARE_bool(with_vector_ids);
DECLARE_bool(with_scalar_pre_filter);
DECLARE_bool(with_scalar_post_filter);
DECLARE_uint32(vector_ids_count);

namespace client {

/*
 * replacement for the openmp '#pragma omp parallel for' directive
 * only handles a subset of functionality (no reductions etc)
 * Process ids from start (inclusive) to end (EXCLUSIVE)
 *
 * The method is borrowed from nmslib
 */
template <class Function>
inline void ParallelFor(size_t start, size_t end, size_t num_threads, Function fn) {
  if (num_threads <= 0) {
    num_threads = std::thread::hardware_concurrency();
  }

  if (num_threads == 1) {
    for (size_t id = start; id < end; id++) {
      fn(id, 0);
    }
  } else {
    std::vector<std::thread> threads;
    std::atomic<size_t> current(start);

    // keep track of exceptions in threads
    // https://stackoverflow.com/a/32428427/1713196
    std::exception_ptr last_exception = nullptr;
    std::mutex last_except_mutex;

    for (size_t thread_id = 0; thread_id < num_threads; ++thread_id) {
      threads.push_back(std::thread([&, thread_id] {
        while (true) {
          size_t id = current.fetch_add(1);

          if (id >= end) {
            break;
          }

          try {
            fn(id, thread_id);
          } catch (...) {
            std::unique_lock<std::mutex> last_excep_lock(last_except_mutex);
            last_exception = std::current_exception();
            /*
             * This will work even when current is the largest value that
             * size_t can fit, because fetch_add returns the previous value
             * before the increment (what will result in overflow
             * and produce 0 instead of current + 1).
             */
            current = end;
            break;
          }
        }
      }));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    if (last_exception) {
      std::rethrow_exception(last_exception);
    }
  }
}

// ============================== meta service ===========================

dingodb::pb::meta::TableRange SendGetTableRange(ServerInteractionPtr interaction, uint64_t table_id) {
  dingodb::pb::meta::GetTableRangeRequest request;
  dingodb::pb::meta::GetTableRangeResponse response;

  auto* mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  interaction->SendRequest("MetaService", "GetTableRange", request, response);

  return response.table_range();
}

dingodb::pb::meta::IndexRange SendGetIndexRange(ServerInteractionPtr interaction, uint64_t table_id) {
  dingodb::pb::meta::GetIndexRangeRequest request;
  dingodb::pb::meta::GetIndexRangeResponse response;

  auto* mut_index_id = request.mutable_index_id();
  mut_index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_index_id->set_entity_id(table_id);

  interaction->SendRequest("MetaService", "GetIndexRange", request, response);

  return response.index_range();
}

std::vector<uint64_t> SendGetTablesBySchema(ServerInteractionPtr interaction) {
  dingodb::pb::meta::GetTablesBySchemaRequest request;
  dingodb::pb::meta::GetTablesBySchemaResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  interaction->SendRequest("MetaService", "GetTablesBySchema", request, response);

  std::vector<uint64_t> table_ids;
  for (const auto& id : response.table_definition_with_ids()) {
    table_ids.push_back(id.table_id().entity_id());
  }

  return table_ids;
}

dingodb::pb::meta::CreateTableRequest BuildCreateTableRequest(const std::string& table_name, int partition_num) {
  dingodb::pb::meta::CreateTableRequest request;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  // string name = 1;
  auto* table_definition = request.mutable_table_definition();
  table_definition->set_name(table_name);

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type("INT");
    column->set_element_type("INT");
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");
  }

  table_definition->set_version(1);
  table_definition->set_ttl(0);
  table_definition->set_engine(::dingodb::pb::common::Engine::ENG_ROCKSDB);
  auto* prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  auto* partition_rule = table_definition->mutable_table_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  auto* range_partition = partition_rule->mutable_range_partition();

  for (int i = 0; i < partition_num; i++) {
    auto* part_range = range_partition->add_ranges();
    auto* part_range_start = part_range->mutable_start_key();
    part_range_start->assign(std::to_string(i * 100));
    auto* part_range_end = part_range->mutable_end_key();
    part_range_end->assign(std::to_string((i + 1) * 100));
  }

  return request;
}

uint64_t SendCreateTable(ServerInteractionPtr interaction, const std::string& table_name, int partition_num) {
  auto request = BuildCreateTableRequest(table_name, partition_num);

  dingodb::pb::meta::CreateTableResponse response;
  interaction->SendRequest("MetaService", "CreateTable", request, response);

  DINGO_LOG(INFO) << "response=" << response.DebugString();
  return response.table_id().entity_id();
}

void SendDropTable(ServerInteractionPtr interaction, uint64_t table_id) {
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  auto* mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  interaction->SendRequest("MetaService", "DropTable", request, response);
}

uint64_t SendGetTableByName(ServerInteractionPtr interaction, const std::string& table_name) {
  dingodb::pb::meta::GetTableByNameRequest request;
  dingodb::pb::meta::GetTableByNameResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_table_name(table_name);

  interaction->SendRequest("MetaService", "GetTableByName", request, response);

  return response.table_definition_with_id().table_id().entity_id();
}

// ============================== meta service ===========================

void SendVectorSearch(ServerInteractionPtr interaction, uint64_t region_id, uint32_t dimension, uint64_t vector_id,
                      uint32_t topn) {
  dingodb::pb::index::VectorSearchRequest request;
  dingodb::pb::index::VectorSearchResponse response;

  request.set_region_id(region_id);
  auto* vector = request.mutable_vector()->mutable_vector();

  if (region_id == 0) {
    DINGO_LOG(ERROR) << "region_id is 0";
    return;
  }

  if (dimension == 0) {
    DINGO_LOG(ERROR) << "dimension is 0";
    return;
  }

  if (topn == 0) {
    DINGO_LOG(ERROR) << "topn is 0";
    return;
  }

  if (vector_id > 0) {
    request.mutable_vector()->set_id(vector_id);
  } else {
    for (int i = 0; i < dimension; i++) {
      vector->add_float_values(1.0 * i);
    }

    request.mutable_parameter()->set_top_n(topn);
  }

  if (FLAGS_without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (FLAGS_with_scalar) {
    request.mutable_parameter()->set_with_scalar_data(true);
  }

  if (FLAGS_with_table) {
    request.mutable_parameter()->set_with_table_data(true);
  }

  if (!FLAGS_key.empty()) {
    auto* key = request.mutable_parameter()->mutable_selected_keys()->Add();
    key->assign(FLAGS_key);
  }

  std::vector<uint64_t> vt_ids;
  if (FLAGS_with_vector_ids) {
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

  if (FLAGS_with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector();

    for (int k = 0; k < 2; k++) {
      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data("value" + std::to_string(k));

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({"key" + std::to_string(k), scalar_value});
    }
  }

  if (FLAGS_with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);

    dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector();

    for (int k = 0; k < 2; k++) {
      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data("value" + std::to_string(k));

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({"key" + std::to_string(k), scalar_value});
    }
  }

  if (FLAGS_print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    interaction->SendRequest("IndexService", "VectorSearch", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearch  span: {} (us)", diff);

  } else {
    interaction->SendRequest("IndexService", "VectorSearch", request, response);
  }

  DINGO_LOG(INFO) << "VectorSearch response: " << response.DebugString();

  // match compare
  if (FLAGS_with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';

    std::vector<uint64_t> result_vt_ids;
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

  if (FLAGS_with_scalar_pre_filter || FLAGS_with_scalar_post_filter) {
    std::vector<uint64_t> result_vt_ids;
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

void SendVectorSearchDebug(ServerInteractionPtr interaction, uint64_t region_id, uint32_t dimension,
                           uint64_t start_vector_id, uint32_t topn, uint32_t batch_count, const std::string& key,
                           const std::string& value) {
  dingodb::pb::index::VectorSearchDebugRequest request;
  dingodb::pb::index::VectorSearchDebugResponse response;

  request.set_region_id(region_id);

  if (region_id == 0) {
    DINGO_LOG(ERROR) << "region_id is 0";
    return;
  }

  if (dimension == 0) {
    DINGO_LOG(ERROR) << "dimension is 0";
    return;
  }

  if (topn == 0) {
    DINGO_LOG(ERROR) << "topn is 0";
    return;
  }

  if (batch_count == 0) {
    DINGO_LOG(ERROR) << "batch_count is 0";
    return;
  }

  if (start_vector_id > 0) {
    for (int count = 0; count < batch_count; count++) {
      auto* add_vector_with_id = request.add_vector_with_ids();
      add_vector_with_id->set_id(start_vector_id + count);
    }
  } else {
    std::random_device seed;
    std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(0, 100);

    for (int count = 0; count < batch_count; count++) {
      auto* vector = request.add_vector_with_ids()->mutable_vector();
      for (int i = 0; i < dimension; i++) {
        auto random = static_cast<double>(distrib(engine)) / 10.123;
        vector->add_float_values(random);
      }
    }

    request.mutable_parameter()->set_top_n(topn);
  }

  if (FLAGS_without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (FLAGS_with_scalar) {
    request.mutable_parameter()->set_with_scalar_data(true);
  }

  if (FLAGS_with_table) {
    request.mutable_parameter()->set_with_table_data(true);
  }

  if (!FLAGS_key.empty()) {
    auto* key = request.mutable_parameter()->mutable_selected_keys()->Add();
    key->assign(FLAGS_key);
  }

  std::vector<uint64_t> vt_ids;
  if (FLAGS_with_vector_ids) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (int i = 0; i < FLAGS_vector_ids_count; i++) {
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
    std::cout << std::endl;
  }

  if (FLAGS_with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (uint32_t m = 0; m < batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({key, scalar_value});
    }

  }

  if (FLAGS_with_scalar_post_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_POST);

    for (uint32_t m = 0; m < batch_count; m++) {
      dingodb::pb::common::VectorWithId* vector_with_id = request.mutable_vector_with_ids(m);

      ::dingodb::pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      ::dingodb::pb::common::ScalarField* field = scalar_value.add_fields();
      field->set_string_data(value);

      vector_with_id->mutable_scalar_data()->mutable_scalar_data()->insert({key, scalar_value});
    }

  }

  if (FLAGS_print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    interaction->SendRequest("IndexService", "VectorSearchDebug", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearchDebug  span: {} (us)", diff);

  } else {
    interaction->SendRequest("IndexService", "VectorSearchDebug", request, response);
  }

  DINGO_LOG(INFO) << "VectorSearchDebug response: " << response.DebugString();

  DINGO_LOG(INFO) << "VectorSearchDebug response, batch_result_size: " << response.batch_results_size();
  for (const auto& batch_result : response.batch_results()) {
    DINGO_LOG(INFO) << "VectorSearchDebug response, batch_result_dist_size: "
                    << batch_result.vector_with_distances_size();
  }

#if 0
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
      std::vector<uint64_t> result_vt_ids;
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
      std::vector<uint64_t> result_vt_ids;
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

void SendVectorBatchSearch(ServerInteractionPtr interaction, uint64_t region_id, uint32_t dimension, uint64_t vector_id,
                           uint32_t topn, uint32_t batch_count) {
  dingodb::pb::index::VectorSearchRequest request;
  dingodb::pb::index::VectorSearchResponse response;

  request.set_region_id(region_id);

  if (region_id == 0) {
    DINGO_LOG(ERROR) << "region_id is 0";
    return;
  }

  if (dimension == 0) {
    DINGO_LOG(ERROR) << "dimension is 0";
    return;
  }

  if (topn == 0) {
    DINGO_LOG(ERROR) << "topn is 0";
    return;
  }

  if (batch_count == 0) {
    DINGO_LOG(ERROR) << "batch_count is 0";
    return;
  }

  if (vector_id > 0) {
    request.mutable_vector()->set_id(vector_id);
  } else {
    std::random_device seed;
    std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(0, 100);

    for (int count = 0; count < batch_count; count++) {
      auto* vector = request.add_vector_with_ids()->mutable_vector();
      for (int i = 0; i < dimension; i++) {
        auto random = static_cast<double>(distrib(engine)) / 10.123;
        vector->add_float_values(random);
      }
    }

    request.mutable_parameter()->set_top_n(topn);
  }

  if (FLAGS_without_vector) {
    request.mutable_parameter()->set_without_vector_data(true);
  }

  if (FLAGS_with_scalar) {
    request.mutable_parameter()->set_with_scalar_data(true);
  }

  if (FLAGS_with_table) {
    request.mutable_parameter()->set_with_table_data(true);
  }

  if (!FLAGS_key.empty()) {
    auto* key = request.mutable_parameter()->mutable_selected_keys()->Add();
    key->assign(FLAGS_key);
  }

  std::vector<uint64_t> vt_ids;
  if (FLAGS_with_vector_ids) {
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

  if (FLAGS_with_scalar_pre_filter) {
    request.mutable_parameter()->set_vector_filter(::dingodb::pb::common::VectorFilter::SCALAR_FILTER);
    request.mutable_parameter()->set_vector_filter_type(::dingodb::pb::common::VectorFilterType::QUERY_PRE);

    for (uint32_t m = 0; m < batch_count; m++) {
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

  if (FLAGS_print_vector_search_delay) {
    auto start = std::chrono::steady_clock::now();
    interaction->SendRequest("IndexService", "VectorSearch", request, response);
    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    DINGO_LOG(INFO) << fmt::format("SendVectorSearch  span: {} (us)", diff);

  } else {
    interaction->SendRequest("IndexService", "VectorSearch", request, response);
  }

  DINGO_LOG(INFO) << "VectorSearch response: " << response.DebugString();

  DINGO_LOG(INFO) << "VectorSearch response, batch_result_size: " << response.batch_results_size();
  for (const auto& batch_result : response.batch_results()) {
    DINGO_LOG(INFO) << "VectorSearch response, batch_result_dist_size: " << batch_result.vector_with_distances_size();
  }

  // match compare
  if (FLAGS_with_vector_ids) {
    std::cout << "vector_ids : " << request.parameter().vector_ids().size() << " [ ";

    for (auto id : request.parameter().vector_ids()) {
      std::cout << id << " ";
    }
    std::cout << "]";
    std::cout << '\n';

    std::cout << "response.batch_results() size : " << response.batch_results().size() << '\n';

    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<uint64_t> result_vt_ids;
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

  if (FLAGS_with_scalar_pre_filter) {
    for (const auto& vector_with_distance_result : response.batch_results()) {
      std::vector<uint64_t> result_vt_ids;
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

void SendVectorBatchQuery(ServerInteractionPtr interaction, uint64_t region_id, std::vector<uint64_t> vector_ids) {
  dingodb::pb::index::VectorBatchQueryRequest request;
  dingodb::pb::index::VectorBatchQueryResponse response;

  request.set_region_id(region_id);
  for (auto vector_id : vector_ids) {
    request.add_vector_ids(vector_id);
  }

  if (FLAGS_without_vector) {
    request.set_without_vector_data(true);
  }

  if (FLAGS_with_scalar) {
    request.set_with_scalar_data(true);
  }

  if (FLAGS_with_table) {
    request.set_with_table_data(true);
  }

  if (!FLAGS_key.empty()) {
    auto* key = request.mutable_selected_keys()->Add();
    key->assign(FLAGS_key);
  }

  interaction->SendRequest("IndexService", "VectorBatchQuery", request, response);

  DINGO_LOG(INFO) << "VectorBatchQuery response: " << response.DebugString();
}

void SendVectorScanQuery(ServerInteractionPtr interaction, uint64_t region_id, uint64_t start_id, uint64_t end_id,
                         uint64_t limit, bool is_reverse) {
  dingodb::pb::index::VectorScanQueryRequest request;
  dingodb::pb::index::VectorScanQueryResponse response;

  request.set_region_id(region_id);
  request.set_vector_id_start(start_id);
  request.set_vector_id_end(end_id);

  if (limit > 0) {
    request.set_max_scan_count(limit);
  } else {
    request.set_max_scan_count(10);
  }

  request.set_is_reverse_scan(is_reverse);

  if (FLAGS_without_vector) {
    request.set_without_vector_data(true);
  }

  if (FLAGS_with_scalar) {
    request.set_with_scalar_data(true);
  }

  if (FLAGS_with_table) {
    request.set_with_table_data(true);
  }

  if (!FLAGS_key.empty()) {
    auto* key = request.mutable_selected_keys()->Add();
    key->assign(FLAGS_key);
  }

  if (!FLAGS_scalar_filter_key.empty()) {
    auto* scalar_data = request.mutable_scalar_for_filter()->mutable_scalar_data();
    dingodb::pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    scalar_value.add_fields()->set_string_data(FLAGS_scalar_filter_value);
    (*scalar_data)[FLAGS_scalar_filter_key] = scalar_value;

    request.set_use_scalar_filter(true);

    DINGO_LOG(INFO) << "scalar_filter_key: " << FLAGS_scalar_filter_key
                    << " scalar_filter_value: " << FLAGS_scalar_filter_value;
  }

  if (!FLAGS_scalar_filter_key2.empty()) {
    auto* scalar_data = request.mutable_scalar_for_filter()->mutable_scalar_data();
    dingodb::pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    scalar_value.add_fields()->set_string_data(FLAGS_scalar_filter_value2);
    (*scalar_data)[FLAGS_scalar_filter_key2] = scalar_value;

    request.set_use_scalar_filter(true);

    DINGO_LOG(INFO) << "scalar_filter_key2: " << FLAGS_scalar_filter_key2
                    << " scalar_filter_value2: " << FLAGS_scalar_filter_value2;
  }

  DINGO_LOG(INFO) << "VectorScanQuery response: " << response.DebugString();

  interaction->SendRequest("IndexService", "VectorScanQuery", request, response);

  DINGO_LOG(INFO) << "VectorScanQuery response: " << response.DebugString();
}

void SendVectorGetRegionMetrics(ServerInteractionPtr interaction, uint64_t region_id) {
  dingodb::pb::index::VectorGetRegionMetricsRequest request;
  dingodb::pb::index::VectorGetRegionMetricsResponse response;

  request.set_region_id(region_id);

  interaction->SendRequest("IndexService", "VectorGetRegionMetrics", request, response);

  DINGO_LOG(INFO) << "VectorGetRegionMetrics response: " << response.DebugString();
}

int SendBatchVectorAdd(ServerInteractionPtr interaction, uint64_t region_id, uint32_t dimension,
                       std::vector<uint64_t> vector_ids, bool with_scalar, bool with_table) {
  dingodb::pb::index::VectorAddRequest request;
  dingodb::pb::index::VectorAddResponse response;

  request.set_region_id(region_id);

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  for (auto vector_id : vector_ids) {
    auto* vector_with_id = request.add_vectors();
    vector_with_id->set_id(vector_id);
    vector_with_id->mutable_vector()->set_dimension(dimension);
    vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector_with_id->mutable_vector()->add_float_values(distrib(rng));
    }

    if (with_scalar) {
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
    }

    if (with_table) {
      auto* table_data = vector_with_id->mutable_table_data();
      table_data->set_table_key(fmt::format("table_key{}", vector_id));
      table_data->set_table_value(fmt::format("table_value{}", vector_id));
    }
  }

  butil::Status status = interaction->SendRequest("IndexService", "VectorAdd", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "VectorAdd repsonse error: " << response.error().DebugString();
  }

  DINGO_LOG(INFO) << fmt::format("VectorAdd response success region: {} count: {} fail count: {} vector count: {}",
                                 region_id, success_count, response.key_states().size() - success_count,
                                 request.vectors().size());

  return response.error().errcode();
}

uint64_t DecodeVectorId(const std::string& value) {
  dingodb::Buf buf(value);
  if (value.size() == 17) {
    buf.Skip(9);
  } else if (value.size() == 16) {
    buf.Skip(8);
  } else {
    DINGO_LOG(ERROR) << "Decode vector id failed, value size is not 16 or 17, value:["
                     << dingodb::Helper::StringToHex(value) << "]";
    return 0;
  }

  return buf.ReadLong();
}

bool QueryRegionIdByVectorId(dingodb::pb::meta::IndexRange& index_range, uint64_t vector_id,  // NOLINT
                             uint64_t& region_id) {                                           // NOLINT
  for (const auto& item : index_range.range_distribution()) {
    const auto& range = item.range();
    uint64_t min_vector_id = DecodeVectorId(range.start_key());
    uint64_t max_vector_id = DecodeVectorId(range.end_key());
    max_vector_id = max_vector_id == 0 ? UINT64_MAX : max_vector_id;
    if (vector_id >= min_vector_id && vector_id < max_vector_id) {
      region_id = item.id().entity_id();
      return true;
    }
  }

  DINGO_LOG(ERROR) << fmt::format("query region id by key failed, vector_id {}", vector_id);
  return false;
}

void PrintIndexRange(dingodb::pb::meta::IndexRange& index_range) {  // NOLINT
  DINGO_LOG(INFO) << "refresh route...";
  for (const auto& item : index_range.range_distribution()) {
    DINGO_LOG(INFO) << fmt::format("region {} range [{}-{})", item.id().entity_id(),
                                   dingodb::Helper::StringToHex(item.range().start_key()),
                                   dingodb::Helper::StringToHex(item.range().end_key()));
  }
}

void SendVectorAddRetry(std::shared_ptr<Context> ctx) {  // NOLINT
  auto index_range = SendGetIndexRange(ctx->coordinator_interaction, ctx->table_id);
  PrintIndexRange(index_range);

  int end_id = ctx->start_id + ctx->count;
  std::vector<uint64_t> vector_ids;
  vector_ids.reserve(ctx->step_count);
  for (int i = ctx->start_id; i < end_id; i += ctx->step_count) {
    for (int j = i; j < i + ctx->step_count; ++j) {
      vector_ids.push_back(j);
    }

    uint64_t region_id = 0;
    if (!QueryRegionIdByVectorId(index_range, i, region_id)) {
      DINGO_LOG(ERROR) << fmt::format("query region id by vector id failed, vector id {}", i);
      return;
    }

    int ret = SendBatchVectorAdd(ctx->store_interaction, region_id, ctx->dimension, vector_ids, FLAGS_with_scalar,
                                 FLAGS_with_table);
    if (ret == dingodb::pb::error::EKEY_OUT_OF_RANGE || ret == dingodb::pb::error::EREGION_REDIRECT) {
      bthread_usleep(1000 * 500);  // 500ms
      index_range = SendGetIndexRange(ctx->coordinator_interaction, ctx->table_id);
      PrintIndexRange(index_range);
    }

    DINGO_LOG(INFO) << fmt::format("schedule: {}/{}", i, end_id);

    vector_ids.clear();
  }
}

void SendVectorAdd(std::shared_ptr<Context> ctx) {  // NOLINT
  int end_id = ctx->start_id + ctx->count;
  std::vector<uint64_t> vector_ids;
  vector_ids.reserve(ctx->step_count);
  for (int i = ctx->start_id; i < end_id; i += ctx->step_count) {
    for (int j = i; j < i + ctx->step_count; ++j) {
      vector_ids.push_back(j);
    }

    int ret = SendBatchVectorAdd(ctx->store_interaction, ctx->region_id, ctx->dimension, vector_ids, FLAGS_with_scalar,
                                 FLAGS_with_table);

    vector_ids.clear();
  }
}

void SendVectorDelete(ServerInteractionPtr interaction, uint64_t region_id, uint32_t start_id,  // NOLINT
                      uint32_t count) {                                                         // NOLINT
  dingodb::pb::index::VectorDeleteRequest request;
  dingodb::pb::index::VectorDeleteResponse response;

  request.set_region_id(region_id);
  for (int i = 0; i < count; ++i) {
    request.add_ids(i + start_id);
  }

  interaction->SendRequest("IndexService", "VectorDelete", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }

  DINGO_LOG(INFO) << "VectorDelete repsonse error: " << response.error().DebugString();
  DINGO_LOG(INFO) << fmt::format("VectorDelete response success count: {} fail count: {}", success_count,
                                 response.key_states().size() - success_count);
}

void SendVectorGetMaxId(ServerInteractionPtr interaction, uint64_t region_id) {  // NOLINT
  dingodb::pb::index::VectorGetBorderIdRequest request;
  dingodb::pb::index::VectorGetBorderIdResponse response;

  request.set_region_id(region_id);

  interaction->SendRequest("IndexService", "VectorGetBorderId", request, response);

  DINGO_LOG(INFO) << "VectorGetBorderId response: " << response.DebugString();
}

void SendVectorGetMinId(ServerInteractionPtr interaction, uint64_t region_id) {  // NOLINT
  dingodb::pb::index::VectorGetBorderIdRequest request;
  dingodb::pb::index::VectorGetBorderIdResponse response;

  request.set_region_id(region_id);
  request.set_get_min(true);

  interaction->SendRequest("IndexService", "VectorGetBorderId", request, response);

  DINGO_LOG(INFO) << "VectorGetBorderId response: " << response.DebugString();
}

void SendVectorAddBatch(ServerInteractionPtr interaction, uint64_t region_id, uint32_t dimension, uint32_t count,
                        uint32_t step_count, int64_t start_id, const std::string& file) {
  if (step_count == 0) {
    DINGO_LOG(ERROR) << "step_count must be greater than 0";
    return;
  }
  if (region_id == 0) {
    DINGO_LOG(ERROR) << "region_id must be greater than 0";
    return;
  }
  if (dimension == 0) {
    DINGO_LOG(ERROR) << "dimension must be greater than 0";
    return;
  }
  if (count == 0) {
    DINGO_LOG(ERROR) << "count must be greater than 0";
    return;
  }
  if (start_id < 0) {
    DINGO_LOG(ERROR) << "start_id must be greater than 0";
    return;
  }
  if (file.empty()) {
    DINGO_LOG(ERROR) << "vector_index_add_cost_file must not be empty";
    return;
  }

  std::filesystem::path url(file);
  std::fstream out;
  if (!std::filesystem::exists(url)) {
    // not exist
    out.open(file, std::ios::out | std::ios::binary);
  } else {
    out.open(file, std::ios::out | std::ios::binary | std::ios::trunc);
  }

  if (!out.is_open()) {
    DINGO_LOG(ERROR) << fmt::format("{} open failed", file);
    out.close();
    return;
  }

  out << "index,cost(us)\n";
  int64_t total = 0;

  if (count % step_count != 0) {
    DINGO_LOG(ERROR) << fmt::format("count {} must be divisible by step_count {}", count, step_count);
    return;
  }

  uint32_t cnt = count / step_count;

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  std::vector<float> random_seeds;
  random_seeds.resize(count * dimension);
  for (uint32_t i = 0; i < count; ++i) {
    for (uint32_t j = 0; j < dimension; ++j) {
      random_seeds[i * dimension + j] = distrib(rng);
    }

    if (i % 10000 == 0) {
      DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", i, count);
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
  //   DINGO_LOG(ERROR) << "generate random data failed, error=" << e.what();
  //   return;
  // }

  DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", count, count);

  for (uint32_t x = 0; x < cnt; x++) {
    auto start = std::chrono::steady_clock::now();
    {
      dingodb::pb::index::VectorAddRequest request;
      dingodb::pb::index::VectorAddResponse response;

      request.set_region_id(region_id);

      uint64_t real_start_id = start_id + x * step_count;
      for (int i = real_start_id; i < real_start_id + step_count; ++i) {
        auto* vector_with_id = request.add_vectors();
        vector_with_id->set_id(i);
        vector_with_id->mutable_vector()->set_dimension(dimension);
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        for (int j = 0; j < dimension; j++) {
          vector_with_id->mutable_vector()->add_float_values(random_seeds[(i - start_id) * dimension + j]);
        }

        if (FLAGS_with_scalar) {
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

      butil::Status status = interaction->SendRequest("IndexService", "VectorAdd", request, response);
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

    DINGO_LOG(INFO) << "index : " << x << " : " << diff << " us, avg : " << static_cast<long double>(diff) / step_count
                    << " us";

    total += diff;
  }

  DINGO_LOG(INFO) << fmt::format("total : {} cost : {} (us) avg : {} us", count, total,
                                 static_cast<long double>(total) / count);

  out.close();
}

void SendVectorAddBatchDebug(ServerInteractionPtr interaction, uint64_t region_id, uint32_t dimension, uint32_t count,
                             uint32_t step_count, int64_t start_id, const std::string& file) {
  if (step_count == 0) {
    DINGO_LOG(ERROR) << "step_count must be greater than 0";
    return;
  }
  if (region_id == 0) {
    DINGO_LOG(ERROR) << "region_id must be greater than 0";
    return;
  }
  if (dimension == 0) {
    DINGO_LOG(ERROR) << "dimension must be greater than 0";
    return;
  }
  if (count == 0) {
    DINGO_LOG(ERROR) << "count must be greater than 0";
    return;
  }
  if (start_id < 0) {
    DINGO_LOG(ERROR) << "start_id must be greater than 0";
    return;
  }
  if (file.empty()) {
    DINGO_LOG(ERROR) << "vector_index_add_cost_file must not be empty";
    return;
  }

  std::filesystem::path url(file);
  std::fstream out;
  if (!std::filesystem::exists(url)) {
    // not exist
    out.open(file, std::ios::out | std::ios::binary);
  } else {
    out.open(file, std::ios::out | std::ios::binary | std::ios::trunc);
  }

  if (!out.is_open()) {
    DINGO_LOG(ERROR) << fmt::format("{} open failed", file);
    out.close();
    return;
  }

  out << "index,cost(us)\n";
  int64_t total = 0;

  if (count % step_count != 0) {
    DINGO_LOG(ERROR) << fmt::format("count {} must be divisible by step_count {}", count, step_count);
    return;
  }

  uint32_t cnt = count / step_count;

  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(0.0, 10.0);

  std::vector<float> random_seeds;
  random_seeds.resize(count * dimension);
  for (uint32_t i = 0; i < count; ++i) {
    for (uint32_t j = 0; j < dimension; ++j) {
      random_seeds[i * dimension + j] = distrib(rng);
    }

    if (i % 10000 == 0) {
      DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", i, count);
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
  //   DINGO_LOG(ERROR) << "generate random data failed, error=" << e.what();
  //   return;
  // }

  DINGO_LOG(INFO) << fmt::format("generate random seeds: {}/{}", count, count);

  for (uint32_t x = 0; x < cnt; x++) {
    auto start = std::chrono::steady_clock::now();
    {
      dingodb::pb::index::VectorAddRequest request;
      dingodb::pb::index::VectorAddResponse response;

      request.set_region_id(region_id);

      uint64_t real_start_id = start_id + x * step_count;
      for (int i = real_start_id; i < real_start_id + step_count; ++i) {
        auto* vector_with_id = request.add_vectors();
        vector_with_id->set_id(i);
        vector_with_id->mutable_vector()->set_dimension(dimension);
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        for (int j = 0; j < dimension; j++) {
          vector_with_id->mutable_vector()->add_float_values(random_seeds[(i - start_id) * dimension + j]);
        }

        if (FLAGS_with_scalar) {
          auto index = (i - start_id);
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

      butil::Status status = interaction->SendRequest("IndexService", "VectorAdd", request, response);
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

    DINGO_LOG(INFO) << "index : " << x << " : " << diff << " us, avg : " << static_cast<long double>(diff) / step_count
                    << " us";

    total += diff;
  }

  DINGO_LOG(INFO) << fmt::format("total : {} cost : {} (us) avg : {} us", count, total,
                                 static_cast<long double>(total) / count);

  out.close();
}

void SendVectorCalcDistance(ServerInteractionPtr interaction, [[maybe_unused]] uint64_t region_id, uint32_t dimension,
                            const std::string& alg_type, const std::string& metric_type, int32_t left_vector_size,
                            int32_t right_vector_size, bool is_return_normlize) {
  ::dingodb::pb::index::VectorCalcDistanceRequest request;
  ::dingodb::pb::index::VectorCalcDistanceResponse response;

  // if (dimension == 0) {
  //   DINGO_LOG(ERROR) << "step_count must be greater than 0";
  //   return;
  // }

  std::string real_alg_type = alg_type;
  std::string real_metric_type = metric_type;

  std::transform(real_alg_type.begin(), real_alg_type.end(), real_alg_type.begin(), ::tolower);
  std::transform(real_metric_type.begin(), real_metric_type.end(), real_metric_type.begin(), ::tolower);

  bool is_faiss = ("faiss" == real_alg_type);
  bool is_hnsw = ("hnsw" == real_alg_type);

  // if (!is_faiss && !is_hnsw) {
  //   DINGO_LOG(ERROR) << "invalid alg_type :  use faiss or hnsw!!!";
  //   return;
  // }

  bool is_l2 = ("l2" == real_metric_type);
  bool is_ip = ("ip" == real_metric_type);
  bool is_cosine = ("cosine" == real_metric_type);
  // if (!is_l2 && !is_ip && !is_cosine) {
  //   DINGO_LOG(ERROR) << "invalid metric_type :  use L2 or IP or cosine !!!";
  //   return;
  // }

  // if (left_vector_size <= 0) {
  //   DINGO_LOG(ERROR) << "left_vector_size <=0 : " << left_vector_size;
  //   return;
  // }

  // if (right_vector_size <= 0) {
  //   DINGO_LOG(ERROR) << "right_vector_size <=0 : " << left_vector_size;
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
  for (size_t i = 0; i < left_vector_size; i++) {
    ::dingodb::pb::common::Vector op_left_vector;
    for (uint32_t i = 0; i < dimension; i++) {
      op_left_vector.add_float_values(distrib(rng));
    }
    op_left_vectors.Add(std::move(op_left_vector));
  }

  // op right assignment
  for (size_t i = 0; i < right_vector_size; i++) {
    ::dingodb::pb::common::Vector op_right_vector;
    for (uint32_t i = 0; i < dimension; i++) {
      op_right_vector.add_float_values(distrib(rng));
    }
    op_right_vectors.Add(std::move(op_right_vector));  // NOLINT
  }

  request.set_algorithm_type(algorithm_type);
  request.set_metric_type(my_metric_type);
  request.set_is_return_normlize(is_return_normlize);
  request.mutable_op_left_vectors()->Add(op_left_vectors.begin(), op_left_vectors.end());
  request.mutable_op_right_vectors()->Add(op_right_vectors.begin(), op_right_vectors.end());

  DINGO_LOG(INFO) << "SendVectorCalcDistance request: " << request.DebugString();

  interaction->SendRequest("IndexService", "VectorCalcDistance", request, response);

  DINGO_LOG(INFO) << "SendVectorCalcDistance response: " << response.DebugString();
}

void SendKvGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key, std::string& value) {
  dingodb::pb::store::KvGetRequest request;
  dingodb::pb::store::KvGetResponse response;

  request.set_region_id(region_id);
  request.set_key(key);

  interaction->SendRequest("StoreService", "KvGet", request, response);

  value = response.value();
}

void SendKvBatchGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count) {
  dingodb::pb::store::KvBatchGetRequest request;
  dingodb::pb::store::KvBatchGetResponse response;

  request.set_region_id(region_id);
  for (int i = 0; i < count; ++i) {
    std::string key = prefix + Helper::GenRandomString(30);
    request.add_keys(key);
  }

  interaction->SendRequest("StoreService", "KvBatchGet", request, response);
}

int SendKvPut(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key, std::string value) {
  dingodb::pb::store::KvPutRequest request;
  dingodb::pb::store::KvPutResponse response;

  request.set_region_id(region_id);
  auto* kv = request.mutable_kv();
  kv->set_key(key);
  kv->set_value(value.empty() ? Helper::GenRandomString(64) : value);

  interaction->SendRequest("StoreService", "KvPut", request, response);
  return response.error().errcode();
}

void SendKvBatchPut(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count) {
  dingodb::pb::store::KvBatchPutRequest request;
  dingodb::pb::store::KvBatchPutResponse response;

  request.set_region_id(region_id);
  for (int i = 0; i < count; ++i) {
    std::string key = prefix + Helper::GenRandomString(30);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(Helper::GenRandomString(64));
  }

  interaction->SendRequest("StoreService", "KvBatchPut", request, response);
}

void SendKvPutIfAbsent(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key) {
  dingodb::pb::store::KvPutIfAbsentRequest request;
  dingodb::pb::store::KvPutIfAbsentResponse response;

  request.set_region_id(region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(key);
  kv->set_value(Helper::GenRandomString(64));

  interaction->SendRequest("StoreService", "KvPutIfAbsent", request, response);
}

void SendKvBatchPutIfAbsent(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix,
                            int count) {
  dingodb::pb::store::KvBatchPutIfAbsentRequest request;
  dingodb::pb::store::KvBatchPutIfAbsentResponse response;

  request.set_region_id(region_id);
  for (int i = 0; i < count; ++i) {
    std::string key = prefix + Helper::GenRandomString(30);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(Helper::GenRandomString(64));
  }

  interaction->SendRequest("StoreService", "KvBatchPutIfAbsent", request, response);
}

void SendKvBatchDelete(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key) {
  dingodb::pb::store::KvBatchDeleteRequest request;
  dingodb::pb::store::KvBatchDeleteResponse response;

  request.set_region_id(region_id);
  request.add_keys(key);

  interaction->SendRequest("StoreService", "KvBatchDelete", request, response);
}

void SendKvDeleteRange(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix) {
  dingodb::pb::store::KvDeleteRangeRequest request;
  dingodb::pb::store::KvDeleteRangeResponse response;

  request.set_region_id(region_id);
  request.mutable_range()->mutable_range()->set_start_key(prefix);
  request.mutable_range()->mutable_range()->set_end_key(dingodb::Helper::PrefixNext(prefix));
  request.mutable_range()->set_with_start(true);
  request.mutable_range()->set_with_end(false);

  interaction->SendRequest("StoreService", "KvDeleteRange", request, response);
  DINGO_LOG(INFO) << "delete count: " << response.delete_count();
}

void SendKvScan(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix) {
  dingodb::pb::store::KvScanBeginRequest request;
  dingodb::pb::store::KvScanBeginResponse response;

  request.set_region_id(region_id);
  request.mutable_range()->mutable_range()->set_start_key(prefix);
  request.mutable_range()->mutable_range()->set_end_key(dingodb::Helper::PrefixNext(prefix));
  request.mutable_range()->set_with_start(true);
  request.mutable_range()->set_with_end(false);

  interaction->SendRequest("StoreService", "KvScanBegin", request, response);
  if (response.error().errcode() != 0) {
    return;
  }

  dingodb::pb::store::KvScanContinueRequest continue_request;
  dingodb::pb::store::KvScanContinueResponse continue_response;
  continue_request.set_region_id(region_id);
  continue_request.set_scan_id(response.scan_id());
  int batch_size = 1000;
  continue_request.set_max_fetch_cnt(batch_size);

  int count = 0;
  for (;;) {
    interaction->SendRequest("StoreService", "KvScanContinue", continue_request, continue_response);
    if (continue_response.error().errcode() != 0) {
      return;
    }

    count += continue_response.kvs().size();
    if (continue_response.kvs().size() < batch_size) {
      break;
    }
  }

  DINGO_LOG(INFO) << "scan count: " << count;

  dingodb::pb::store::KvScanReleaseRequest release_request;
  dingodb::pb::store::KvScanReleaseResponse release_response;

  release_request.set_region_id(region_id);
  release_request.set_scan_id(response.scan_id());

  interaction->SendRequest("StoreService", "KvScanRelease", release_request, release_response);
}

void SendKvCompareAndSet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key) {
  dingodb::pb::store::KvCompareAndSetRequest request;
  dingodb::pb::store::KvCompareAndSetResponse response;

  request.set_region_id(region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(key);
  kv->set_value(Helper::GenRandomString(64));
  request.set_expect_value("");

  interaction->SendRequest("StoreService", "KvCompareAndSet", request, response);
}

void SendKvBatchCompareAndSet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix,
                              int count) {
  dingodb::pb::store::KvBatchCompareAndSetRequest request;
  dingodb::pb::store::KvBatchCompareAndSetResponse response;

  request.set_region_id(region_id);
  for (int i = 0; i < count; ++i) {
    std::string key = prefix + Helper::GenRandomString(30);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(Helper::GenRandomString(64));
  }

  for (int i = 0; i < count; i++) {
    request.add_expect_values("");
  }

  request.set_is_atomic(false);

  interaction->SendRequest("StoreService", "KvBatchCompareAndSet", request, response);
}

dingodb::pb::common::RegionDefinition BuildRegionDefinition(uint64_t region_id, const std::string& raft_group,
                                                            std::vector<std::string>& raft_addrs,
                                                            const std::string& start_key, const std::string& end_key) {
  dingodb::pb::common::RegionDefinition region_definition;
  region_definition.set_id(region_id);
  region_definition.set_epoch(1);
  region_definition.set_name(raft_group);
  dingodb::pb::common::Range* range = region_definition.mutable_range();
  range->set_start_key(start_key);
  range->set_end_key(end_key);

  int count = 0;
  for (auto& addr : raft_addrs) {
    std::vector<std::string> host_port_idx;
    butil::SplitString(addr, ':', &host_port_idx);

    auto* peer = region_definition.add_peers();
    peer->set_store_id(1000 + (++count));
    auto* raft_loc = peer->mutable_raft_location();
    raft_loc->set_host(host_port_idx[0]);
    raft_loc->set_port(std::stoi(host_port_idx[1]));
    if (host_port_idx.size() > 2) {
      raft_loc->set_port(std::stoi(host_port_idx[2]));
    }
  }

  return region_definition;
}

void SendAddRegion(ServerInteractionPtr interaction, uint64_t region_id, const std::string& raft_group,
                   std::vector<std::string> raft_addrs) {
  dingodb::pb::region_control::AddRegionRequest request;
  request.mutable_region()->CopyFrom(BuildRegionDefinition(region_id, raft_group, raft_addrs, "a", "z"));
  dingodb::pb::region_control::AddRegionResponse response;

  interaction->SendRequest("RegionControlService", "AddRegion", request, response);
}

void SendChangeRegion(ServerInteractionPtr interaction, uint64_t region_id, const std::string& raft_group,
                      std::vector<std::string> raft_addrs) {
  dingodb::pb::region_control::ChangeRegionRequest request;
  dingodb::pb::region_control::ChangeRegionResponse response;

  request.mutable_region()->CopyFrom(BuildRegionDefinition(region_id, raft_group, raft_addrs, "a", "z"));
  dingodb::pb::common::RegionDefinition* region = request.mutable_region();

  interaction->SendRequest("RegionControlService", "ChangeRegion", request, response);
}

void SendDestroyRegion(ServerInteractionPtr interaction, uint64_t region_id) {
  dingodb::pb::region_control::DestroyRegionRequest request;
  dingodb::pb::region_control::DestroyRegionResponse response;

  request.set_region_id(region_id);

  interaction->SendRequest("RegionControlService", "DestroyRegion", request, response);
}

void SendSnapshot(ServerInteractionPtr interaction, uint64_t region_id) {
  dingodb::pb::region_control::SnapshotRequest request;
  dingodb::pb::region_control::SnapshotResponse response;

  request.set_region_id(region_id);

  interaction->SendRequest("RegionControlService", "Snapshot", request, response);
}

void SendSnapshotVectorIndex(ServerInteractionPtr interaction, uint64_t vector_index_id) {
  dingodb::pb::region_control::SnapshotVectorIndexRequest request;
  dingodb::pb::region_control::SnapshotVectorIndexResponse response;

  request.set_vector_index_id(vector_index_id);

  interaction->SendRequest("RegionControlService", "SnapshotVectorIndex", request, response);
}

void SendTransferLeader(ServerInteractionPtr interaction, uint64_t region_id, const dingodb::pb::common::Peer& peer) {
  dingodb::pb::region_control::TransferLeaderRequest request;
  dingodb::pb::region_control::TransferLeaderResponse response;

  request.set_region_id(region_id);
  request.mutable_peer()->CopyFrom(peer);

  interaction->SendRequest("RegionControlService", "TransferLeader", request, response);
}

void SendTransferLeaderByCoordinator(ServerInteractionPtr interaction, uint64_t region_id, uint64_t leader_store_id) {
  dingodb::pb::coordinator::TransferLeaderRegionRequest request;
  dingodb::pb::coordinator::TransferLeaderRegionResponse response;

  request.set_region_id(region_id);
  request.set_leader_store_id(leader_store_id);

  interaction->SendRequest("CoordinatorService", "TransferLeaderRegion", request, response);
}

struct BatchPutGetParam {
  uint64_t region_id;
  int32_t req_num;
  int32_t thread_no;
  std::string prefix;
  ServerInteractionPtr interaction;

  std::map<std::string, std::shared_ptr<dingodb::pb::store::StoreService_Stub>> stubs;
};

void BatchPut(std::shared_ptr<Context> ctx) {
  std::vector<uint64_t> latencys;
  for (int i = 0; i < ctx->req_num; ++i) {
    std::string key = ctx->prefix + Helper::GenRandomStringV2(32);
    std::string value = Helper::GenRandomString(256);
    SendKvPut(ctx->store_interaction, ctx->region_id, key, value);
    latencys.push_back(ctx->store_interaction->GetLatency());
  }

  int64_t sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<uint64_t>(0));
  DINGO_LOG(INFO) << "Put average latency: " << sum / latencys.size() << " us";
}

bool QueryRegionIdByKey(dingodb::pb::meta::TableRange& table_range, const std::string& key, uint64_t& region_id) {
  for (const auto& item : table_range.range_distribution()) {
    const auto& range = item.range();
    if (key >= range.start_key() && key < range.end_key()) {
      region_id = item.id().entity_id();
      return true;
    }
  }

  DINGO_LOG(ERROR) << fmt::format("query region id by key failed, key {}", dingodb::Helper::StringToHex(key));

  return false;
}

void PrintTableRange(dingodb::pb::meta::TableRange& table_range) {
  DINGO_LOG(INFO) << "refresh route...";
  for (const auto& item : table_range.range_distribution()) {
    DINGO_LOG(INFO) << fmt::format("region {} range [{}-{})", item.id().entity_id(),
                                   dingodb::Helper::StringToHex(item.range().start_key()),
                                   dingodb::Helper::StringToHex(item.range().end_key()));
  }
}

void BatchPutTable(std::shared_ptr<Context> ctx) {
  auto table_range = SendGetTableRange(ctx->coordinator_interaction, ctx->table_id);
  PrintTableRange(table_range);

  std::vector<uint64_t> latencys;
  for (int i = 0; i < ctx->req_num; ++i) {
    std::string key = ctx->prefix + Helper::GenRandomStringV2(32);
    std::string value = Helper::GenRandomString(256);

    uint64_t region_id = 0;
    if (!QueryRegionIdByKey(table_range, key, region_id)) {
      break;
    }

    int ret = SendKvPut(ctx->store_interaction, region_id, key, value);
    if (ret == dingodb::pb::error::EKEY_OUT_OF_RANGE || ret == dingodb::pb::error::EREGION_REDIRECT) {
      bthread_usleep(1000 * 500);  // 500ms
      table_range = SendGetTableRange(ctx->coordinator_interaction, ctx->table_id);
      PrintTableRange(table_range);
    }

    latencys.push_back(ctx->store_interaction->GetLatency());
  }
}

void TestBatchPut(std::shared_ptr<Context> ctx) {
  std::vector<bthread_t> tids;
  tids.resize(ctx->thread_num);
  for (int i = 0; i < ctx->thread_num; ++i) {
    auto copy_ctx = ctx->Clone();
    copy_ctx->thread_no = i;

    if (bthread_start_background(
            &tids[i], nullptr,
            [](void* arg) -> void* {
              std::shared_ptr<Context> ctx(static_cast<Context*>(arg));

              LOG(INFO) << "========thread: " << ctx->thread_no;
              if (ctx->table_id > 0) {
                BatchPutTable(ctx);
              } else {
                BatchPut(ctx);
              }
              return nullptr;
            },
            copy_ctx.release()) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < ctx->thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void BatchPutGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int req_num) {
  auto dataset = Helper::GenDataset(prefix, req_num);

  std::vector<uint64_t> latencys;
  latencys.reserve(dataset.size());
  for (auto& [key, value] : dataset) {
    SendKvPut(interaction, region_id, key, value);

    latencys.push_back(interaction->GetLatency());
  }

  int64_t sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<uint64_t>(0));
  DINGO_LOG(INFO) << "Put average latency: " << sum / latencys.size() << " us";

  latencys.clear();
  for (auto& [key, expect_value] : dataset) {
    std::string value;
    SendKvGet(interaction, region_id, key, value);
    if (value != expect_value) {
      DINGO_LOG(INFO) << "Not match: " << key << " = " << value << " expected=" << expect_value;
    }
    latencys.push_back(interaction->GetLatency());
  }

  sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<uint64_t>(0));
  DINGO_LOG(INFO) << "Get average latency: " << sum / latencys.size() << " us";
}

void TestBatchPutGet(ServerInteractionPtr interaction, uint64_t region_id, int thread_num, int req_num,
                     const std::string& prefix) {
  std::vector<bthread_t> tids;
  tids.resize(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    BatchPutGetParam* param = new BatchPutGetParam;
    param->req_num = req_num;
    param->region_id = region_id;
    param->thread_no = i;
    param->interaction = interaction;
    param->prefix = prefix;

    if (bthread_start_background(
            &tids[i], nullptr,
            [](void* arg) -> void* {
              std::unique_ptr<BatchPutGetParam> param(static_cast<BatchPutGetParam*>(arg));

              LOG(INFO) << "========thread: " << param->thread_no;
              BatchPutGet(param->interaction, param->region_id, param->prefix, param->req_num);

              return nullptr;
            },
            param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

struct AddRegionParam {
  uint64_t start_region_id;
  int32_t region_count;
  std::string raft_group;
  int req_num;
  std::string prefix;

  std::vector<std::string> raft_addrs;
  ServerInteractionPtr interaction;
};

void* AdddRegionRoutine(void* arg) {
  std::unique_ptr<AddRegionParam> param(static_cast<AddRegionParam*>(arg));
  auto interaction = param->interaction;
  for (int i = 0; i < param->region_count; ++i) {
    dingodb::pb::region_control::AddRegionRequest request;
    request.mutable_region()->CopyFrom(
        BuildRegionDefinition(param->start_region_id + i, param->raft_group, param->raft_addrs, "a", "z"));
    dingodb::pb::region_control::AddRegionResponse response;

    interaction->AllSendRequest("StoreService", "AddRegion", request, response);

    bthread_usleep(3 * 1000 * 1000L);
  }

  return nullptr;
}

void BatchSendAddRegion(ServerInteractionPtr interaction, int start_region_id, int region_count, int thread_num,
                        const std::string& raft_group, std::vector<std::string>& raft_addrs) {
  int32_t step = region_count / thread_num;
  std::vector<bthread_t> tids;
  tids.resize(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    AddRegionParam* param = new AddRegionParam;
    param->start_region_id = start_region_id + i * step;
    param->region_count = (i + 1 == thread_num) ? region_count - i * step : step;
    param->raft_group = raft_group;
    param->interaction = interaction;
    param->raft_addrs = raft_addrs;

    if (bthread_start_background(&tids[i], nullptr, AdddRegionRoutine, param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void* OperationRegionRoutine(void* arg) {
  std::unique_ptr<AddRegionParam> param(static_cast<AddRegionParam*>(arg));
  auto interaction = param->interaction;

  bthread_usleep((Helper::GetRandInt() % 1000) * 1000L);
  for (int i = 0; i < param->region_count; ++i) {
    uint64_t region_id = param->start_region_id + i;

    // Create region
    {
      DINGO_LOG(INFO) << "======Create region " << region_id;
      dingodb::pb::region_control::AddRegionRequest request;
      request.mutable_region()->CopyFrom(
          BuildRegionDefinition(param->start_region_id + i, param->raft_group, param->raft_addrs, "a", "z"));
      dingodb::pb::region_control::AddRegionResponse response;

      interaction->AllSendRequest("StoreService", "AddRegion", request, response);
    }

    // Put/Get
    {
      bthread_usleep(3 * 1000 * 1000L);
      DINGO_LOG(INFO) << "======Put region " << region_id;
      BatchPutGet(interaction, region_id, param->prefix, param->req_num);
    }

    // Destroy region
    {
      bthread_usleep(3 * 1000 * 1000L);
      DINGO_LOG(INFO) << "======Delete region " << region_id;
      dingodb::pb::region_control::DestroyRegionRequest request;
      dingodb::pb::region_control::DestroyRegionResponse response;

      request.set_region_id(region_id);

      interaction->AllSendRequest("StoreService", "DestroyRegion", request, response);
    }

    bthread_usleep(1 * 1000 * 1000L);
  }

  return nullptr;
}

void TestRegionLifecycle(ServerInteractionPtr interaction, uint64_t region_id, const std::string& raft_group,
                         std::vector<std::string>& raft_addrs, int region_count, int thread_num, int req_num,
                         const std::string& prefix) {
  int32_t step = region_count / thread_num;
  std::vector<bthread_t> tids;
  tids.resize(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    AddRegionParam* param = new AddRegionParam;
    param->start_region_id = region_id + i * step;
    param->region_count = (i + 1 == thread_num) ? region_count - i * step : step;
    param->raft_addrs = raft_addrs;
    param->interaction = interaction;
    param->raft_group = raft_group;
    param->req_num = req_num;
    param->prefix = prefix;

    if (bthread_start_background(&tids[i], nullptr, OperationRegionRoutine, param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void TestDeleteRangeWhenTransferLeader(std::shared_ptr<Context> ctx, uint64_t region_id,
                                       int req_num,  // NOLINT (*unused)
                                       const std::string& prefix) {
  // put data
  DINGO_LOG(INFO) << "batch put...";
  // BatchPut(ctx->store_interaction, region_id, prefix, req_num);

  // transfer leader
  dingodb::pb::common::Peer new_leader_peer;
  auto region = SendQueryRegion(ctx->coordinator_interaction, region_id);
  for (const auto& peer : region.definition().peers()) {
    if (region.leader_store_id() != peer.store_id()) {
      new_leader_peer = peer;
    }
  }

  DINGO_LOG(INFO) << fmt::format("transfer leader {}:{}", new_leader_peer.raft_location().host(),
                                 new_leader_peer.raft_location().port());
  SendTransferLeader(ctx->store_interaction, region_id, new_leader_peer);

  // delete range
  DINGO_LOG(INFO) << "delete range...";
  SendKvDeleteRange(ctx->store_interaction, region_id, prefix);

  // scan data
  DINGO_LOG(INFO) << "scan...";
  SendKvScan(ctx->store_interaction, region_id, prefix);
}

// Create table / Put data / Get data / Destroy table
void* CreateAndPutAndGetAndDestroyTableRoutine(void* arg) {
  std::shared_ptr<Context> ctx(static_cast<Context*>(arg));

  DINGO_LOG(INFO) << "======= Create table " << ctx->table_name;
  uint64_t table_id = SendCreateTable(ctx->coordinator_interaction, ctx->table_name, ctx->partition_num);

  DINGO_LOG(INFO) << "======= Put/Get table " << ctx->table_name;
  int batch_count = ctx->req_num / kBatchSize + 1;
  for (int i = 0; i < batch_count; ++i) {
    auto table_range = SendGetTableRange(ctx->coordinator_interaction, table_id);

    for (const auto& range_dist : table_range.range_distribution()) {
      if (range_dist.leader().host().empty()) {
        bthread_usleep(1 * 1000 * 1000);
        continue;
      }
      uint64_t region_id = range_dist.id().entity_id();
      std::string prefix = range_dist.range().start_key();

      BatchPutGet(ctx->store_interaction, region_id, prefix, kBatchSize);
    }
  }

  DINGO_LOG(INFO) << "======= Drop table " << ctx->table_name;
  SendDropTable(ctx->coordinator_interaction, table_id);

  return nullptr;
}

dingodb::pb::common::StoreMap SendGetStoreMap(ServerInteractionPtr interaction) {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(1);

  interaction->SendRequest("CoordinatorService", "GetStoreMap", request, response);

  return response.storemap();
}

dingodb::pb::common::Region SendQueryRegion(ServerInteractionPtr interaction, uint64_t region_id) {
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  request.set_region_id(region_id);

  interaction->SendRequest("CoordinatorService", "QueryRegion", request, response);

  return response.region();
}

void SendChangePeer(ServerInteractionPtr interaction, const dingodb::pb::common::RegionDefinition& region_definition) {
  dingodb::pb::coordinator::ChangePeerRegionRequest request;
  dingodb::pb::coordinator::ChangePeerRegionResponse response;

  auto* mut_definition = request.mutable_change_peer_request()->mutable_region_definition();
  mut_definition->CopyFrom(region_definition);

  interaction->SendRequest("CoordinatorService", "ChangePeerRegion", request, response);
}

void SendSplitRegion(ServerInteractionPtr interaction, const dingodb::pb::common::RegionDefinition& region_definition) {
  dingodb::pb::coordinator::SplitRegionRequest request;
  dingodb::pb::coordinator::SplitRegionResponse response;

  request.mutable_split_request()->set_split_from_region_id(region_definition.id());

  // calc the mid value between start_vec and end_vec
  const auto& start_key = region_definition.range().start_key();
  const auto& end_key = region_definition.range().end_key();

  auto diff = dingodb::Helper::StringSubtract(start_key, end_key);
  auto half_diff = dingodb::Helper::StringDivideByTwo(diff);
  auto mid = dingodb::Helper::StringAdd(start_key, half_diff);
  auto real_mid = mid.substr(1, mid.size() - 1);

  DINGO_LOG(INFO) << fmt::format("split range: [{}, {}) diff: {} half_diff: {} mid: {} real_mid: {}",
                                 dingodb::Helper::StringToHex(start_key), dingodb::Helper::StringToHex(end_key),
                                 dingodb::Helper::StringToHex(diff), dingodb::Helper::StringToHex(half_diff),
                                 dingodb::Helper::StringToHex(mid), dingodb::Helper::StringToHex(real_mid));

  request.mutable_split_request()->set_split_watershed_key(real_mid);

  interaction->SendRequest("CoordinatorService", "SplitRegion", request, response);
}

std::string FormatPeers(dingodb::pb::common::RegionDefinition definition) {
  std::string str;
  for (const auto& peer : definition.peers()) {
    str +=
        fmt::format("{}:{}:{}", peer.raft_location().host(), peer.raft_location().port(), peer.raft_location().index());
    str += ",";
  }

  return str;
}

// Expand/Shrink/Split region
void* AutoExpandAndShrinkAndSplitRegion(void* arg) {
  std::shared_ptr<Context> ctx(static_cast<Context*>(arg));

  for (;;) {
    uint64_t table_id = SendGetTableByName(ctx->coordinator_interaction, ctx->table_name);
    if (table_id == 0) {
      DINGO_LOG(INFO) << fmt::format("table: {} table_id: {}", ctx->table_name, table_id);
      bthread_usleep(1 * 1000 * 1000);
      continue;
    }

    auto store_map = SendGetStoreMap(ctx->coordinator_interaction);
    auto table_range = SendGetTableRange(ctx->coordinator_interaction, table_id);

    // Traverse region
    for (const auto& range_dist : table_range.range_distribution()) {
      uint64_t region_id = range_dist.id().entity_id();
      if (region_id == 0) {
        DINGO_LOG(INFO) << fmt::format("Get table range failed, table: {} region_id: {}", ctx->table_name, region_id);
        continue;
      }

      auto region = SendQueryRegion(ctx->coordinator_interaction, region_id);
      if (region.id() == 0) {
        DINGO_LOG(INFO) << fmt::format("Get region failed, table: {} region_id: {}", ctx->table_name, region_id);
        continue;
      }

      DINGO_LOG(INFO) << fmt::format("region {} state {} row_count {} min_key {} max_key {} region_size {}", region_id,
                                     static_cast<int>(region.state()), region.metrics().row_count(),
                                     dingodb::Helper::StringToHex(region.metrics().min_key()),
                                     dingodb::Helper::StringToHex(region.metrics().max_key()),
                                     region.metrics().region_size());
      if (region.state() != dingodb::pb::common::RegionState::REGION_NORMAL) {
        continue;
      }

      // Expand region
      if (Helper::RandomChoice()) {
        // Traverse store, get add new peer.
        dingodb::pb::common::Peer expand_peer;
        for (const auto& store : store_map.stores()) {
          bool is_exist = false;
          for (const auto& peer : region.definition().peers()) {
            if (store.id() == peer.store_id()) {
              is_exist = true;
            }
          }

          // Store not exist at the raft group, may add peer.
          if (!is_exist && Helper::RandomChoice()) {
            expand_peer.set_store_id(store.id());
            expand_peer.set_role(dingodb::pb::common::PeerRole::VOTER);
            expand_peer.mutable_server_location()->CopyFrom(store.server_location());
            expand_peer.mutable_raft_location()->CopyFrom(store.raft_location());
            break;
          }
        }

        // Add new peer.
        if (expand_peer.store_id() != 0) {
          dingodb::pb::common::RegionDefinition region_definition;
          region_definition.CopyFrom(region.definition());
          *region_definition.add_peers() = expand_peer;
          DINGO_LOG(INFO) << fmt::format("======= Expand region {}/{} region {} peers {}", ctx->table_name, table_id,
                                         region.id(), FormatPeers(region_definition));

          // SendChangePeer(ctx->coordinator_interaction, region_definition);
        }
      } else {  // Shrink region
        if (region.definition().peers().size() <= 3) {
          continue;
        }

        dingodb::pb::common::Peer shrink_peer;
        for (const auto& peer : region.definition().peers()) {
          if (peer.store_id() != region.leader_store_id() && Helper::RandomChoice()) {
            shrink_peer = peer;
            break;
          }
        }

        if (shrink_peer.store_id() != 0) {
          dingodb::pb::common::RegionDefinition region_definition;
          region_definition.CopyFrom(region.definition());
          region_definition.mutable_peers()->Clear();
          for (const auto& peer : region.definition().peers()) {
            if (peer.store_id() != shrink_peer.store_id()) {
              *region_definition.add_peers() = peer;
            }
          }

          DINGO_LOG(INFO) << fmt::format("======= Shrink region {}/{} region {} peers {}", ctx->table_name, table_id,
                                         region.id(), FormatPeers(region_definition));

          // SendChangePeer(ctx->coordinator_interaction, region.definition());
        }
      }

      // Split region, when row count greater than 1 million.
      if (region.metrics().row_count() > 1 * 1000 * 1000) {
        SendSplitRegion(ctx->coordinator_interaction, region.definition());
      }
    }

    bthread_usleep(1 * 1000 * 1000);
  }

  return nullptr;
}

void AutoTest(std::shared_ptr<Context> ctx) {
  std::vector<std::function<void*(void*)>> funcs = {CreateAndPutAndGetAndDestroyTableRoutine,
                                                    AutoExpandAndShrinkAndSplitRegion};
  std::vector<bthread_t> tids;
  tids.resize(funcs.size());

  // Thread: Create table / Put data / Get data / Destroy table
  // Thread: Expand/Shrink/Split region
  // Thread: Random kill/launch Node
  for (int i = 0; i < funcs.size(); ++i) {
    int ret = bthread_start_background(&tids[i], nullptr, *funcs[i].target<void* (*)(void*)>(), ctx->Clone().release());
    if (ret != 0) {
      DINGO_LOG(ERROR) << "Create bthread failed, ret: " << ret;
      return;
    }
  }

  for (auto& tid : tids) {
    bthread_join(tid, nullptr);
  }
}

void AutoDropTable(std::shared_ptr<Context> ctx) {
  // Get all table
  auto table_ids = SendGetTablesBySchema(ctx->coordinator_interaction);
  DINGO_LOG(INFO) << "table nums: " << table_ids.size();

  // Drop table
  std::sort(table_ids.begin(), table_ids.end());
  for (int i = 0; i < ctx->req_num && i < table_ids.size(); ++i) {
    DINGO_LOG(INFO) << "Delete table: " << table_ids[i];
    SendDropTable(ctx->coordinator_interaction, table_ids[i]);
  }
}

}  // namespace client
