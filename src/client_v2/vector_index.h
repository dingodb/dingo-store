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

#ifndef DINGODB_CLIENT_VECTOR_INDEX_H_
#define DINGODB_CLIENT_VECTOR_INDEX_H_

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "CLI/CLI.hpp"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"

namespace client_v2 {

void SetUpVectorIndexSubCommands(CLI::App &app);

// vector operation
struct CreateIndexOptions {
  std::string coor_url;
  std::string name;
  int64_t schema_id;
  int32_t part_count;
  int64_t replica;
  bool with_auto_increment;
  bool with_scalar_schema;
  std::string vector_index_type;
  int32_t dimension;
  std::string metrics_type;
  int32_t max_elements;
  int32_t efconstruction;
  int32_t nlinks;
  int ncentroids;
  int nbits_per_idx;
  int nsubvector;
};
void SetUpCreateIndex(CLI::App &app);
void RunCreateIndex(CreateIndexOptions const &opt);

struct VectorSearchOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int32_t topn;
  std::string vector_data;
  std::string key;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  bool with_scalar_pre_filter;
  bool with_table_pre_filter;
  std::string scalar_filter_key;
  std::string scalar_filter_value;
  std::string scalar_filter_key2;
  std::string scalar_filter_value2;
  bool with_scalar_post_filter;
  int64_t ef_search;
  bool bruteforce;
  bool print_vector_search_delay;
  std::string csv_output;
};
void SetUpVectorSearch(CLI::App &app);
void RunVectorSearch(VectorSearchOptions const &opt);

struct VectorSearchDebugOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int32_t topn;
  int64_t start_vector_id;
  int32_t batch_count;
  std::string key;
  std::string value;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  int32_t vector_ids_count;
  bool with_scalar_pre_filter;
  bool with_scalar_post_filter;
  bool print_vector_search_delay;
};
void SetUpVectorSearchDebug(CLI::App &app);
void RunVectorSearchDebug(VectorSearchDebugOptions const &opt);

struct VectorRangeSearchOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  double radius;
  std::string key;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  bool with_scalar_pre_filter;
  bool with_scalar_post_filter;
  bool print_vector_search_delay;
};
void SetUpVectorRangeSearch(CLI::App &app);
void RunVectorRangeSearch(VectorRangeSearchOptions const &opt);

struct VectorRangeSearchDebugOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  double radius;
  int64_t start_vector_id;
  int32_t batch_count;
  std::string key;
  std::string value;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  int32_t vector_ids_count;
  bool with_scalar_pre_filter;
  bool with_scalar_post_filter;
  bool print_vector_search_delay;
};
void SetUpVectorRangeSearchDebug(CLI::App &app);
void RunVectorRangeSearchDebug(VectorRangeSearchDebugOptions const &opt);

struct VectorBatchSearchOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int32_t topn;
  int32_t batch_count;
  std::string key;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  bool with_scalar_pre_filter;
  bool with_scalar_post_filter;
  bool print_vector_search_delay;
};
void SetUpVectorBatchSearch(CLI::App &app);
void RunVectorBatchSearch(VectorBatchSearchOptions const &opt);

struct VectorBatchQueryOptions {
  std::string coor_url;
  int64_t region_id;
  std::vector<int64_t> vector_ids;
  std::string key;
  bool without_vector;
  bool without_scalar;
  bool without_table;
};
void SetUpVectorBatchQuery(CLI::App &app);
void RunVectorBatchQuery(VectorBatchQueryOptions const &opt);

struct VectorScanQueryOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
  int64_t limit;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool is_reverse;
  std::string key;
  std::string scalar_filter_key;
  std::string scalar_filter_value;
  std::string scalar_filter_key2;
  std::string scalar_filter_value2;
};
void SetUpVectorScanQuery(CLI::App &app);
void RunVectorScanQuery(VectorScanQueryOptions const &opt);

struct VectorScanDumpOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
  int64_t limit;
  bool is_reverse;
  std::string csv_output;
};
void SetUpVectorScanDump(CLI::App &app);
void RunVectorScanDump(VectorScanDumpOptions const &opt);

struct VectorGetRegionMetricsOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpVectorGetRegionMetrics(CLI::App &app);
void RunVectorGetRegionMetricsd(VectorGetRegionMetricsOptions const &opt);

struct VectorAddOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t region_id;
  int32_t dimension;
  int64_t start_id;
  int32_t count;
  int32_t step_count;
  bool without_scalar;
  bool without_table;
  std::string csv_data;
  std::string json_data;

  std::string scalar_filter_key;
  std::string scalar_filter_value;
  std::string scalar_filter_key2;
  std::string scalar_filter_value2;
};
void SetUpVectorAdd(CLI::App &app);
void RunVectorAdd(VectorAddOptions const &opt);

struct VectorDeleteOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int32_t count;
};
void SetUpVectorDelete(CLI::App &app);
void RunVectorDelete(VectorDeleteOptions const &opt);

struct VectorGetMaxIdOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpVectorGetMaxId(CLI::App &app);
void RunVectorGetMaxId(VectorGetMaxIdOptions const &opt);

struct VectorGetMinIdOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpVectorGetMinId(CLI::App &app);
void RunVectorGetMinId(VectorGetMinIdOptions const &opt);

struct VectorAddBatchOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int64_t start_id;
  int32_t count;
  int32_t step_count;
  std::string vector_index_add_cost_file;
  bool without_scalar;
};
void SetUpVectorAddBatch(CLI::App &app);
void RunVectorAddBatch(VectorAddBatchOptions const &opt);

struct VectorAddBatchDebugOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int64_t start_id;
  int32_t count;
  int32_t step_count;
  std::string vector_index_add_cost_file;
  bool without_scalar;
};
void SetUpVectorAddBatchDebug(CLI::App &app);
void RunVectorAddBatchDebug(VectorAddBatchDebugOptions const &opt);

struct VectorCalcDistanceOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  std::string alg_type;
  std::string metric_type;
  int32_t left_vector_size;
  int32_t right_vector_size;
  bool is_return_normlize;
};
void SetUpVectorCalcDistance(CLI::App &app);
void RunVectorCalcDistance(VectorCalcDistanceOptions const &opt);

struct CalcDistanceOptions {
  std::string vector_data1;
  std::string vector_data2;
};
void SetUpCalcDistance(CLI::App &app);
void RunCalcDistance(CalcDistanceOptions const &opt);

struct VectorCountOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
};
void SetUpVectorCount(CLI::App &app);
void RunVectorCount(VectorCountOptions const &opt);

struct CountVectorTableOptions {
  std::string coor_url;
  std::string store_addrs;
  int64_t table_id;
};
void SetUpCountVectorTable(CLI::App &app);
void RunCountVectorTable(CountVectorTableOptions const &opt);

// vector
void SendVectorSearch(VectorSearchOptions const &opt);
void SendVectorSearchDebug(VectorSearchDebugOptions const &opt);
void SendVectorRangeSearch(VectorRangeSearchOptions const &opt);
void SendVectorRangeSearchDebug(VectorRangeSearchDebugOptions const &opt);
void SendVectorBatchSearch(VectorBatchSearchOptions const &opt);
void SendVectorBatchQuery(VectorBatchQueryOptions const &opt);
void SendVectorAddRetry(VectorAddOptions const &opt);
void SendVectorAdd(VectorAddOptions const &opt);
void SendVectorDelete(VectorDeleteOptions const &opt);
void SendVectorGetMaxId(VectorGetMaxIdOptions const &opt);
void SendVectorGetMinId(VectorGetMinIdOptions const &opt);
void SendVectorAddBatch(VectorAddBatchOptions const &opt);
void SendVectorScanQuery(VectorScanQueryOptions const &opt);
void SendVectorScanDump(VectorScanDumpOptions const &opt);
void SendVectorAddBatchDebug(VectorAddBatchDebugOptions const &opt);
void SendVectorGetRegionMetrics(VectorGetRegionMetricsOptions const &opt);
void SendVectorCalcDistance(VectorCalcDistanceOptions const &opt);
void SendCalcDistance(CalcDistanceOptions const &opt);
int64_t SendVectorCount(VectorCountOptions const &opt, bool show);
void CountVectorTable(CountVectorTableOptions const &opt);

}  // namespace client_v2

#endif  // DINGODB_CLIENT_VECTOR_INDEX_H_