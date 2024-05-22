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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/synchronization.h"
#include "glog/logging.h"
#include "sdk/client.h"
#include "sdk/status.h"
#include "sdk/types.h"
#include "sdk/vector.h"

using dingodb::sdk::Status;

DEFINE_string(coordinator_url, "", "coordinator url");
static std::shared_ptr<dingodb::sdk::Client> g_client;
static int64_t g_schema_id{2};
static int64_t g_index_id{0};
static std::string g_index_name = "example01";
static std::vector<int64_t> g_range_partition_seperator_ids{5, 10, 20};
static int32_t g_dimension = 2;
static dingodb::sdk::FlatParam g_flat_param(g_dimension, dingodb::sdk::MetricType::kL2);
static std::vector<int64_t> g_vector_ids;
static dingodb::sdk::VectorClient* g_vector_client;

static const dingodb::sdk::Type kDefaultType = dingodb::sdk::Type::kINT64;
static std::vector<std::string> g_scalar_col{"id", "fake_id"};
static std::vector<dingodb::sdk::Type> g_scalar_col_typ{kDefaultType, dingodb::sdk::Type::kDOUBLE};

static void PrepareVectorIndex() {
  dingodb::sdk::VectorIndexCreator* creator;
  Status built = g_client->NewVectorIndexCreator(&creator);
  CHECK(built.IsOK()) << "dingo creator build fail:" << built.ToString();
  CHECK_NOTNULL(creator);
  dingodb::ScopeGuard guard([&]() { delete creator; });

  dingodb::sdk::VectorScalarSchema schema;
  // NOTE: may be add more
  schema.cols.push_back({g_scalar_col[0], g_scalar_col_typ[0], true});
  schema.cols.push_back({g_scalar_col[1], g_scalar_col_typ[1], true});
  Status create = creator->SetSchemaId(g_schema_id)
                      .SetName(g_index_name)
                      .SetReplicaNum(3)
                      .SetRangePartitions(g_range_partition_seperator_ids)
                      .SetFlatParam(g_flat_param)
                      .SetScalarSchema(schema)
                      .Create(g_index_id);
  DINGO_LOG(INFO) << "Create index status: " << create.ToString() << ", index_id:" << g_index_id;
  sleep(20);
}

void PostClean(bool use_index_name = false) {
  Status tmp;
  if (use_index_name) {
    int64_t index_id;
    tmp = g_client->GetIndexId(g_schema_id, g_index_name, index_id);
    if (tmp.ok()) {
      CHECK_EQ(index_id, g_index_id);
      tmp = g_client->DropIndexByName(g_schema_id, g_index_name);
    }
  } else {
    tmp = g_client->DropIndex(g_index_id);
  }
  DINGO_LOG(INFO) << "drop index status: " << tmp.ToString() << ", index_id:" << g_index_id;
  delete g_vector_client;
  g_vector_ids.clear();
}

static void PrepareVectorClient() {
  dingodb::sdk::VectorClient* client;
  Status built = g_client->NewVectorClient(&client);
  CHECK(built.IsOK()) << "dingo vector client build fail:" << built.ToString();
  CHECK_NOTNULL(client);
  g_vector_client = client;
  CHECK_NOTNULL(g_vector_client);
}

static void VectorAdd(bool use_index_name = false) {
  std::vector<dingodb::sdk::VectorWithId> vectors;

  float delta = 0.1;
  for (const auto& id : g_range_partition_seperator_ids) {
    dingodb::sdk::Vector tmp_vector{dingodb::sdk::ValueType::kFloat, g_dimension};
    tmp_vector.float_values.push_back(1.0 + delta);
    tmp_vector.float_values.push_back(2.0 + delta);

    dingodb::sdk::VectorWithId tmp(id, std::move(tmp_vector));
    {
      {
        dingodb::sdk::ScalarValue scalar_value;
        scalar_value.type = kDefaultType;

        dingodb::sdk::ScalarField field;
        field.long_data = id;
        scalar_value.fields.push_back(field);

        tmp.scalar_data.insert(std::make_pair(g_scalar_col[0], scalar_value));
      }
      {
        dingodb::sdk::ScalarValue scalar_value;
        scalar_value.type = dingodb::sdk::kDOUBLE;

        dingodb::sdk::ScalarField field;
        field.double_data = id;
        scalar_value.fields.push_back(field);

        tmp.scalar_data.insert(std::make_pair(g_scalar_col[1], scalar_value));
      }
    }

    vectors.push_back(std::move(tmp));

    g_vector_ids.push_back(id);
    delta++;
  }
  Status add;
  if (use_index_name) {
    add = g_vector_client->AddByIndexName(g_schema_id, g_index_name, vectors, false, false);
  } else {
    add = g_vector_client->AddByIndexId(g_index_id, vectors, false, false);
  }

  DINGO_LOG(INFO) << "vector add:" << add.ToString();
}

static void VectorSearch(bool use_index_name = false) {
  std::vector<dingodb::sdk::VectorWithId> target_vectors;
  float init = 0.1f;
  for (int i = 0; i < 5; i++) {
    dingodb::sdk::Vector tmp_vector{dingodb::sdk::ValueType::kFloat, g_dimension};
    tmp_vector.float_values.clear();
    tmp_vector.float_values.push_back(init);
    tmp_vector.float_values.push_back(init);

    dingodb::sdk::VectorWithId tmp;
    tmp.vector = std::move(tmp_vector);
    target_vectors.push_back(std::move(tmp));

    init = init + 0.1;
  }

  dingodb::sdk::SearchParam param;
  param.topk = 2;
  param.with_scalar_data = true;
  // param.use_brute_force = true;
  param.extra_params.insert(std::make_pair(dingodb::sdk::kParallelOnQueries, 10));

  Status tmp;
  std::vector<dingodb::sdk::SearchResult> result;
  if (use_index_name) {
    tmp = g_vector_client->SearchByIndexName(g_schema_id, g_index_name, param, target_vectors, result);
  } else {
    tmp = g_vector_client->SearchByIndexId(g_index_id, param, target_vectors, result);
  }

  DINGO_LOG(INFO) << "vector search status: " << tmp.ToString();
  for (const auto& r : result) {
    DINGO_LOG(INFO) << "vector search result: " << r.ToString();
  }

  CHECK_EQ(result.size(), target_vectors.size());
  for (auto i = 0; i < result.size(); i++) {
    auto& search_result = result[i];
    if (!search_result.vector_datas.empty()) {
      CHECK_EQ(search_result.vector_datas.size(), param.topk);
    }
    const auto& vector_id = search_result.id;
    CHECK_EQ(vector_id.id, target_vectors[i].id);
    CHECK_EQ(vector_id.vector.Size(), target_vectors[i].vector.Size());
  }
}

static void VectorSearchUseExpr(bool use_index_name = false) {
  std::vector<dingodb::sdk::VectorWithId> target_vectors;
  float init = 0.1f;
  for (int i = 0; i < 5; i++) {
    dingodb::sdk::Vector tmp_vector{dingodb::sdk::ValueType::kFloat, g_dimension};
    tmp_vector.float_values.clear();
    tmp_vector.float_values.push_back(init);
    tmp_vector.float_values.push_back(init);

    dingodb::sdk::VectorWithId tmp;
    tmp.vector = std::move(tmp_vector);
    target_vectors.push_back(std::move(tmp));

    init = init + 0.1;
  }

  {
    dingodb::sdk::SearchParam param;
    {
      param.topk = 100;
      param.with_scalar_data = true;
      param.extra_params.insert(std::make_pair(dingodb::sdk::kParallelOnQueries, 10));

      std::string json_str =
          R"({
      "type": "operator",
      "operator": "and",
      "arguments": [
        {
          "type": "comparator",
          "comparator": "gte",
          "attribute": "id",
          "value": 5,
          "value_type": "INT64"
        },
        {
          "type": "comparator",
          "comparator": "lt",
          "attribute": "id",
          "value": 20,
          "value_type": "INT64"
        }
      ]
    }
  )";

      param.langchain_expr_json = json_str;
    }

    Status tmp;
    std::vector<dingodb::sdk::SearchResult> result;
    if (use_index_name) {
      tmp = g_vector_client->SearchByIndexName(g_schema_id, g_index_name, param, target_vectors, result);
    } else {
      tmp = g_vector_client->SearchByIndexId(g_index_id, param, target_vectors, result);
    }

    DINGO_LOG(INFO) << "vector search expr status: " << tmp.ToString();
    for (const auto& r : result) {
      DINGO_LOG(INFO) << "vector search expr result: " << r.ToString();
    }

    for (auto& search_result : result) {
      for (auto& distance : search_result.vector_datas) {
        const auto& vector_id = distance.vector_data.id;
        CHECK_GE(vector_id, 5);
        CHECK_LT(vector_id, 20);
      }
    }
  }

  {
    // schema type convert
    dingodb::sdk::SearchParam param;
    {
      param.topk = 100;
      param.with_scalar_data = true;
      param.extra_params.insert(std::make_pair(dingodb::sdk::kParallelOnQueries, 10));

      // fake_id schema type is double, int64 can convert to double
      std::string json_str =
          R"({
      "type": "operator",
      "operator": "and",
      "arguments": [
        {
          "type": "comparator",
          "comparator": "gte",
          "attribute": "fake_id",
          "value": 5,
          "value_type": "INT64"
        },
        {
          "type": "comparator",
          "comparator": "lt",
          "attribute": "fake_id",
          "value": 20,
          "value_type": "INT64"
        }
      ]
    }
  )";

      param.langchain_expr_json = json_str;
    }

    Status tmp;
    std::vector<dingodb::sdk::SearchResult> result;
    if (use_index_name) {
      tmp = g_vector_client->SearchByIndexName(g_schema_id, g_index_name, param, target_vectors, result);
    } else {
      tmp = g_vector_client->SearchByIndexId(g_index_id, param, target_vectors, result);
    }

    DINGO_LOG(INFO) << "vector search expr with schema convert status: " << tmp.ToString();
    for (const auto& r : result) {
      DINGO_LOG(INFO) << "vector search expr with schema convert result: " << r.ToString();
    }

    for (auto& search_result : result) {
      for (auto& distance : search_result.vector_datas) {
        const auto& vector_id = distance.vector_data.id;
        CHECK_GE(vector_id, 5);
        CHECK_LT(vector_id, 20);
      }
    }
  }
  {
    // schema type convert
    dingodb::sdk::SearchParam param;
    {
      param.topk = 100;
      param.with_scalar_data = true;
      param.extra_params.insert(std::make_pair(dingodb::sdk::kParallelOnQueries, 10));

      // id schema type is int64, double can't convert to int64
      std::string json_str =
          R"({
      "type": "operator",
      "operator": "and",
      "arguments": [
        {
          "type": "comparator",
          "comparator": "gte",
          "attribute": "id",
          "value": 5,
          "value_type": "DOUBLE"
        },
        {
          "type": "comparator",
          "comparator": "lt",
          "attribute": "id",
          "value": 20,
          "value_type": "DOUBLE"
        }
      ]
    }
  )";

      param.langchain_expr_json = json_str;
    }

    Status tmp;
    std::vector<dingodb::sdk::SearchResult> result;
    if (use_index_name) {
      tmp = g_vector_client->SearchByIndexName(g_schema_id, g_index_name, param, target_vectors, result);
    } else {
      tmp = g_vector_client->SearchByIndexId(g_index_id, param, target_vectors, result);
    }

    DINGO_LOG(INFO) << "vector search expr with schema can't convert status: " << tmp.ToString();
    CHECK(!tmp.ok());
  }
}

static void VectorQuey(bool use_index_name = false) {
  dingodb::sdk::QueryParam param;
  param.vector_ids = g_vector_ids;
  param.with_scalar_data = true;

  Status query;
  dingodb::sdk::QueryResult result;
  if (use_index_name) {
    query = g_vector_client->BatchQueryByIndexName(g_schema_id, g_index_name, param, result);
  } else {
    query = g_vector_client->BatchQueryByIndexId(g_index_id, param, result);
  }

  DINGO_LOG(INFO) << "vector query: " << query.ToString();
  DINGO_LOG(INFO) << "vector query result:" << result.ToString();
  CHECK_EQ(result.vectors.size(), g_vector_ids.size());
}

static void VectorGetBorder(bool use_index_name = false) {
  {
    // get max
    Status tmp;
    int64_t vector_id = 0;
    if (use_index_name) {
      tmp = g_vector_client->GetBorderByIndexName(g_schema_id, g_index_name, true, vector_id);
    } else {
      tmp = g_vector_client->GetBorderByIndexId(g_index_id, true, vector_id);
    }

    DINGO_LOG(INFO) << "vector get border:" << tmp.ToString() << ", max vecotor id:" << vector_id;
    if (tmp.ok()) {
      CHECK_EQ(vector_id, g_vector_ids[g_vector_ids.size() - 1]);
    }
  }

  {
    // get min
    Status tmp;
    int64_t vector_id = 0;
    if (use_index_name) {
      tmp = g_vector_client->GetBorderByIndexName(g_schema_id, g_index_name, false, vector_id);
    } else {
      tmp = g_vector_client->GetBorderByIndexId(g_index_id, false, vector_id);
    }

    DINGO_LOG(INFO) << "vector get border:" << tmp.ToString() << ", min vecotor id:" << vector_id;
    if (tmp.ok()) {
      CHECK_EQ(vector_id, g_vector_ids[0]);
    }
  }
}

static void VectorScanQuery(bool use_index_name = false) {
  {
    // forward
    dingodb::sdk::ScanQueryParam param;
    param.vector_id_start = g_vector_ids[0];
    param.vector_id_end = g_vector_ids[g_vector_ids.size() - 1];
    param.max_scan_count = 2;

    dingodb::sdk::ScanQueryResult result;
    Status tmp;
    if (use_index_name) {
      tmp = g_vector_client->ScanQueryByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_vector_client->ScanQueryByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "vector forward scan query: " << tmp.ToString() << ", result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.vectors[0].id, g_vector_ids[0]);
      CHECK_EQ(result.vectors[1].id, g_vector_ids[1]);
    }
  }

  {
    // backward
    dingodb::sdk::ScanQueryParam param;
    param.vector_id_start = g_vector_ids[g_vector_ids.size() - 1];
    param.vector_id_end = g_vector_ids[0];
    param.max_scan_count = 2;
    param.is_reverse = true;

    dingodb::sdk::ScanQueryResult result;
    Status tmp;
    if (use_index_name) {
      tmp = g_vector_client->ScanQueryByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_vector_client->ScanQueryByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "vector backward scan query: " << tmp.ToString() << ", result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.vectors[0].id, g_vector_ids[g_vector_ids.size() - 1]);
      CHECK_EQ(result.vectors[1].id, g_vector_ids[g_vector_ids.size() - 2]);
    }
  }

  {
    // forward with scalar filter
    dingodb::sdk::ScanQueryParam param;
    param.vector_id_start = g_vector_ids[0];
    param.vector_id_end = g_vector_ids[g_vector_ids.size() - 1];
    param.with_scalar_data = true;
    param.use_scalar_filter = true;

    int64_t filter_id = 5;
    {
      dingodb::sdk::ScalarValue scalar_value;
      scalar_value.type = kDefaultType;

      dingodb::sdk::ScalarField field;
      field.long_data = filter_id;
      scalar_value.fields.push_back(field);
      param.scalar_data.insert(std::make_pair(g_scalar_col[0], scalar_value));
    }

    dingodb::sdk::ScanQueryResult result;
    Status tmp;
    if (use_index_name) {
      tmp = g_vector_client->ScanQueryByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_vector_client->ScanQueryByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "vector forward scan query with filter:" << tmp.ToString() << ", result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.vectors[0].id, filter_id);
    }
  }
}

static void VectorGetIndexMetrics(bool use_index_name = false) {
  Status tmp;
  dingodb::sdk::IndexMetricsResult result;
  if (use_index_name) {
    tmp = g_vector_client->GetIndexMetricsByIndexName(g_schema_id, g_index_name, result);
  } else {
    tmp = g_vector_client->GetIndexMetricsByIndexId(g_index_id, result);
  }

  DINGO_LOG(INFO) << "vector get index metrics:" << tmp.ToString() << ", result :" << result.ToString();
  if (tmp.ok()) {
    CHECK_EQ(result.index_type, dingodb::sdk::VectorIndexType::kFlat);
    CHECK_EQ(result.count, g_vector_ids.size());
    CHECK_EQ(result.deleted_count, 0);
    CHECK_EQ(result.max_vector_id, g_vector_ids[g_vector_ids.size() - 1]);
    CHECK_EQ(result.min_vector_id, g_vector_ids[0]);
  }
}

static void VectorCount(bool use_index_name = false) {
  {
    Status tmp;
    int64_t result{0};
    if (use_index_name) {
      tmp = g_vector_client->CountByIndexName(g_schema_id, g_index_name, 0, g_vector_ids[g_vector_ids.size() - 1] + 1,
                                              result);
    } else {
      tmp = g_vector_client->CountByIndexId(g_index_id, 0, g_vector_ids[g_vector_ids.size() - 1] + 1, result);
    }

    DINGO_LOG(INFO) << "vector count:" << tmp.ToString() << ", result :" << result;
    if (tmp.ok()) {
      CHECK_EQ(result, g_vector_ids.size());
    }
  }

  {
    Status tmp;
    int64_t result{0};
    int64_t start_vector_id = g_vector_ids[g_vector_ids.size() - 1] + 1;
    int64_t end_vector_id = start_vector_id + 1;
    if (use_index_name) {
      tmp = g_vector_client->CountByIndexName(g_schema_id, g_index_name, start_vector_id, end_vector_id, result);
    } else {
      tmp = g_vector_client->CountByIndexId(g_index_id, start_vector_id, end_vector_id, result);
    }

    DINGO_LOG(INFO) << "vector count:" << tmp.ToString() << ", result :" << result;
    if (tmp.ok()) {
      CHECK_EQ(result, 0);
    }
  }

  {
    Status tmp;
    int64_t result{0};
    if (use_index_name) {
      tmp = g_vector_client->CountByIndexName(g_schema_id, g_index_name, g_vector_ids[0],
                                              g_vector_ids[g_vector_ids.size() - 1], result);
    } else {
      tmp = g_vector_client->CountByIndexId(g_index_id, g_vector_ids[0], g_vector_ids[g_vector_ids.size() - 1], result);
    }

    DINGO_LOG(INFO) << "vector count:" << tmp.ToString() << ", result :" << result;
    if (tmp.ok()) {
      CHECK_EQ(result, g_vector_ids.size() - 1);
    }
  }
}

static void VectorDelete(bool use_index_name = false) {
  Status tmp;
  std::vector<dingodb::sdk::DeleteResult> result;
  if (use_index_name) {
    tmp = g_vector_client->DeleteByIndexName(g_schema_id, g_index_name, g_vector_ids, result);
  } else {
    tmp = g_vector_client->DeleteByIndexId(g_index_id, g_vector_ids, result);
  }
  DINGO_LOG(INFO) << "vector delete status: " << tmp.ToString();
  for (const auto& r : result) {
    DINGO_LOG(INFO) << "vector delete result:" << r.ToString();
  }
}

static void VectorAddWithAutoId(int64_t start_id) {
  std::vector<int64_t> vector_ids;
  Status add;
  {
    std::vector<dingodb::sdk::VectorWithId> vectors;

    float delta = 0.1;
    int64_t count = 5;
    for (auto id = start_id; id < start_id + count; id++) {
      dingodb::sdk::Vector tmp_vector{dingodb::sdk::ValueType::kFloat, g_dimension};
      tmp_vector.float_values.push_back(1.0 + delta);
      tmp_vector.float_values.push_back(2.0 + delta);

      dingodb::sdk::VectorWithId tmp(0, std::move(tmp_vector));
      vectors.push_back(std::move(tmp));

      vector_ids.push_back(id);

      delta++;
    }

    add = g_vector_client->AddByIndexId(g_index_id, vectors, false, false);

    DINGO_LOG(INFO) << "vector add:" << add.ToString();
  }

  {
    dingodb::sdk::ScanQueryParam param;
    param.vector_id_start = 1;
    param.vector_id_end = 100;
    param.max_scan_count = 100;

    dingodb::sdk::ScanQueryResult result;
    Status tmp = g_vector_client->ScanQueryByIndexId(g_index_id, param, result);

    DINGO_LOG(INFO) << "vector forward scan query: " << tmp.ToString() << ", result:" << result.ToString();
    if (tmp.ok()) {
      std::vector<int64_t> target_ids;
      target_ids.reserve(result.vectors.size());
      for (auto& vector : result.vectors) {
        target_ids.push_back(vector.id);
      }

      if (add.ok()) {
        // sort vecotor_ids and target_ids, and check equal
        std::sort(vector_ids.begin(), vector_ids.end());
        std::sort(target_ids.begin(), target_ids.end());
        CHECK(std::equal(vector_ids.begin(), vector_ids.end(), target_ids.begin(), target_ids.end()));
      }
    }
  }
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  // FLAGS_v = dingodb::kGlobalValueOfDebug;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_coordinator_url.empty()) {
    DINGO_LOG(ERROR) << "coordinator url is empty, try to use file://./coor_list";
    FLAGS_coordinator_url = "file://./coor_list";
  }

  dingodb::sdk::Client* tmp;
  Status built = dingodb::sdk::Client::Build(FLAGS_coordinator_url, &tmp);
  if (!built.ok()) {
    DINGO_LOG(ERROR) << "Fail to build client, please check parameter --url=" << FLAGS_coordinator_url;
    return -1;
  }
  CHECK_NOTNULL(tmp);
  g_client.reset(tmp);

  {
    PrepareVectorIndex();
    PrepareVectorClient();

    VectorAdd();
    VectorQuey();
    VectorSearch();
    VectorSearchUseExpr();
    VectorGetBorder();
    VectorScanQuery();
    VectorGetIndexMetrics();
    VectorCount();
    VectorDelete();
    VectorSearch();

    PostClean();
  }

  {
    PrepareVectorIndex();
    PrepareVectorClient();

    VectorAdd(true);
    VectorQuey(true);
    VectorSearch(true);
    VectorSearchUseExpr(true);
    VectorGetBorder(true);
    VectorScanQuery(true);
    VectorGetIndexMetrics(true);
    VectorCount(true);
    VectorDelete(true);
    VectorSearch(true);

    PostClean(true);
  }

  {
    int64_t start_id = 1;

    {
      dingodb::sdk::VectorIndexCreator* creator;
      Status built = g_client->NewVectorIndexCreator(&creator);
      CHECK(built.IsOK()) << "dingo creator build fail:" << built.ToString();
      CHECK_NOTNULL(creator);
      dingodb::ScopeGuard guard([&]() { delete creator; });

      Status create = creator->SetSchemaId(g_schema_id)
                          .SetName(g_index_name)
                          .SetReplicaNum(3)
                          .SetRangePartitions(g_range_partition_seperator_ids)
                          .SetFlatParam(g_flat_param)
                          .SetAutoIncrementStart(start_id)
                          .Create(g_index_id);
      DINGO_LOG(INFO) << "Create index status: " << create.ToString() << ", index_id:" << g_index_id;
      sleep(20);
    }

    PrepareVectorClient();
    VectorAddWithAutoId(start_id);
    PostClean(true);
  }
}
