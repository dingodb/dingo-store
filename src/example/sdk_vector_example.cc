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

#include "common/logging.h"
#include "common/synchronization.h"
#include "glog/logging.h"
#include "sdk/client.h"
#include "sdk/status.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_get_index_metrics_task.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_index_cache.h"

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

static void PrepareVectorIndex() {
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

// TODO: remove
static void VectorIndexCacheSearch() {
  auto coordinator_proxy = std::make_shared<dingodb::sdk::CoordinatorProxy>();
  Status open = coordinator_proxy->Open(FLAGS_coordinator_url);
  CHECK(open.ok()) << "Fail to open coordinator_proxy, please check parameter --url=" << FLAGS_coordinator_url;

  dingodb::sdk::VectorIndexCache cache(*coordinator_proxy);
  {
    std::shared_ptr<dingodb::sdk::VectorIndex> index;
    Status got = cache.GetVectorIndexById(g_index_id, index);
    CHECK(got.ok()) << "Fail to get vector index, index_id:" << g_index_id << ", status:" << got.ToString();
    CHECK(index.get() != nullptr);
    CHECK_EQ(index->GetId(), g_index_id);
    CHECK_EQ(index->GetName(), g_index_name);
  }

  {
    std::shared_ptr<dingodb::sdk::VectorIndex> index;
    Status got = cache.GetVectorIndexByKey(dingodb::sdk::EncodeVectorIndexCacheKey(g_schema_id, g_index_name), index);
    CHECK(got.ok()) << "Fail to get vector index, index_name:" << g_index_name << ", status:" << got.ToString();
    CHECK(index.get() != nullptr);
    CHECK_EQ(index->GetId(), g_index_id);
    CHECK_EQ(index->GetName(), g_index_name);
  }

  {
    int64_t index_id{0};
    Status got = cache.GetIndexIdByKey(dingodb::sdk::EncodeVectorIndexCacheKey(g_schema_id, g_index_name), index_id);
    CHECK(got.ok()) << "Fail to get index_id, index_name" << g_index_name << ", status:" << got.ToString();
    CHECK_EQ(index_id, g_index_id);
  }

  {
    cache.RemoveVectorIndexById(g_index_id);
    {
      std::shared_ptr<dingodb::sdk::VectorIndex> index;
      Status got = cache.GetVectorIndexByKey(dingodb::sdk::EncodeVectorIndexCacheKey(g_schema_id, g_index_name), index);
      CHECK(got.ok()) << "Fail to get vector index, index_name:" << g_index_name << ", status:" << got.ToString();
      CHECK(index.get() != nullptr);
      CHECK_EQ(index->GetId(), g_index_id);
      CHECK_EQ(index->GetName(), g_index_name);
    }
  }
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
    DINGO_LOG(INFO) << "vector search result:" << r.ToString();
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

static void VectorQuey(bool use_index_name = false) {
  dingodb::sdk::QueryParam param;
  param.vector_ids = g_vector_ids;

  Status query;
  dingodb::sdk::QueryResult result;
  if (use_index_name) {
    query = g_vector_client->BatchQueryByIndexName(g_schema_id, g_index_name, param, result);
  } else {
    query = g_vector_client->BatchQueryByIndexId(g_index_id, param, result);
  }

  DINGO_LOG(INFO) << "vector query:" << query.ToString();
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

    DINGO_LOG(INFO) << "vector scan query:" << tmp.ToString() << ", result:" << result.ToString();
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

    DINGO_LOG(INFO) << "vector scan query:" << tmp.ToString() << ", result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.vectors[0].id, g_vector_ids[g_vector_ids.size() - 1]);
      CHECK_EQ(result.vectors[1].id, g_vector_ids[g_vector_ids.size() - 2]);
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
    VectorIndexCacheSearch();
    PrepareVectorClient();

    VectorAdd();
    VectorSearch();
    VectorQuey();
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
    VectorSearch(true);
    VectorQuey(true);
    VectorGetBorder(true);
    VectorScanQuery(true);
    VectorGetIndexMetrics(true);
    VectorCount(true);
    VectorDelete(true);
    VectorSearch(true);

    PostClean(true);
  }
}
