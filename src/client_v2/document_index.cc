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

#include "client_v2/document_index.h"

#include <cstdint>

#include "client_v2/coordinator.h"
#include "client_v2/pretty.h"
#include "common/helper.h"
#include "fmt/format.h"

const int kBatchSize = 3;

namespace client_v2 {

void SetUpDocumentIndexSubCommands(CLI::App& app) {
  SetUpCreateDocumentIndex(app);
  SetUpDocumentAdd(app);
  SetUpDocumentBatchAdd(app);
  SetUpDocumentDelete(app);
  SetUpDocumentSearch(app);
  SetUpDocumentSearchAll(app);
  SetUpDocumentBatchQuery(app);
  SetUpDocumentScanQuery(app);
  SetUpDocumentGetMaxId(app);
  SetUpDocumentGetMinId(app);
  SetUpDocumentCount(app);
  SetUpDocumentGetRegionMetrics(app);
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

std::string ToRFC3339(const std::chrono::system_clock::time_point& time_point) {
  std::time_t time_t = std::chrono::system_clock::to_time_t(time_point);
  std::tm* tm_ptr = std::gmtime(&time_t);  // Get UTC time

  std::ostringstream oss;
  oss << std::put_time(tm_ptr, "%Y-%m-%dT%H:%M:%SZ");  // RFC 3339
  return oss.str();
}

void SendDocumentAdd(DocumentAddOptions const& opt) {
  dingodb::pb::document::DocumentAddRequest request;
  dingodb::pb::document::DocumentAddResponse response;

  if (opt.document_id <= 0) {
    std::cout << "document_id is invalid" << std::endl;
    return;
  }

  if (opt.document_text1.empty()) {
    std::cout << "document_text1 is empty" << std::endl;
    return;
  }

  if (opt.document_text2.empty()) {
    std::cout << "document_text2 is empty" << std::endl;
    return;
  }

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  auto* document = request.add_documents();
  document->set_id(opt.document_id);
  auto* document_data = document->mutable_document()->mutable_document_data();

  // col1 text
  {
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data(opt.document_text1);
    (*document_data)["col1"] = document_value1;
  }

  // col2 int64
  {
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
    document_value1.mutable_field_value()->set_long_data(opt.document_id);
    (*document_data)["col2"] = document_value1;
  }

  // col3 double
  {
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
    document_value1.mutable_field_value()->set_double_data(opt.document_id * 1.0);
    (*document_data)["col3"] = document_value1;
  }

  // col4 text
  {
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data(opt.document_text2);
    (*document_data)["col4"] = document_value1;
  }

  // col5 datetime
  {
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::DATETIME);
    auto now = std::chrono::system_clock::now();
    std::string time_str = ToRFC3339(now);
    document_value1.mutable_field_value()->set_datetime_data(time_str);
    std::cout << "doc_id: " << opt.document_id << " ,time_str:" << time_str << std::endl;
    (*document_data)["col5"] = document_value1;
  }
  // col6 bool
  {
    if (opt.document_bool == "true") {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::BOOL);
      document_value1.mutable_field_value()->set_bool_data(true);
      (*document_data)["col6"] = document_value1;
    } else if (opt.document_bool == "false") {
      dingodb::pb::common::DocumentValue document_value1;
      document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::BOOL);
      document_value1.mutable_field_value()->set_bool_data(false);
      (*document_data)["col6"] = document_value1;
    }
  }
  if (opt.is_update) {
    request.set_is_update(true);
  }

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentAdd", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Document add failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg() << std::endl;
    return;
  }
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }

  std::cout << "Document add success. success_count: " << success_count << std::endl;
}

void SendDocumentBatchAdd(DocumentBatchAddOptions const& opt) {
  int64_t total_count = 0;
  int64_t total_fail_count = 0;
  for (int64_t i = 0; i < opt.total_size; i += opt.batch_size) {
    dingodb::pb::document::DocumentAddRequest request;
    dingodb::pb::document::DocumentAddResponse response;

    if (opt.start_document_id <= 0) {
      std::cout << "document_id is invalid" << std::endl;
      return;
    }

    *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

    for (int64_t j = 0; j < opt.batch_size; ++j) {
      int64_t document_id = opt.start_document_id + i + j;
      auto* document = request.add_documents();
      document->set_id(document_id);
      auto* document_data = document->mutable_document()->mutable_document_data();

      // col1 text
      {
        dingodb::pb::common::DocumentValue document_value1;
        document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
        document_value1.mutable_field_value()->set_string_data(opt.text_prefix + Helper::GenRandomString(64));
        (*document_data)["col1"] = document_value1;
      }

      // col2 int64
      {
        dingodb::pb::common::DocumentValue document_value1;
        document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
        document_value1.mutable_field_value()->set_long_data(document_id);
        (*document_data)["col2"] = document_value1;
      }

      // col3 double
      {
        dingodb::pb::common::DocumentValue document_value1;
        document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
        document_value1.mutable_field_value()->set_double_data(document_id * 1.0);
        (*document_data)["col3"] = document_value1;
      }

      // col4 text
      {
        dingodb::pb::common::DocumentValue document_value1;
        document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
        document_value1.mutable_field_value()->set_string_data(opt.text_prefix + Helper::GenRandomString(64));
        (*document_data)["col4"] = document_value1;
      }

      // col5 datetime
      {
        dingodb::pb::common::DocumentValue document_value1;
        document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::DATETIME);
        auto now = std::chrono::system_clock::now();
        std::string time_str = ToRFC3339(now);
        document_value1.mutable_field_value()->set_string_data(time_str);
        (*document_data)["col5"] = document_value1;
      }
    }

    if (opt.is_update) {
      request.set_is_update(true);
    }

    butil::Status status =
        InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentAdd", request, response);
    if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
      std::cout << fmt::format(
                       "Document add failed, error: {} {}",
                       dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name(),
                       response.error().errmsg())
                << std::endl;
      return;
    }

    int fail_count = 0;
    for (auto key_state : response.key_states()) {
      if (!key_state) {
        ++fail_count;
      }
    }

    total_count += request.documents_size();
    total_fail_count += fail_count;

    if (fail_count > 0) {
      std::cout << "Document add fail, fail_count: " << fail_count << std::endl;
    }

    if (total_count % 1000 == 0) {
      std::cout << fmt::format("schedule: {}/{}", total_count, opt.total_size) << std::endl;
    }

    if (opt.wait_time_ms > 0) {
      bthread_usleep(opt.wait_time_ms * 1000L);
    }
  }

  std::cout << fmt::format("Total document count: {} fail count: {}", total_count, total_fail_count) << std::endl;
}

void SendDocumentDelete(DocumentDeleteOptions const& opt) {
  dingodb::pb::document::DocumentDeleteRequest request;
  dingodb::pb::document::DocumentDeleteResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (int i = 0; i < opt.count; ++i) {
    request.add_ids(i + opt.start_id);
  }

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentDelete", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Document delete failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg() << std::endl;
    return;
  }
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }
  std::cout << "Document delete success. success_count: " << success_count << std::endl;
}

void SendDocumentSearch(DocumentSearchOptions const& opt) {
  dingodb::pb::document::DocumentSearchRequest request;
  dingodb::pb::document::DocumentSearchResponse response;

  if (opt.query_string.empty()) {
    std::cout << "query_string is empty" << std::endl;
    return;
  }

  if (opt.topn == 0) {
    std::cout << "topn is 0" << std::endl;
    return;
  }
  std::cout << "len:" << opt.query_string.length() << ", query:" << opt.query_string << std::endl;

  auto* parameter = request.mutable_parameter();
  parameter->set_top_n(opt.topn);
  parameter->set_query_string(opt.query_string);
  parameter->set_without_scalar_data(opt.without_scalar);

  if (opt.doc_id > 0) {
    parameter->add_document_ids(opt.doc_id);
  }
  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentSearch", request, response);

  Pretty::Show(response);
}

void SendDocumentSearchAll(DocumentSearchOptions const& opt) {
  // dingodb::pb::document::DocumentSearchRequest request;
  // dingodb::pb::document::DocumentSearchResponse response;

  if (opt.query_string.empty()) {
    std::cout << "query_string is empty" << std::endl;
    return;
  }

  auto response = SendSearchAllByStreamMode(opt);
  std::cout << "search all documents response:" << response.DebugString() << std::endl;
  Pretty::Show(response);
}

dingodb::pb::document::DocumentSearchAllResponse SendSearchAllByStreamMode(DocumentSearchOptions const& opt) {
  dingodb::pb::document::DocumentSearchAllRequest request;
  dingodb::pb::document::DocumentSearchAllResponse response;
  auto* parameter = request.mutable_parameter();
  parameter->set_query_string(opt.query_string);
  parameter->set_without_scalar_data(opt.without_scalar);
  parameter->set_query_unlimited(true);
  request.mutable_stream_meta()->set_limit(kBatchSize);
  if (opt.doc_id > 0) {
    parameter->add_document_ids(opt.doc_id);
  }
  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  for (;;) {
    dingodb::pb::document::DocumentSearchAllResponse sub_response;
    // maybe current store interaction is not store node, so need reset.
    InteractionManager::GetInstance().ResetStoreInteraction();
    auto status = InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentSearchAll",
                                                                           request, sub_response);
    std::cout << "search all request: " << request.DebugString() << ", sub_response: " << sub_response.DebugString()
              << std::endl;
    if (!status.ok()) {
      response.mutable_error()->set_errcode(dingodb::pb::error::Errno(status.error_code()));
      response.mutable_error()->set_errmsg(status.error_str());
      break;
    }

    if (sub_response.error().errcode() != dingodb::pb::error::OK) {
      *response.mutable_error() = sub_response.error();
      break;
    }

    // set request stream id
    if (!sub_response.stream_meta().stream_id().empty()) {
      request.mutable_stream_meta()->set_stream_id(sub_response.stream_meta().stream_id());
    }

    // copy data
    for (int i = 0; i < sub_response.document_with_scores_size(); ++i) {
      response.add_document_with_scores()->Swap(&sub_response.mutable_document_with_scores()->at(i));
    }
    if (!sub_response.stream_meta().has_more()) {
      break;
    }
  }

  return response;
}

void SendDocumentBatchQuery(DocumentBatchQueryOptions const& opt) {
  dingodb::pb::document::DocumentBatchQueryRequest request;
  dingodb::pb::document::DocumentBatchQueryResponse response;
  auto document_ids = {static_cast<int64_t>(opt.document_id)};

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (auto document_id : document_ids) {
    request.add_document_ids(document_id);
  }

  if (opt.without_scalar) {
    request.set_without_scalar_data(true);
  }

  if (!opt.key.empty()) {
    auto* keys = request.mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentBatchQuery", request, response);

  Pretty::Show(response);
  // DINGO_LOG(INFO) << "DocumentBatchQuery response: " << response.DebugString();
}

void SendDocumentGetMaxId(DocumentGetMaxIdOptions const& opt) {  // NOLINT
  dingodb::pb::document::DocumentGetBorderIdRequest request;
  dingodb::pb::document::DocumentGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentGetBorderId", request, response);

  Pretty::Show(response);
}

void SendDocumentGetMinId(DocumentGetMinIdOptions const& opt) {  // NOLINT
  dingodb::pb::document::DocumentGetBorderIdRequest request;
  dingodb::pb::document::DocumentGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_get_min(true);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentGetBorderId", request, response);

  Pretty::Show(response);
}

void SendDocumentScanQuery(DocumentScanQueryOptions const& opt) {
  dingodb::pb::document::DocumentScanQueryRequest request;
  dingodb::pb::document::DocumentScanQueryResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_document_id_start(opt.start_id);
  request.set_document_id_end(opt.end_id);

  if (opt.limit > 0) {
    request.set_max_scan_count(opt.limit);
  } else {
    request.set_max_scan_count(10);
  }

  request.set_is_reverse_scan(opt.is_reverse);

  request.set_without_scalar_data(opt.without_scalar);
  if (!opt.key.empty()) {
    auto* keys = request.mutable_selected_keys()->Add();
    keys->assign(opt.key);
  }

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentScanQuery", request, response);

  // DINGO_LOG(INFO) << "DocumentScanQuery response: " << response.DebugString()
  //                 << " documents count: " << response.documents_size();
  Pretty::Show(response);
}

void SendDocumentCount(DocumentCountOptions const& opt) {
  dingodb::pb::document::DocumentCountRequest request;
  dingodb::pb::document::DocumentCountResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  if (opt.start_id > 0) {
    request.set_document_id_start(opt.start_id);
  }
  if (opt.end_id > 0) {
    request.set_document_id_end(opt.end_id);
  }

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentCount", request, response);
  Pretty::Show(response);
}

void SendDocumentGetRegionMetrics(DocumentGetRegionMetricsOptions const& opt) {
  dingodb::pb::document::DocumentGetRegionMetricsRequest request;
  dingodb::pb::document::DocumentGetRegionMetricsResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentGetRegionMetrics", request,
                                                           response);
  Pretty::Show(response);
}

void SetUpCreateDocumentIndex(CLI::App& app) {
  auto opt = std::make_shared<CreateDocumentIndexOptions>();
  auto* cmd = app.add_subcommand("CreateDocumentIndex", "Create document index ")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--name", opt->name, "Request parameter region name")->required();
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")->default_val(1);
  cmd->add_option("--with_auto_increment", opt->with_auto_increment, "Request parameter with_auto_increment")
      ->default_val(true)
      ->default_str("true");
  cmd->add_option("--use_json_parameter", opt->use_json_parameter, "Request parameter use_json_parameter")
      ->default_val(true)
      ->default_str("true");
  cmd->add_option("--replica", opt->replica, "Request parameter replica num, must greater than 0")->default_val(3);

  cmd->callback([opt]() { RunCreateDocumentIndex(*opt); });
}
void RunCreateDocumentIndex(CreateDocumentIndexOptions const& opt) {
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

  std::string multi_type_column_json =
      R"({"col1": { "tokenizer": { "type": "chinese"}}, "col2": { "tokenizer": {"type": "i64", "indexed": true }}, "col3": { "tokenizer": {"type": "f64", "indexed": true }}, "col4": { "tokenizer": {"type": "chinese"}}, "col5": { "tokenizer": {"type": "datetime", "indexed": true }}, "col6": { "tokenizer": {"type": "bool", "indexed": true }} })";

  // document index parameter
  index_definition->mutable_index_parameter()->set_index_type(dingodb::pb::common::IndexType::INDEX_TYPE_DOCUMENT);
  auto* document_index_parameter = index_definition->mutable_index_parameter()->mutable_document_index_parameter();

  if (opt.use_json_parameter) {
    document_index_parameter->set_json_parameter(multi_type_column_json);
  }

  auto* scalar_schema = document_index_parameter->mutable_scalar_schema();
  auto* field_col1 = scalar_schema->add_fields();
  field_col1->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
  field_col1->set_key("col1");
  auto* field_col2 = scalar_schema->add_fields();
  field_col2->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
  field_col2->set_key("col2");
  auto* field_col3 = scalar_schema->add_fields();
  field_col3->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
  field_col3->set_key("col3");
  auto* field_col4 = scalar_schema->add_fields();
  field_col4->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
  field_col4->set_key("col4");
  auto* field_col5 = scalar_schema->add_fields();
  field_col5->set_field_type(::dingodb::pb::common::ScalarFieldType::DATETIME);
  field_col5->set_key("col5");
  auto* field_col6 = scalar_schema->add_fields();
  field_col6->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
  field_col6->set_key("col6");

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
    index_definition->set_auto_increment(100);
  }

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateIndex",
                                                                                                  request, response);
  Pretty::Show(response);
}

void SetUpDocumentDelete(CLI::App& app) {
  auto opt = std::make_shared<DocumentDeleteOptions>();
  auto* cmd = app.add_subcommand("DocumentDelete", "Document delete")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start id")->required();
  cmd->add_option("--count", opt->count, "Request parameter start id")->default_val(50);
  cmd->callback([opt]() { RunDocumentDelete(*opt); });
}

void RunDocumentDelete(DocumentDeleteOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentDelete(opt);
}

void SetUpDocumentAdd(CLI::App& app) {
  auto opt = std::make_shared<DocumentAddOptions>();
  auto* cmd = app.add_subcommand("DocumentAdd", "Document add")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--document_id", opt->document_id, "Request parameter document_id")->required();
  cmd->add_option("--document_text1", opt->document_text1, "Request parameter document_text1")->required();
  cmd->add_option("--document_text2", opt->document_text2, "Request parameter document_text2")->required();
  cmd->add_option("--is_update", opt->is_update, "Request parameter is_update")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--document_bool", opt->document_bool,
                  "Request parameter document_bool can set true or false, empty means not set");
  cmd->callback([opt]() { RunDocumentAdd(*opt); });
}

void RunDocumentAdd(DocumentAddOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentAdd(opt);
}

void SetUpDocumentBatchAdd(CLI::App& app) {
  auto opt = std::make_shared<DocumentBatchAddOptions>();
  auto* cmd = app.add_subcommand("DocumentBatchAdd", "Document batch add")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--start_document_id", opt->start_document_id, "Request parameter start document id")->default_val(1);
  cmd->add_option("--total_size", opt->total_size, "Request parameter total size")->default_val(INT64_MAX);
  cmd->add_option("--batch_size", opt->batch_size, "Request parameter batch size")->default_val(1);
  cmd->add_option("--wait_time_ms", opt->batch_size, "Request parameter wait time(ms)")->default_val(0);
  cmd->add_option("--text_prefix", opt->text_prefix, "Request parameter text prefix")->required();
  cmd->add_option("--is_update", opt->is_update, "Request parameter is_update")
      ->default_val(false)
      ->default_str("false");
  cmd->callback([opt]() { RunDocumentBatchAdd(*opt); });
}

void RunDocumentBatchAdd(DocumentBatchAddOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentBatchAdd(opt);
}

void SetUpDocumentSearch(CLI::App& app) {
  auto opt = std::make_shared<DocumentSearchOptions>();
  auto* cmd = app.add_subcommand("DocumentSearch", "Document search")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--query_string", opt->query_string, "Request parameter query_string")->required();
  cmd->add_option("--topn", opt->topn, "Request parameter topn")->default_val(10);
  cmd->add_option("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--doc_id", opt->doc_id, "Request parameter alive id");
  cmd->callback([opt]() { RunDocumentSearch(*opt); });
}

void RunDocumentSearch(DocumentSearchOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentSearch(opt);
}

void SetUpDocumentSearchAll(CLI::App& app) {
  auto opt = std::make_shared<DocumentSearchOptions>();
  auto* cmd = app.add_subcommand("DocumentSearchAll", "Document search all documents")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--query_string", opt->query_string, "Request parameter query_string")->required();
  cmd->add_option("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--doc_id", opt->doc_id, "Request parameter alive id");
  cmd->callback([opt]() { RunDocumentSearchAll(*opt); });
}

void RunDocumentSearchAll(DocumentSearchOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentSearchAll(opt);
}

void SetUpDocumentBatchQuery(CLI::App& app) {
  auto opt = std::make_shared<DocumentBatchQueryOptions>();
  auto* cmd = app.add_subcommand("DocumentBatchQuery", "Document batch query")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--document_id", opt->document_id, "Request parameter document id")->required();
  cmd->add_option("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--key", opt->key, "Request parameter key");
  cmd->callback([opt]() { RunDocumentBatchQuery(*opt); });
}

void RunDocumentBatchQuery(DocumentBatchQueryOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentBatchQuery(opt);
}

void SetUpDocumentScanQuery(CLI::App& app) {
  auto opt = std::make_shared<DocumentScanQueryOptions>();
  auto* cmd = app.add_subcommand("DocumentScanQuery", "Document scan query")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start id")->required();
  cmd->add_option("--end_id", opt->end_id, "Request parameter end id")->required();
  cmd->add_option("--limit", opt->limit, "Request parameter limit")->default_val(50);
  cmd->add_option("--is_reverse", opt->is_reverse, "Request parameter is_reverse")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--without_scalar", opt->without_scalar, "Request parameter without_scalar")
      ->default_val(false)
      ->default_str("false");
  cmd->add_option("--key", opt->key, "Request parameter key");
  cmd->callback([opt]() { RunDocumentScanQuery(*opt); });
}

void RunDocumentScanQuery(DocumentScanQueryOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentScanQuery(opt);
}

void SetUpDocumentGetMaxId(CLI::App& app) {
  auto opt = std::make_shared<DocumentGetMaxIdOptions>();
  auto* cmd = app.add_subcommand("DocumentGetMaxId", "Document get max id")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunDocumentGetMaxId(*opt); });
}

void RunDocumentGetMaxId(DocumentGetMaxIdOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentGetMaxId(opt);
}

void SetUpDocumentGetMinId(CLI::App& app) {
  auto opt = std::make_shared<DocumentGetMinIdOptions>();
  auto* cmd = app.add_subcommand("DocumentGetMinId", "Document get min id")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunDocumentGetMinId(*opt); });
}

void RunDocumentGetMinId(DocumentGetMinIdOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentGetMinId(opt);
}

void SetUpDocumentCount(CLI::App& app) {
  auto opt = std::make_shared<DocumentCountOptions>();
  auto* cmd = app.add_subcommand("DocumentCount", "Document count")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start id");
  cmd->add_option("--end_id", opt->end_id, "Request parameter end id");
  cmd->callback([opt]() { RunDocumentCount(*opt); });
}

void RunDocumentCount(DocumentCountOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentCount(opt);
}

void SetUpDocumentGetRegionMetrics(CLI::App& app) {
  auto opt = std::make_shared<DocumentGetRegionMetricsOptions>();
  auto* cmd = app.add_subcommand("DocumentGetRegionMetrics", "Document get region metrics")->group("Document Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->callback([opt]() { RunDocumentGetRegionMetrics(*opt); });
}

void RunDocumentGetRegionMetrics(DocumentGetRegionMetricsOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentGetRegionMetrics(opt);
}

}  // namespace client_v2
