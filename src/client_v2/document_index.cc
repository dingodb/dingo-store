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

namespace client_v2 {

void SetUpDocumentIndexSubCommands(CLI::App& app) {}

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

void SendDocumentAdd(DocumentAddOptions const& opt) {
  dingodb::pb::document::DocumentAddRequest request;
  dingodb::pb::document::DocumentAddResponse response;

  if (opt.document_id <= 0) {
    DINGO_LOG(ERROR) << "document_id is invalid";
    return;
  }

  if (opt.document_text1.empty()) {
    DINGO_LOG(ERROR) << "document_text1 is empty";
    return;
  }

  if (opt.document_text2.empty()) {
    DINGO_LOG(ERROR) << "document_text2 is empty";
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

  if (opt.is_update) {
    request.set_is_update(true);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  butil::Status status =
      InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentAdd", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendDocumentDelete(DocumentDeleteOptions const& opt) {
  dingodb::pb::document::DocumentDeleteRequest request;
  dingodb::pb::document::DocumentDeleteResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  for (int i = 0; i < opt.count; ++i) {
    request.add_ids(i + opt.start_id);
  }

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentDelete", request, response);
  int success_count = 0;
  for (auto key_state : response.key_states()) {
    if (key_state) {
      ++success_count;
    }
  }

  DINGO_LOG(INFO) << "Response: " << response.DebugString();
}

void SendDocumentSearch(DocumentSearchOptions const& opt) {
  dingodb::pb::document::DocumentSearchRequest request;
  dingodb::pb::document::DocumentSearchResponse response;

  if (opt.query_string.empty()) {
    DINGO_LOG(ERROR) << "query_string is empty";
    return;
  }

  if (opt.topn == 0) {
    DINGO_LOG(ERROR) << "topn is 0";
    return;
  }

  auto* parameter = request.mutable_parameter();
  parameter->set_top_n(opt.topn);
  parameter->set_query_string(opt.query_string);
  parameter->set_without_scalar_data(opt.without_scalar);

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentSearch", request, response);

  DINGO_LOG(INFO) << "DocumentSearch response: " << response.DebugString();
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

  DINGO_LOG(INFO) << "DocumentBatchQuery response: " << response.DebugString();
}

void SendDocumentGetMaxId(DocumentGetMaxIdOptions const& opt) {  // NOLINT
  dingodb::pb::document::DocumentGetBorderIdRequest request;
  dingodb::pb::document::DocumentGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentGetBorderId", request, response);

  DINGO_LOG(INFO) << "DocumentGetBorderId response: " << response.DebugString();
}

void SendDocumentGetMinId(DocumentGetMinIdOptions const& opt) {  // NOLINT
  dingodb::pb::document::DocumentGetBorderIdRequest request;
  dingodb::pb::document::DocumentGetBorderIdResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);
  request.set_get_min(true);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentGetBorderId", request, response);

  DINGO_LOG(INFO) << "DocumentGetBorderId response: " << response.DebugString();
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

  DINGO_LOG(INFO) << "DocumentScanQuery response: " << response.DebugString()
                  << " documents count: " << response.documents_size();
}

int64_t SendDocumentCount(DocumentCountOptions const& opt) {
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

  return response.error().errcode() != 0 ? 0 : response.count();
}

void SendDocumentGetRegionMetrics(DocumentGetRegionMetricsOptions const& opt) {
  dingodb::pb::document::DocumentGetRegionMetricsRequest request;
  dingodb::pb::document::DocumentGetRegionMetricsResponse response;

  *(request.mutable_context()) = RegionRouter::GetInstance().GenConext(opt.region_id);

  InteractionManager::GetInstance().SendRequestWithContext("DocumentService", "DocumentGetRegionMetrics", request,
                                                           response);

  DINGO_LOG(INFO) << "DocumentGetRegionMetrics response: " << response.DebugString();
}

void SetUpDocumentDelete(CLI::App& app) {
  auto opt = std::make_shared<DocumentDeleteOptions>();
  auto* cmd = app.add_subcommand("DocumentDelete", "Document delete ")->group("Store Manager Commands");
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
  auto* cmd = app.add_subcommand("DocumentAdd", "Document add ")->group("Store Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--document_id", opt->document_id, "Request parameter document_id")->required();
  cmd->add_option("--document_text1", opt->document_text1, "Request parameter document_text1")->required();
  cmd->add_option("--document_text2", opt->document_text2, "Request parameter document_text2")->required();
  cmd->add_flag("--is_update", opt->is_update, "Request parameter is_update")->default_val(false);
  cmd->callback([opt]() { RunDocumentAdd(*opt); });
}

void RunDocumentAdd(DocumentAddOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentAdd(opt);
}

void SetUpDocumentSearch(CLI::App& app) {
  auto opt = std::make_shared<DocumentSearchOptions>();
  auto* cmd = app.add_subcommand("DocumentSearch", "Document search ")->group("Store Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--query_string", opt->query_string, "Request parameter query_string")->required();
  cmd->add_option("--topn", opt->topn, "Request parameter topn")->required();
  cmd->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")->default_val(false);
  cmd->callback([opt]() { RunDocumentSearch(*opt); });
}

void RunDocumentSearch(DocumentSearchOptions const& opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  client_v2::SendDocumentSearch(opt);
}

void SetUpDocumentBatchQuery(CLI::App& app) {
  auto opt = std::make_shared<DocumentBatchQueryOptions>();
  auto* cmd = app.add_subcommand("DocumentBatchQuery", "Document batch query ")->group("Store Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--document_id", opt->document_id, "Request parameter document id")->required();
  cmd->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")->default_val(false);
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
  auto* cmd = app.add_subcommand("DocumentScanQuery", "Document scan query ")->group("Store Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start id")->required();
  cmd->add_option("--end_id", opt->end_id, "Request parameter end id")->required();
  cmd->add_option("--limit", opt->limit, "Request parameter limit")->default_val(50);
  cmd->add_flag("--is_reverse", opt->is_reverse, "Request parameter is_reverse")->default_val(false);
  cmd->add_flag("--without_scalar", opt->without_scalar, "Request parameter without_scalar")->default_val(false);
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
  auto* cmd = app.add_subcommand("DocumentGetMaxId", "Document get max id ")->group("Store Manager Commands");
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
  auto* cmd = app.add_subcommand("DocumentGetMinId", "Document get min id ")->group("Store Manager Commands");
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
  auto* cmd = app.add_subcommand("DocumentCount", "Document count ")->group("Store Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")->required();
  cmd->add_option("--start_id", opt->start_id, "Request parameter start id")->required();
  cmd->add_option("--end_id", opt->end_id, "Request parameter end id")->required();
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
  auto* cmd =
      app.add_subcommand("DocumentGetRegionMetrics", "Document get region metrics ")->group("Store Manager Commands");
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
