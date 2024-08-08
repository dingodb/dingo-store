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

#ifndef DINGODB_CLIENT_PRETTY_H_
#define DINGODB_CLIENT_PRETTY_H_

#include <cstdint>
#include <string>
#include <vector>

#include "butil/status.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/debug.pb.h"
#include "proto/document.pb.h"
#include "proto/index.pb.h"
#include "proto/meta.pb.h"

namespace client_v2 {

class Pretty {
 public:
  struct TenantInfo {
    int64_t id;
    std::string name;
    std::string comment;
    int64_t create_time;
    int64_t update_time;
  };

  static bool ShowError(const butil::Status &status);
  static bool ShowError(const dingodb::pb::error::Error &error);

  static void Show(dingodb::pb::coordinator::GetCoordinatorMapResponse &response);
  static void Show(dingodb::pb::coordinator::GetStoreMapResponse &response);

  static void Show(const dingodb::pb::debug::DumpRegionResponse::Data &data,
                   const dingodb::pb::meta::TableDefinition &table_definition = {},
                   const std::vector<std::string> &exclude_columns = {});
  static void Show(dingodb::pb::debug::DumpRegionResponse &response);

  static void Show(std::vector<TenantInfo> tenants);
  static void Show(dingodb::pb::store::TxnScanResponse &response);
  static void Show(dingodb::pb::store::TxnScanLockResponse &response);

  static void Show(dingodb::pb::meta::CreateIndexResponse &response);
  static void Show(dingodb::pb::document::DocumentSearchResponse &response);
  static void Show(dingodb::pb::document::DocumentBatchQueryResponse &response);
  static void Show(dingodb::pb::document::DocumentGetBorderIdResponse &response);
  static void Show(dingodb::pb::document::DocumentScanQueryResponse &response);
  static void Show(dingodb::pb::document::DocumentCountResponse &response);
  static void Show(dingodb::pb::document::DocumentGetRegionMetricsResponse &response);

  static void Show(const std::vector<dingodb::pb::common::Region> &regions);

  static void Show(const dingodb::pb::meta::TableDefinitionWithId &table_definition_with_id);
  static void Show(dingodb::pb::meta::TsoResponse &response);
  static void ShowSchemas(const std::vector<dingodb::pb::meta::Schema> &schemas);
  static void Show(dingodb::pb::meta::GetSchemasResponse &response);
  static void Show(dingodb::pb::meta::GetSchemaResponse &response);
  static void Show(dingodb::pb::meta::GetSchemaByNameResponse &response);

  static void Show(dingodb::pb::meta::GetTablesBySchemaResponse &response);

  static void Show(dingodb::pb::coordinator::GetGCSafePointResponse &response);
  static void Show(dingodb::pb::coordinator::GetTaskListResponse &response);

  static void Show(dingodb::pb::coordinator::GetExecutorMapResponse &response);
  static void Show(dingodb::pb::coordinator::QueryRegionResponse &response);
  static void Show(dingodb::pb::index::VectorGetBorderIdResponse &response);
  static void Show(dingodb::pb::index::VectorCountResponse &response);
  static void ShowTotalCount(int64_t total_count);
  static void Show(dingodb::pb::index::VectorCalcDistanceResponse &response);
  static void Show(dingodb::pb::index::VectorGetRegionMetricsResponse &response);
};

}  // namespace client_v2

#endif  // DINGODB_CLIENT_PRETTY_H_