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
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "ftxui/dom/elements.hpp"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/debug.pb.h"
#include "proto/document.pb.h"
#include "proto/index.pb.h"
#include "proto/meta.pb.h"
#include "proto/store_internal.pb.h"

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

  struct ShowRegionTreeParam {
    using TableDefinitionWithId = dingodb::pb::meta::TableDefinitionWithId;
    using Region = dingodb::pb::store_internal::Region;
    using DebugResponse = dingodb::pb::debug::DebugResponse;

   public:
    ShowRegionTreeParam() = delete;
    ShowRegionTreeParam(std::vector<DebugResponse> in_responses, std::vector<int64_t> in_region_ids,
                        bool in_show_ancestor, bool in_dot_format);

    ~ShowRegionTreeParam() = default;

    ShowRegionTreeParam(const ShowRegionTreeParam &) = delete;
    ShowRegionTreeParam &operator=(const ShowRegionTreeParam &) = delete;

    ShowRegionTreeParam(ShowRegionTreeParam &&) = delete;
    ShowRegionTreeParam &operator=(ShowRegionTreeParam &&) = delete;

    void SetSchema(const dingodb::pb::meta::Schema &in_schema);
    void SetTableDefinitionWithId(const TableDefinitionWithId &in_table_def);
    void AddIndexDefinitionWithId(const TableDefinitionWithId &in_index_def);

   public:
    // input parameters (owns copies — no lifetime dependency on caller)
    bool show_ancestor;
    std::vector<int64_t> region_ids;
    bool dot_format;

    // input parameters (table/index info)
    dingodb::pb::meta::Schema schema;
    TableDefinitionWithId table_def;
    std::map<int64_t, TableDefinitionWithId> index_defs;

    // intermediate data built from response in constructor
    std::map<int64_t, const Region *> region_map;
    std::map<int64_t, std::vector<const Region *>> region_children;

   private:
    std::vector<DebugResponse> responses_;
  };

  struct ShowRegionPeersParam {
    using RaftMetaMap = std::map<int64_t, std::map<int64_t, dingodb::pb::store_internal::RaftMeta>>;
    using RaftLogMetaMap = std::map<int64_t, std::map<int64_t, dingodb::pb::debug::DebugResponse_RaftLogMeta>>;
    using RegionMetaDetailMap = std::map<int64_t, std::map<int64_t, dingodb::pb::store_internal::Region>>;

    // constructor
    ShowRegionPeersParam() = delete;
    ShowRegionPeersParam(std::vector<dingodb::pb::common::Region> in_regions, RaftMetaMap in_raft_meta_map,
                         RaftLogMetaMap in_raft_log_meta_map, RegionMetaDetailMap in_region_meta_detail_map,
                         std::map<int64_t, int64_t> in_table_index_replica_num_map, int64_t in_severe_lag_threshold,
                         int64_t in_warn_lag_threshold, bool in_issues_only)
        : regions(std::move(in_regions)),
          raft_meta_map(std::move(in_raft_meta_map)),
          raft_log_meta_map(std::move(in_raft_log_meta_map)),
          region_meta_detail_map(std::move(in_region_meta_detail_map)),
          table_index_replica_num_map(std::move(in_table_index_replica_num_map)),
          apply_lag_severe(in_severe_lag_threshold),
          apply_lag_warn(in_warn_lag_threshold),
          issues_only(in_issues_only) {}
    ~ShowRegionPeersParam() = default;

    std::vector<dingodb::pb::common::Region> regions;
    RaftMetaMap raft_meta_map;
    RaftLogMetaMap raft_log_meta_map;
    RegionMetaDetailMap region_meta_detail_map;
    std::map<int64_t, int64_t> table_index_replica_num_map;
    int64_t apply_lag_severe;
    int64_t apply_lag_warn;
    bool issues_only;
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
  static void Show(dingodb::pb::document::DocumentSearchAllResponse &response);
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
  static void Show(const dingodb::pb::debug::DebugResponse::GCMetrics &gc_metrics, bool include_region,
                   bool region_only = false);
  static void Show(dingodb::pb::coordinator::GetJobListResponse &response, bool is_interactive);

  static void Show(dingodb::pb::coordinator::GetExecutorMapResponse &response);
  static void Show(dingodb::pb::coordinator::QueryRegionResponse &response);
  static void Show(dingodb::pb::index::VectorGetBorderIdResponse &response);
  static void Show(dingodb::pb::index::VectorCountResponse &response);
  static void ShowTotalCount(int64_t total_count);
  static void Show(dingodb::pb::index::VectorCalcDistanceResponse &response);
  static void Show(dingodb::pb::index::VectorGetRegionMetricsResponse &response);
  static void Show(dingodb::pb::meta::GetTenantsResponse &response);
  static void Show(dingodb::pb::coordinator::CreateIdsResponse &response);
  static void Show(dingodb::pb::store::TxnScanResponse &response, bool calc_count);
  static void PrintTableInteractive(const std::vector<std::vector<ftxui::Element>> &rows);

  static void Show(const ShowRegionPeersParam &param);
  static void Show(const ShowRegionTreeParam &param);
};

}  // namespace client_v2

#endif  // DINGODB_CLIENT_PRETTY_H_