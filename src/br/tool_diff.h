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

#ifndef DINGODB_BR_TOOL_DIFF_H_
#define DINGODB_BR_TOOL_DIFF_H_

#include <functional>
#include <memory>

#include "br/parameter.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"

namespace br {

class ToolDiff : public std::enable_shared_from_this<ToolDiff> {
 public:
  ToolDiff(const ToolDiffParams& params);
  ~ToolDiff();

  ToolDiff(const ToolDiff&) = delete;
  const ToolDiff& operator=(const ToolDiff&) = delete;
  ToolDiff(ToolDiff&&) = delete;
  ToolDiff& operator=(ToolDiff&&) = delete;

  std::shared_ptr<ToolDiff> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  static butil::Status CheckParameterAndHandle(const std::string& br_diff_file, const std::string& br_diff_file_name,
                                               std::string& path_internal, std::string& file_name);
  static butil::Status CompareDirect(const std::string& path1, const std::string& file_name1, const std::string& path2,
                                     const std::string& file_name2, const std::string& desc,
                                     std::function<butil::Status(const std::string&, const std::string&, bool&,
                                                                 std::string&, std::string&, std::string&)>
                                         handler);

  static butil::Status CompareSst(const std::string& path1, const std::string& file_name1, const std::string& path2,
                                  const std::string& file_name2, const std::string& desc,
                                  std::function<butil::Status(const std::string&, const std::string&, bool&,
                                                              std::string&, std::string&, std::string&)>
                                      handler);

  static butil::Status CompareDirectFunction(const std::string& path1, const std::string& path2, bool& is_same,
                                             std::string& compare_content, std::string& content1,
                                             std::string& content2);
  static butil::Status CompareBackupMetaFunction(const std::string& path1, const std::string& path2, bool& is_same,
                                                 std::string& compare_content, std::string& content1,
                                                 std::string& content2);
  static butil::Status ReadSstForBackupMeta(
      const std::string& path, dingodb::pb::common::BackupParam& backup_param, bool& backup_param_exist,
      dingodb::pb::common::BackupMeta& backup_meta_schema, bool& backup_meta_schema_exist,
      dingodb::pb::common::BackupMeta& backup_meta_data_file, bool& backup_meta_data_file_exist,
      dingodb::pb::meta::IdEpochTypeAndValue& id_epoch_type_and_value, bool& id_epoch_type_and_value_exist,
      dingodb::pb::meta::TableIncrementGroup& table_increment_group, bool& table_increment_group_exist);

  static butil::Status CompareBackupMetaDataFileFunction(const std::string& path1, const std::string& path2,
                                                         bool& is_same, std::string& compare_content,
                                                         std::string& content1, std::string& content2);

  static butil::Status ReadSstForBackupMetaDataFile(
      const std::string& path, dingodb::pb::common::BackupMeta& store_region_sql_data_sst,
      bool& store_region_sql_data_sst_exist, dingodb::pb::common::BackupMeta& store_cf_sst_meta_sql_data_sst,
      bool& store_cf_sst_meta_sql_data_sst_exist, dingodb::pb::common::BackupMeta& index_region_sql_data_sst,
      bool& index_region_sql_data_sst_exist, dingodb::pb::common::BackupMeta& index_cf_sst_meta_sql_data_sst,
      bool& index_cf_sst_meta_sql_data_sst_exist, dingodb::pb::common::BackupMeta& document_region_sql_data_sst,
      bool& document_region_sql_data_sst_exist, dingodb::pb::common::BackupMeta& document_cf_sst_meta_sql_data_sst,
      bool& document_cf_sst_meta_sql_data_sst_exist, dingodb::pb::common::BackupMeta& store_region_sdk_data_sst,
      bool& store_region_sdk_data_sst_exist, dingodb::pb::common::BackupMeta& store_cf_sst_meta_sdk_data_sst,
      bool& store_cf_sst_meta_sdk_data_sst_exist, dingodb::pb::common::BackupMeta& index_region_sdk_data_sst,
      bool& index_region_sdk_data_sst_exist, dingodb::pb::common::BackupMeta& index_cf_sst_meta_sdk_data_sst,
      bool& index_cf_sst_meta_sdk_data_sst_exist, dingodb::pb::common::BackupMeta& document_region_sdk_data_sst,
      bool& document_region_sdk_data_sst_exist, dingodb::pb::common::BackupMeta& document_cf_sst_meta_sdk_data_sst,
      bool& document_cf_sst_meta_sdk_data_sst_exist);

  static void CompareBackupMetaPb(const std::string& sst_name, const dingodb::pb::common::BackupMeta& backup_meta_1,
                                  const dingodb::pb::common::BackupMeta& backup_meta_2, bool& is_same,
                                  std::string& compare_content);

  static butil::Status CompareBackupMetaSchemaFunction(const std::string& path1, const std::string& path2,
                                                       bool& is_same, std::string& compare_content,
                                                       std::string& content1, std::string& content2);

  static butil::Status ReadSstForBackupMetaSchema(const std::string& path,
                                                  dingodb::pb::common::BackupMeta& store_cf_sst_meta_sql_meta_sst,
                                                  bool& store_cf_sst_meta_sql_meta_sst_exist,
                                                  dingodb::pb::common::BackupMeta& store_region_sql_meta_sst,
                                                  bool& store_region_sql_meta_sst_exist,
                                                  dingodb::pb::common::BackupMeta& coordinator_sdk_meta_sst,
                                                  bool& coordinator_sdk_meta_sst_exist);

  static butil::Status CompareCoordinatorSdkMetaFunction(const std::string& path1, const std::string& path2,
                                                         bool& is_same, std::string& compare_content,
                                                         std::string& content1, std::string& content2);

  static butil::Status ReadSstForCoordinatorSdkMeta(const std::string& path, dingodb::pb::meta::MetaALL& meta_all,
                                                    bool& meta_all_exist);

  static void CompareCoordinatorSdkMetaPb(const std::string& sst_name, const dingodb::pb::meta::MetaALL& meta_all_1,
                                          const dingodb::pb::meta::MetaALL& meta_all_2, bool& is_same,
                                          std::string& compare_content);

  static void CompareTenantPb(const std::string& name, const std::optional<dingodb::pb::meta::Tenant>& tenant_1,
                              const std::optional<dingodb::pb::meta::Tenant>& tenant_2, bool& is_same,
                              std::string& compare_content);

  static void CompareSchemasPb(const std::string& name, const std::optional<dingodb::pb::meta::Schemas>& schemas_1,
                               const std::optional<dingodb::pb::meta::Schemas>& schemas_2, bool& is_same,
                               std::string& compare_content);

  static void CompareSchemaPb(const std::string& name, const std::optional<dingodb::pb::meta::Schema>& schema_1,
                              const std::optional<dingodb::pb::meta::Schema>& schema_2, bool& is_same,
                              std::string& compare_content);

  static void CompareDingoCommonIdsPb(const std::string& name, const std::string& name2,
                                      const std::optional<std::vector<dingodb::pb::meta::DingoCommonId>>& common_ids_1,
                                      const std::optional<std::vector<dingodb::pb::meta::DingoCommonId>>& common_ids_2,
                                      bool& is_same, std::string& compare_content);

  static void CompareDingoCommonIdPb(const std::string& name, const std::string& name2,
                                     const std::optional<dingodb::pb::meta::DingoCommonId>& common_id_1,
                                     const std::optional<dingodb::pb::meta::DingoCommonId>& common_id_2, bool& is_same,
                                     std::string& compare_content);

  static void CompareTablesAndIndexesPb(const std::string& name,
                                        const std::optional<dingodb::pb::meta::TablesAndIndexes>& tables_and_indexes_1,
                                        const std::optional<dingodb::pb::meta::TablesAndIndexes>& tables_and_indexes_2,
                                        bool& is_same, std::string& compare_content);

  static void CompareIndexDefinitionWithIdPb(
      const std::string& name, const std::string& name2,
      const std::optional<dingodb::pb::meta::IndexDefinitionWithId>& index_definition_with_id_1,
      const std::optional<dingodb::pb::meta::IndexDefinitionWithId>& index_definition_with_id_2, bool& is_same,
      std::string& compare_content);

  static void CompareIndexDefinitionPb(const std::string& name, const std::string& name2,
                                       const std::optional<dingodb::pb::meta::IndexDefinition>& index_definition_1,
                                       const std::optional<dingodb::pb::meta::IndexDefinition>& index_definition_2,
                                       bool& is_same, std::string& compare_content);

  static butil::Status CompareRegionCfSst(
      const std::string& path1, const std::string& file_name1, const std::string& path2, const std::string& file_name2,
      const std::string& desc,
      std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&,
                                  std::string&, std::string&, std::string&)>
          handler);

  static butil::Status CompareRegionDefinitionSst(
      const std::string& path1, const std::string& file_name1, const std::string& path2, const std::string& file_name2,
      const std::string& desc,
      std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&,
                                  std::string&, std::string&, std::string&)>
          handler);

  static butil::Status CompareRegionCfFunction(const std::string& path1, const std::string& path2, bool& is_same,
                                               std::string& compare_content, std::string& content1,
                                               std::string& content2, std::string& same_content,
                                               std::string& diff_content);

  static butil::Status ReadSstForBackupDataFileValueSstMetaGroup(
      const std::string& path, std::map<std::string, dingodb::pb::common::BackupDataFileValueSstMetaGroup>& groups);

  static void CompareBackupDataFileValueSstMetaGroupPb(
      const std::string& name, const std::string& name2,
      const std::optional<dingodb::pb::common::BackupDataFileValueSstMetaGroup>& group_1,
      const std::optional<dingodb::pb::common::BackupDataFileValueSstMetaGroup>& group_2, bool& is_same,
      std::string& compare_content, std::string& same_content, std::string& diff_content);

  static void CompareBackupDataFileValueSstMetaPb(
      const std::string& name, const std::string& name2,
      const std::optional<dingodb::pb::common::BackupDataFileValueSstMeta>& group_1,
      const std::optional<dingodb::pb::common::BackupDataFileValueSstMeta>& group_2, bool& is_same,
      std::string& compare_content, bool& is_same_content);

  static butil::Status CompareRegionDefinitionFunction(const std::string& path1, const std::string& path2,
                                                       bool& is_same, std::string& compare_content,
                                                       std::string& content1, std::string& content2,
                                                       std::string& same_content, std::string& diff_content);

  static butil::Status ReadSstForRegion(const std::string& path,
                                        std::map<std::string, dingodb::pb::common::Region>& regions);

  static void CompareRegionPb(const std::string& name, const std::string& name2,
                              const std::optional<dingodb::pb::common::Region>& region_1,
                              const std::optional<dingodb::pb::common::Region>& region_2, bool& is_same,
                              std::string& compare_content, std::string& same_content, std::string& diff_content);

  static void CompareRegionDefinitionPb(const std::string& name, const std::string& name2,
                                        const std::optional<dingodb::pb::common::RegionDefinition>& definition_1,
                                        const std::optional<dingodb::pb::common::RegionDefinition>& definition_2,
                                        bool& is_same, std::string& compare_content, std::string& same_content,
                                        std::string& diff_content);

  static butil::Status ParseNode(const std::string& local_path, const std::string& path, const std::string& param_flag,
                                 std::string& node);

  static butil::Status CompareRegionDataSst(
      const std::string& path1, const std::string& file_name1, const std::string& path2, const std::string& file_name2,
      const std::string& desc,
      std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&,
                                  std::string&, std::string&, std::string&)>
          handler);

  static butil::Status CompareRegionDataFunction(const std::string& path1, const std::string& path2, bool& is_same,
                                                 std::string& compare_content, std::string& content1,
                                                 std::string& content2, std::string& same_content,
                                                 std::string& diff_content);

  static butil::Status ParseFileName(const std::string& file_name, std::string& region_id, std::string& region_cf,
                                     std::string& file_name_summary);

  static butil::Status ReadSstForRegionData(const std::string& path, std::map<std::string, std::string>& kvs);

  ToolDiffParams tool_diff_params_;
  std::string path_internal_1_;
  std::string file_name_1_;
  std::string path_internal_2_;
  std::string file_name_2_;
};

}  // namespace br

#endif  // DINGODB_BR_TOOL_DIFF_H_