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

#include "br/tool_diff.h"

#include <google/protobuf/util/message_differencer.h>

#include <cstddef>
#include <memory>
#include <optional>

#include "br/sst_file_reader.h"
#include "br/tool_utils.h"
#include "br/utils.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"

namespace br {

ToolDiff::ToolDiff(const ToolDiffParams& params) : tool_diff_params_(params) {}

ToolDiff::~ToolDiff() = default;

std::shared_ptr<ToolDiff> ToolDiff::GetSelf() { return shared_from_this(); }

butil::Status ToolDiff::Init() {
  butil::Status status;

  status = ToolUtils::CheckParameterAndHandle(tool_diff_params_.br_diff_file1, "br_diff_file1", path_internal_1_,
                                              file_name_1_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ToolUtils::CheckParameterAndHandle(tool_diff_params_.br_diff_file2, "br_diff_file2", path_internal_2_,
                                              file_name_2_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::Run() {
  butil::Status status;
  std::string desc;

  // dump direct.  backup.lock;  backupmeta.encryption;  backupmeta.debug;
  if (kBackupFileLock == file_name_1_ || dingodb::Constant::kBackupMetaDebugName == file_name_1_ ||
      dingodb::Constant::kBackupMetaEncryptionName == file_name_1_) {
    if (file_name_1_ != file_name_2_) {
      std::string s = fmt::format("file name is not same. {} {}", file_name_1_, file_name_2_);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    if (kBackupFileLock == file_name_1_) {
      desc =
          "This file is used as a backup file, just to prevent multiple backup programs from backing up to the same "
          "directory";
    }

    if (dingodb::Constant::kBackupMetaDebugName == file_name_1_) {
      desc = "This file is used to store the backup meta debug information";
    }

    if (dingodb::Constant::kBackupMetaEncryptionName == file_name_1_) {
      desc = "This file is used to store the backup meta encryption information";
    }

    status = ToolDiff::CompareDirect(path_internal_1_, file_name_1_, path_internal_2_, file_name_2_, desc,
                                     CompareDirectFunction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }
  // dump sst. backupmeta; backupmeta.datafile; backupmeta.schema; coordinator_sdk_meta.sst;
  else if (dingodb::Constant::kBackupMetaName == file_name_1_ ||
           dingodb::Constant::kBackupMetaDataFileName == file_name_1_ ||
           dingodb::Constant::kBackupMetaSchemaName == file_name_1_ ||
           dingodb::Constant::kCoordinatorSdkMetaSstName == file_name_1_) {
    std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&,
                                std::string&)>
        handler = nullptr;

    if (file_name_1_ != file_name_2_) {
      std::string s = fmt::format("file name is not same. {} {}", file_name_1_, file_name_2_);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    if (dingodb::Constant::kBackupMetaName == file_name_1_) {
      desc = "This file is used to store the backup meta information";
      handler = ToolDiff::CompareBackupMetaFunction;
    }

    if (dingodb::Constant::kBackupMetaDataFileName == file_name_1_) {
      desc = "This file is used to store the backup meta data information";
      handler = ToolDiff::CompareBackupMetaDataFileFunction;
    }

    if (dingodb::Constant::kBackupMetaSchemaName == file_name_1_) {
      desc = "This file is used to store the backup meta schema information";
      handler = ToolDiff::CompareBackupMetaSchemaFunction;
    }

    if (dingodb::Constant::kCoordinatorSdkMetaSstName == file_name_1_) {
      desc = "This file is used to store the coordinator sdk meta information";
      handler = ToolDiff::CompareCoordinatorSdkMetaFunction;
    }

    status = ToolDiff::CompareDirect(path_internal_1_, file_name_1_, path_internal_2_, file_name_2_, desc, handler);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }
  // diff region cf sst.
  // store_cf_sst_meta_sql_meta.sst;
  // store_cf_sst_meta_sdk_data.sst; store_cf_sst_meta_sql_data.sst;
  // index_cf_sst_meta_sdk_data.sst; index_cf_sst_meta_sql_data.sst;
  // document_cf_sst_meta_sdk_data.sst; document_cf_sst_meta_sql_data.sst;
  else if (dingodb::Constant::kStoreCfSstMetaSqlMetaSstName == file_name_1_ ||
           dingodb::Constant::kStoreCfSstMetaSdkDataSstName == file_name_1_ ||
           dingodb::Constant::kIndexCfSstMetaSdkDataSstName == file_name_1_ ||
           dingodb::Constant::kDocumentCfSstMetaSdkDataSstName == file_name_1_ ||
           dingodb::Constant::kStoreCfSstMetaSqlDataSstName == file_name_1_ ||
           dingodb::Constant::kIndexCfSstMetaSqlDataSstName == file_name_1_ ||
           dingodb::Constant::kDocumentCfSstMetaSqlDataSstName == file_name_1_) {
    std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&, std::string&,
                                std::string&, std::string&)>
        handler = nullptr;

    if (file_name_1_ != file_name_2_) {
      std::string s = fmt::format("file name is not same. {} {}", file_name_1_, file_name_2_);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    if (dingodb::Constant::kStoreCfSstMetaSqlMetaSstName == file_name_1_) {
      desc = "This file is used to store the store cf sst meta sql meta information";
      handler = ToolDiff::CompareRegionCfFunction;
    }

    if (dingodb::Constant::kStoreCfSstMetaSdkDataSstName == file_name_1_) {
      desc = "This file is used to store the store cf sst meta sdk data information";
      handler = ToolDiff::CompareRegionCfFunction;
    }

    if (dingodb::Constant::kIndexCfSstMetaSdkDataSstName == file_name_1_) {
      desc = "This file is used to store the index cf sst meta sdk data information";
      handler = ToolDiff::CompareRegionCfFunction;
    }

    if (dingodb::Constant::kDocumentCfSstMetaSdkDataSstName == file_name_1_) {
      desc = "This file is used to store the document cf sst meta sdk data information";
      handler = ToolDiff::CompareRegionCfFunction;
    }

    if (dingodb::Constant::kStoreCfSstMetaSqlDataSstName == file_name_1_) {
      desc = "This file is used to store the store cf sst meta sql data information";
      handler = ToolDiff::CompareRegionCfFunction;
    }

    if (dingodb::Constant::kIndexCfSstMetaSqlDataSstName == file_name_1_) {
      desc = "This file is used to store the index cf sst meta sql data information";
      handler = ToolDiff::CompareRegionCfFunction;
    }

    if (dingodb::Constant::kDocumentCfSstMetaSqlDataSstName == file_name_1_) {
      desc = "This file is used to store the document cf sst meta sql data information";
      handler = ToolDiff::CompareRegionCfFunction;
    }

    status =
        ToolDiff::CompareRegionCfSst(path_internal_1_, file_name_1_, path_internal_2_, file_name_2_, desc, handler);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }
  // diff region defintion sst.
  // store_region_sql_meta.sst;
  // store_region_sdk_data.sst; store_region_sql_data.sst;
  // index_region_sdk_data.sst; index_region_sql_data.sst;
  // document_region_sql_data.sst; document_region_sdk_data.sst;
  else if (dingodb::Constant::kStoreRegionSqlMetaSstName == file_name_1_ ||
           dingodb::Constant::kStoreRegionSdkDataSstName == file_name_1_ ||
           dingodb::Constant::kIndexRegionSdkDataSstName == file_name_1_ ||
           dingodb::Constant::kDocumentRegionSdkDataSstName == file_name_1_ ||
           dingodb::Constant::kStoreRegionSqlDataSstName == file_name_1_ ||
           dingodb::Constant::kIndexRegionSqlDataSstName == file_name_1_ ||
           dingodb::Constant::kDocumentRegionSqlDataSstName == file_name_1_) {
    std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&, std::string&,
                                std::string&, std::string&)>
        handler = nullptr;

    if (file_name_1_ != file_name_2_) {
      std::string s = fmt::format("file name is not same. {} {}", file_name_1_, file_name_2_);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    if (dingodb::Constant::kStoreRegionSqlMetaSstName == file_name_1_) {
      desc = "This file is used to store the store region sql meta information";
      handler = ToolDiff::CompareRegionDefinitionFunction;
    }

    if (dingodb::Constant::kStoreRegionSdkDataSstName == file_name_1_) {
      desc = "This file is used to store the store region sdk data information";
      handler = ToolDiff::CompareRegionDefinitionFunction;
    }

    if (dingodb::Constant::kIndexRegionSdkDataSstName == file_name_1_) {
      desc = "This file is used to store the index region sdk data information";
      handler = ToolDiff::CompareRegionDefinitionFunction;
    }

    if (dingodb::Constant::kDocumentRegionSdkDataSstName == file_name_1_) {
      desc = "This file is used to store the document region sdk data information";
      handler = ToolDiff::CompareRegionDefinitionFunction;
    }

    if (dingodb::Constant::kStoreRegionSqlDataSstName == file_name_1_) {
      desc = "This file is used to store the store region sql data information";
      handler = ToolDiff::CompareRegionDefinitionFunction;
    }

    if (dingodb::Constant::kIndexRegionSqlDataSstName == file_name_1_) {
      desc = "This file is used to store the index region sql data information";
      handler = ToolDiff::CompareRegionDefinitionFunction;
    }

    if (dingodb::Constant::kDocumentRegionSqlDataSstName == file_name_1_) {
      desc = "This file is used to store the document region sql data information";
      handler = ToolDiff::CompareRegionDefinitionFunction;
    }

    status = ToolDiff::CompareRegionDefinitionSst(path_internal_1_, file_name_1_, path_internal_2_, file_name_2_, desc,
                                                  handler);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }
  // diff region data sst.
  // like 80049_1-1_d7afb3ab102cfe5e7a8a66d3ec4800efb33ab2fc_1742800412_write.sst;
  else {
    std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&, std::string&,
                                std::string&, std::string&)>
        handler = nullptr;

    std::string node1;
    std::string node2;

    status = ToolDiff::ParseNode(tool_diff_params_.br_diff_file1, path_internal_1_, "br_diff_file1", node1);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = ToolDiff::ParseNode(tool_diff_params_.br_diff_file2, path_internal_2_, "br_diff_file2", node2);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    if (node1 != node2) {
      std::string s = fmt::format("file node is not same. {} {}", node1, node2);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    if (dingodb::Constant::kStoreRegionName == node1) {
      desc = "This file is used to store the store region data information";
      handler = ToolDiff::CompareRegionDataFunction;
    }

    if (dingodb::Constant::kIndexRegionName == node1) {
      desc = "This file is used to store the index region data information";
      handler = ToolDiff::CompareRegionDataFunction;
    }

    if (dingodb::Constant::kDocumentRegionName == node1) {
      desc = "This file is used to store the document region data information";
      handler = ToolDiff::CompareRegionDataFunction;
    }

    status =
        ToolDiff::CompareRegionDataSst(path_internal_1_, file_name_1_, path_internal_2_, file_name_2_, desc, handler);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::Finish() {
  butil::Status status;

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareDirect(const std::string& path1, const std::string& file_name1, const std::string& path2,
                                      const std::string& file_name2, const std::string& desc,
                                      std::function<butil::Status(const std::string&, const std::string&, bool&,
                                                                  std::string&, std::string&, std::string&)>
                                          handler) {
  butil::Status status;

  bool is_same;
  std::string compare_content;
  std::string content1;
  std::string content2;

  status = handler(path1, path2, is_same, compare_content, content1, content2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path1          ", path1);
  ToolUtils::PrintDumpItem("path2          ", path2);

  ToolUtils::PrintDumpItem("name1          ", file_name1);
  ToolUtils::PrintDumpItem("name2          ", file_name2);

  ToolUtils::PrintDumpItem("desc           ", desc);

  ToolUtils::PrintDumpItem("is_same        ", is_same ? "true" : "false");

  ToolUtils::PrintDumpItem("compare_content", compare_content);

  ToolUtils::PrintDumpItem("content1       ", content1);

  ToolUtils::PrintDumpItem("content2       ", content2);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareSst(const std::string& path1, const std::string& file_name1, const std::string& path2,
                                   const std::string& file_name2, const std::string& desc,
                                   std::function<butil::Status(const std::string&, const std::string&, bool&,
                                                               std::string&, std::string&, std::string&)>
                                       handler) {
  butil::Status status;

  bool is_same;
  std::string compare_content;
  std::string content1;
  std::string content2;

  status = handler(path1, path2, is_same, compare_content, content1, content2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path1          ", path1);
  ToolUtils::PrintDumpItem("path2          ", path2);

  ToolUtils::PrintDumpItem("name1          ", file_name1);
  ToolUtils::PrintDumpItem("name2          ", file_name2);

  ToolUtils::PrintDumpItem("desc           ", desc);

  ToolUtils::PrintDumpItem("is_same        ", is_same ? "true" : "false");

  ToolUtils::PrintDumpItem("compare_content", compare_content);

  ToolUtils::PrintDumpItem("content1       ", content1);

  ToolUtils::PrintDumpItem("content2       ", content2);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareDirectFunction(const std::string& path1, const std::string& path2, bool& is_same,
                                              std::string& compare_content, std::string& content1,
                                              std::string& content2) {
  butil::Status status;

  std::vector<std::string> lines1;
  std::vector<std::string> lines2;

  std::ifstream reader(path1, std::ifstream::in);

  std::string line;

  while (std::getline(reader, line)) {
    lines1.push_back(line);
    content1 += "\n" + line;
  }

  if (reader.is_open()) {
    reader.close();
  }

  reader.open(path2, std::ifstream::in);
  if (!reader.is_open()) {
    std::string s = fmt::format("open file failed. {}", path2);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  while (std::getline(reader, line)) {
    lines2.push_back(line);
    content2 += "\n" + line;
  }

  if (reader.is_open()) {
    reader.close();
  }

  is_same = true;

  // compare
  if (lines1.size() != lines2.size()) {
    is_same = false;
  }

  size_t i = 0;
  size_t min = std::min(lines1.size(), lines2.size());
  size_t max = std::max(lines1.size(), lines2.size());
  for (; i < min; i++) {
    if (lines1[i] != lines2[i]) {
      if (is_same) is_same = false;
      // compare_content += fmt::format("\n\033[0;31mline [{}] ≠ L:{} | R:{}\033[0m", i, lines1[i], lines2[i]);
      compare_content += fmt::format("\nline [{}] ≠ L:{} | R:{}", i, lines1[i], lines2[i]);
    } else {
      compare_content += fmt::format("\nline [{}] = L:{} | R:{}", i, lines1[i], lines2[i]);
    }
  }

  for (; i < max; i++) {
    if (is_same) is_same = false;
    if (i < lines1.size()) {
      compare_content += fmt::format("\nline [{}] ≠ L:{} | R: ", i, lines1[i]);
    } else {
      compare_content += fmt::format("\nline [{}] ≠ L: | R:{}", i, lines2[i]);
    }
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareBackupMetaFunction(const std::string& path1, const std::string& path2, bool& is_same,
                                                  std::string& compare_content, std::string& content1,
                                                  std::string& content2) {
  butil::Status status;

  dingodb::pb::common::BackupParam backup_param1;
  bool backup_param_exist1 = false;

  dingodb::pb::common::BackupMeta backup_meta1;
  bool backup_meta_exist1 = false;

  dingodb::pb::meta::IdEpochTypeAndValue id_epoch_type_and_value1;
  bool id_epoch_type_and_value_exist1 = false;

  dingodb::pb::meta::TableIncrementGroup table_increment_group1;
  bool table_increment_group_exist1 = false;

  status = ToolDiff::ReadSstForBackupMeta(path1, backup_param1, backup_param_exist1, backup_meta1, backup_meta_exist1,
                                          id_epoch_type_and_value1, id_epoch_type_and_value_exist1,
                                          table_increment_group1, table_increment_group_exist1);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  dingodb::pb::common::BackupParam backup_param2;
  bool backup_param_exist2 = false;

  dingodb::pb::common::BackupMeta backup_meta2;
  bool backup_meta_exist2 = false;

  dingodb::pb::meta::IdEpochTypeAndValue id_epoch_type_and_value2;
  bool id_epoch_type_and_value_exist2 = false;

  dingodb::pb::meta::TableIncrementGroup table_increment_group2;
  bool table_increment_group_exist2 = false;
  status = ToolDiff::ReadSstForBackupMeta(path2, backup_param2, backup_param_exist2, backup_meta2, backup_meta_exist2,
                                          id_epoch_type_and_value2, id_epoch_type_and_value_exist2,
                                          table_increment_group2, table_increment_group_exist2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  int32_t i = 0;
  if (backup_param_exist1) {
    content1 +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kBackupBackupParamKey, backup_param1.DebugString());
    i++;
  }

  if (backup_meta_exist1) {
    content1 +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kBackupMetaSchemaName, backup_meta1.DebugString());
    i++;
  }

  if (id_epoch_type_and_value_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIdEpochTypeAndValueKey,
                            id_epoch_type_and_value1.DebugString());
    i++;
  }
  if (table_increment_group_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kTableIncrementKey,
                            table_increment_group1.DebugString());
    i++;
  }

  i = 0;
  if (backup_param_exist2) {
    content2 +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kBackupBackupParamKey, backup_param2.DebugString());
    i++;
  }

  if (backup_meta_exist2) {
    content2 +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kBackupMetaSchemaName, backup_meta2.DebugString());
    i++;
  }

  if (id_epoch_type_and_value_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIdEpochTypeAndValueKey,
                            id_epoch_type_and_value2.DebugString());
    i++;
  }
  if (table_increment_group_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kTableIncrementKey,
                            table_increment_group2.DebugString());
    i++;
  }

  // compare
  is_same = true;

  if (backup_param_exist1 && !backup_param_exist2) {
    is_same = false;
  }

  if (!backup_param_exist1 && backup_param_exist2) {
    is_same = false;
  }

  // compare backup_param
  if (backup_param_exist1 && backup_param_exist2) {
    if (backup_param1.coor_addr() != backup_param2.coor_addr()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.coor_addr L:{} | R:{}",
                                     backup_param1.coor_addr(), backup_param2.coor_addr());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.coor_addr L:{} | R:{}",
                                     backup_param1.coor_addr(), backup_param2.coor_addr());
    }

    if (backup_param1.store_addr() != backup_param2.store_addr()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.store_addr L:{} | R:{}",
                                     backup_param1.store_addr(), backup_param2.store_addr());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.store_addr L:{} | R:{}",
                                     backup_param1.store_addr(), backup_param2.store_addr());
    }

    if (backup_param1.index_addr() != backup_param2.index_addr()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.index_addr L:{} | R:{}",
                                     backup_param1.index_addr(), backup_param2.index_addr());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.index_addr L:{} | R:{}",
                                     backup_param1.index_addr(), backup_param2.index_addr());
    }

    if (backup_param1.document() != backup_param2.document()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.document L:{} | R:{}",
                                     backup_param1.document(), backup_param2.document());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.document L:{} | R:{}",
                                     backup_param1.document(), backup_param2.document());
    }

    if (backup_param1.br_type() != backup_param2.br_type()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.br_type L:{} | R:{}",
                                     backup_param1.br_type(), backup_param2.br_type());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.br_type L:{} | R:{}",
                                     backup_param1.br_type(), backup_param2.br_type());
    }

    if (backup_param1.br_backup_type() != backup_param2.br_backup_type()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.br_backup_type L:{} | R:{}",
                                     backup_param1.br_backup_type(), backup_param2.br_backup_type());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.br_backup_type L:{} | R:{}",
                                     backup_param1.br_backup_type(), backup_param2.br_backup_type());
    }

    if (backup_param1.backupts() != backup_param2.backupts()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.backupts L:{} | R:{}",
                                     backup_param1.backupts(), backup_param2.backupts());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.backupts L:{} | R:{}",
                                     backup_param1.backupts(), backup_param2.backupts());
    }

    if (backup_param1.backuptso_internal() != backup_param2.backuptso_internal()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.backuptso_internal L:{} | R:{}",
                                     backup_param1.backuptso_internal(), backup_param2.backuptso_internal());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.backuptso_internal L:{} | R:{}",
                                     backup_param1.backuptso_internal(), backup_param2.backuptso_internal());
    }

    if (backup_param1.storage() != backup_param2.storage()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.storage L:{} | R:{}",
                                     backup_param1.storage(), backup_param2.storage());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.storage L:{} | R:{}",
                                     backup_param1.storage(), backup_param2.storage());
    }
    if (backup_param1.storage_internal() != backup_param2.storage_internal()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupParam.storage_internal L:{} | R:{}",
                                     backup_param1.storage_internal(), backup_param2.storage_internal());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupParam.storage_internal L:{} | R:{}",
                                     backup_param1.storage_internal(), backup_param2.storage_internal());
    }
    compare_content += "\n";
  }

  if (backup_meta_exist1 && !backup_meta_exist2) {
    is_same = false;
  }

  if (!backup_meta_exist1 && backup_meta_exist2) {
    is_same = false;
  }

  // compare backup_meta
  if (backup_meta_exist1 && backup_meta_exist2) {
    if (backup_meta1.remark() != backup_meta2.remark()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupMeta.remark L:{} | R:{}", backup_meta1.remark(),
                                     backup_meta2.remark());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupMeta.remark L:{} | R:{}", backup_meta1.remark(),
                                     backup_meta2.remark());
    }

    if (backup_meta1.exec_node() != backup_meta2.exec_node()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupMeta.exec_node L:{} | R:{}",
                                     backup_meta1.exec_node(), backup_meta2.exec_node());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupMeta.backup_type L:{} | R:{}",
                                     backup_meta1.exec_node(), backup_meta2.exec_node());
    }

    if (backup_meta1.dir_name() != backup_meta2.dir_name()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupMeta.dir_name L:{} | R:{}",
                                     backup_meta1.dir_name(), backup_meta2.dir_name());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupMeta.dir_name L:{} | R:{}",
                                     backup_meta1.dir_name(), backup_meta2.dir_name());
    }

    if (backup_meta1.file_size() != backup_meta2.file_size()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupMeta.file_size L:{} | R:{}",
                                     backup_meta1.file_size(), backup_meta2.file_size());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupMeta.file_size L:{} | R:{}",
                                     backup_meta1.file_size(), backup_meta2.file_size());
    }

    if (backup_meta1.encryption() != backup_meta2.encryption()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupMeta.encryption L:{} | R:{}",
                                     backup_meta1.encryption(), backup_meta2.encryption());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupMeta.encryption L:{} | R:{}",
                                     backup_meta1.encryption(), backup_meta2.encryption());
    }

    if (backup_meta1.file_name() != backup_meta2.file_name()) {
      is_same = false;
      compare_content += fmt::format("\n ≠ dingodb::pb::common::BackupMeta.file_name L:{} | R:{}",
                                     backup_meta1.file_name(), backup_meta2.file_name());
    } else {
      compare_content += fmt::format("\n = dingodb::pb::common::BackupMeta.file_name L:{} | R:{}",
                                     backup_meta1.file_name(), backup_meta2.file_name());
    }
    compare_content += "\n";
  }

  if (id_epoch_type_and_value_exist1 && !id_epoch_type_and_value_exist2) {
    is_same = false;
  }

  if (!id_epoch_type_and_value_exist1 && id_epoch_type_and_value_exist2) {
    is_same = false;
  }

  // compare id_epoch_type_and_value
  if (id_epoch_type_and_value_exist1 && id_epoch_type_and_value_exist2) {
    std::map<int64_t, int64_t> internal_id_epoch_type_and_value1;
    std::map<int64_t, int64_t> internal_id_epoch_type_and_value2;

    for (int i = 0; i < id_epoch_type_and_value1.items_size(); i++) {
      internal_id_epoch_type_and_value1.insert(std::pair<int64_t, int64_t>(id_epoch_type_and_value1.items(i).type(),
                                                                           id_epoch_type_and_value1.items(i).value()));
    }

    for (int i = 0; i < id_epoch_type_and_value2.items_size(); i++) {
      internal_id_epoch_type_and_value2.insert(std::pair<int64_t, int64_t>(id_epoch_type_and_value2.items(i).type(),
                                                                           id_epoch_type_and_value2.items(i).value()));
    }

    for (const auto& [type, value] : internal_id_epoch_type_and_value1) {
      auto iter = internal_id_epoch_type_and_value2.find(type);
      if (iter == internal_id_epoch_type_and_value2.end()) {
        is_same = false;
        compare_content += fmt::format("\n ≠ dingodb::pb::meta::IdEpochTypeAndValue.{} L:{} | R: ",
                                       dingodb::pb::coordinator::IdEpochType_Name(type), value);
      } else {
        if (value != iter->second) {
          is_same = false;
          compare_content += fmt::format("\n ≠ dingodb::pb::meta::IdEpochTypeAndValue.{} L:{} | R:{}",
                                         dingodb::pb::coordinator::IdEpochType_Name(type), value, iter->second);
        } else {
          compare_content += fmt::format("\n = dingodb::pb::meta::IdEpochTypeAndValue.{} L:{} | R:{}",
                                         dingodb::pb::coordinator::IdEpochType_Name(type), value, iter->second);
        }
      }
    }

    for (const auto& [type, value] : internal_id_epoch_type_and_value2) {
      auto iter = internal_id_epoch_type_and_value1.find(type);
      if (iter == internal_id_epoch_type_and_value1.end()) {
        is_same = false;
        compare_content += fmt::format("\n ≠ dingodb::pb::meta::IdEpochTypeAndValue.{} L: | R:{}",
                                       dingodb::pb::coordinator::IdEpochType_Name(type), value);
      }
    }

    compare_content += "\n";
  }

  if (table_increment_group_exist1 && !table_increment_group_exist2) {
    is_same = false;
  }

  if (!table_increment_group_exist1 && table_increment_group_exist2) {
    is_same = false;
  }

  // compare table_increment_group
  if (table_increment_group_exist1 && table_increment_group_exist2) {
    std::map<int64_t, int64_t> internal_table_increment_group1;
    std::map<int64_t, int64_t> internal_table_increment_group2;

    for (int i = 0; i < table_increment_group1.table_increments_size(); i++) {
      internal_table_increment_group1.insert(
          std::pair<int64_t, int64_t>(table_increment_group1.table_increments(i).table_id(),
                                      table_increment_group1.table_increments(i).start_id()));
    }

    for (int i = 0; i < table_increment_group2.table_increments_size(); i++) {
      internal_table_increment_group2.insert(
          std::pair<int64_t, int64_t>(table_increment_group2.table_increments(i).table_id(),
                                      table_increment_group2.table_increments(i).start_id()));
    }

    for (const auto& [table_id, start_id] : internal_table_increment_group1) {
      auto iter = internal_table_increment_group2.find(table_id);
      if (iter == internal_table_increment_group2.end()) {
        is_same = false;
        compare_content +=
            fmt::format("\n ≠ dingodb::pb::meta::TableIncrementGroup.table_id-{} L:{} | R: ", table_id, start_id);
      } else {
        if (start_id != iter->second) {
          is_same = false;
          compare_content += fmt::format("\n ≠ dingodb::pb::meta::TableIncrementGroup.table_id-{} L:{} | R:{}",
                                         table_id, start_id, iter->second);
        } else {
          compare_content += fmt::format("\n = dingodb::pb::meta::TableIncrementGroup.table_id-{} L:{} | R:{}",
                                         table_id, start_id, iter->second);
        }
      }
    }

    for (const auto& [table_id, start_id] : internal_table_increment_group2) {
      auto iter = internal_table_increment_group1.find(table_id);
      if (iter == internal_table_increment_group1.end()) {
        is_same = false;
        compare_content +=
            fmt::format("\n ≠ dingodb::pb::meta::TableIncrementGroup.table_id-{} L: | R:{}", table_id, start_id);
      }
    }
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::ReadSstForBackupMeta(const std::string& path, dingodb::pb::common::BackupParam& backup_param,
                                             bool& backup_param_exist, dingodb::pb::common::BackupMeta& backup_meta,
                                             bool& backup_meta_exist,
                                             dingodb::pb::meta::IdEpochTypeAndValue& id_epoch_type_and_value,
                                             bool& id_epoch_type_and_value_exist,
                                             dingodb::pb::meta::TableIncrementGroup& table_increment_group,
                                             bool& table_increment_group_exist) {
  butil::Status status;

  std::map<std::string, std::string> kvs;

  std::shared_ptr<SstFileReader> sst_reader = std::make_shared<SstFileReader>();

  status = sst_reader->ReadFile(path, kvs);
  if (!status.ok()) {
    std::string s = fmt::format("read sst file failed. {}", status.error_str());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  backup_param_exist = false;
  auto iter = kvs.find(dingodb::Constant::kBackupBackupParamKey);
  if (iter != kvs.end()) {
    auto ret = backup_param.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed");
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    backup_param_exist = true;
  }

  backup_meta_exist = false;
  iter = kvs.find(dingodb::Constant::kBackupMetaSchemaName);
  if (iter != kvs.end()) {
    auto ret = backup_meta.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupMeta failed");
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    backup_meta_exist = true;
  }

  // find IdEpochTypeAndValueKey
  id_epoch_type_and_value_exist = false;
  iter = kvs.find(dingodb::Constant::kIdEpochTypeAndValueKey);
  if (iter != kvs.end()) {
    auto ret = id_epoch_type_and_value.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::meta::IdEpochTypeAndValue failed");
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    id_epoch_type_and_value_exist = true;
  }

  // find TableIncrementKey
  table_increment_group_exist = false;
  iter = kvs.find(dingodb::Constant::kTableIncrementKey);
  if (iter != kvs.end()) {
    auto ret = table_increment_group.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::meta::TableIncrementGroup failed");
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    table_increment_group_exist = true;
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareBackupMetaDataFileFunction(const std::string& path1, const std::string& path2,
                                                          bool& is_same, std::string& compare_content,
                                                          std::string& content1, std::string& content2) {
  butil::Status status;

  dingodb::pb::common::BackupMeta store_region_sql_data_sst1;
  bool store_region_sql_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta store_cf_sst_meta_sql_data_sst1;
  bool store_cf_sst_meta_sql_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta index_region_sql_data_sst1;
  bool index_region_sql_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta index_cf_sst_meta_sql_data_sst1;
  bool index_cf_sst_meta_sql_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta document_region_sql_data_sst1;
  bool document_region_sql_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta document_cf_sst_meta_sql_data_sst1;
  bool document_cf_sst_meta_sql_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta store_region_sdk_data_sst1;
  bool store_region_sdk_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta store_cf_sst_meta_sdk_data_sst1;
  bool store_cf_sst_meta_sdk_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta index_region_sdk_data_sst1;
  bool index_region_sdk_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta index_cf_sst_meta_sdk_data_sst1;
  bool index_cf_sst_meta_sdk_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta document_region_sdk_data_sst1;
  auto document_region_sdk_data_sst_exist1 = false;

  dingodb::pb::common::BackupMeta document_cf_sst_meta_sdk_data_sst1;
  bool document_cf_sst_meta_sdk_data_sst_exist1 = false;

  status = ToolDiff::ReadSstForBackupMetaDataFile(
      path1, store_region_sql_data_sst1, store_region_sql_data_sst_exist1, store_cf_sst_meta_sql_data_sst1,
      store_cf_sst_meta_sql_data_sst_exist1, index_region_sql_data_sst1, index_region_sql_data_sst_exist1,
      index_cf_sst_meta_sql_data_sst1, index_cf_sst_meta_sql_data_sst_exist1, document_region_sql_data_sst1,
      document_region_sql_data_sst_exist1, document_cf_sst_meta_sql_data_sst1, document_cf_sst_meta_sql_data_sst_exist1,
      store_region_sdk_data_sst1, store_region_sdk_data_sst_exist1, store_cf_sst_meta_sdk_data_sst1,
      store_cf_sst_meta_sdk_data_sst_exist1, index_region_sdk_data_sst1, index_region_sdk_data_sst_exist1,
      index_cf_sst_meta_sdk_data_sst1, index_cf_sst_meta_sdk_data_sst_exist1, document_region_sdk_data_sst1,
      document_region_sdk_data_sst_exist1, document_cf_sst_meta_sdk_data_sst1,
      document_cf_sst_meta_sdk_data_sst_exist1);

  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  dingodb::pb::common::BackupMeta store_region_sql_data_sst2;
  bool store_region_sql_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta store_cf_sst_meta_sql_data_sst2;
  bool store_cf_sst_meta_sql_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta index_region_sql_data_sst2;
  bool index_region_sql_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta index_cf_sst_meta_sql_data_sst2;
  bool index_cf_sst_meta_sql_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta document_region_sql_data_sst2;
  bool document_region_sql_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta document_cf_sst_meta_sql_data_sst2;
  bool document_cf_sst_meta_sql_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta store_region_sdk_data_sst2;
  bool store_region_sdk_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta store_cf_sst_meta_sdk_data_sst2;
  bool store_cf_sst_meta_sdk_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta index_region_sdk_data_sst2;
  bool index_region_sdk_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta index_cf_sst_meta_sdk_data_sst2;
  bool index_cf_sst_meta_sdk_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta document_region_sdk_data_sst2;
  bool document_region_sdk_data_sst_exist2 = false;

  dingodb::pb::common::BackupMeta document_cf_sst_meta_sdk_data_sst2;
  bool document_cf_sst_meta_sdk_data_sst_exist2 = false;

  status = ToolDiff::ReadSstForBackupMetaDataFile(
      path2, store_region_sql_data_sst2, store_region_sql_data_sst_exist2, store_cf_sst_meta_sql_data_sst2,
      store_cf_sst_meta_sql_data_sst_exist2, index_region_sql_data_sst2, index_region_sql_data_sst_exist2,
      index_cf_sst_meta_sql_data_sst2, index_cf_sst_meta_sql_data_sst_exist2, document_region_sql_data_sst2,
      document_region_sql_data_sst_exist2, document_cf_sst_meta_sql_data_sst2, document_cf_sst_meta_sql_data_sst_exist2,
      store_region_sdk_data_sst2, store_region_sdk_data_sst_exist2, store_cf_sst_meta_sdk_data_sst2,
      store_cf_sst_meta_sdk_data_sst_exist2, index_region_sdk_data_sst2, index_region_sdk_data_sst_exist2,
      index_cf_sst_meta_sdk_data_sst2, index_cf_sst_meta_sdk_data_sst_exist2, document_region_sdk_data_sst2,
      document_region_sdk_data_sst_exist2, document_cf_sst_meta_sdk_data_sst2,
      document_cf_sst_meta_sdk_data_sst_exist2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }
  int32_t i = 0;

  if (store_region_sql_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreRegionSqlDataSstName,
                            store_region_sql_data_sst1.DebugString());
    i++;
  }

  if (store_cf_sst_meta_sql_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreCfSstMetaSqlDataSstName,
                            store_cf_sst_meta_sql_data_sst1.DebugString());
    i++;
  }

  if (index_region_sql_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexRegionSqlDataSstName,
                            index_region_sql_data_sst1.DebugString());
    i++;
  }

  if (index_cf_sst_meta_sql_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexCfSstMetaSqlDataSstName,
                            index_cf_sst_meta_sql_data_sst1.DebugString());
    i++;
  }

  if (document_region_sql_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentRegionSqlDataSstName,
                            document_region_sql_data_sst1.DebugString());
    i++;
  }

  if (document_cf_sst_meta_sql_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentCfSstMetaSqlDataSstName,
                            document_cf_sst_meta_sql_data_sst1.DebugString());
    i++;
  }

  if (store_region_sdk_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreRegionSdkDataSstName,
                            store_region_sdk_data_sst1.DebugString());
    i++;
  }

  if (store_cf_sst_meta_sdk_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreCfSstMetaSdkDataSstName,
                            store_cf_sst_meta_sdk_data_sst1.DebugString());
    i++;
  }

  if (index_region_sdk_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexRegionSdkDataSstName,
                            index_region_sdk_data_sst1.DebugString());
    i++;
  }

  if (index_cf_sst_meta_sdk_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexCfSstMetaSdkDataSstName,
                            index_cf_sst_meta_sdk_data_sst1.DebugString());
    i++;
  }

  if (document_region_sdk_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentRegionSdkDataSstName,
                            document_region_sdk_data_sst1.DebugString());
    i++;
  }

  if (document_cf_sst_meta_sdk_data_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentCfSstMetaSdkDataSstName,
                            document_cf_sst_meta_sdk_data_sst1.DebugString());
    i++;
  }

  i = 0;

  if (store_region_sql_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreRegionSqlDataSstName,
                            store_region_sql_data_sst2.DebugString());
    i++;
  }

  if (store_cf_sst_meta_sql_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreCfSstMetaSqlDataSstName,
                            store_cf_sst_meta_sql_data_sst2.DebugString());
    i++;
  }

  if (index_region_sql_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexRegionSqlDataSstName,
                            index_region_sql_data_sst2.DebugString());
    i++;
  }

  if (index_cf_sst_meta_sql_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexCfSstMetaSqlDataSstName,
                            index_cf_sst_meta_sql_data_sst2.DebugString());
    i++;
  }

  if (document_region_sql_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentRegionSqlDataSstName,
                            document_region_sql_data_sst2.DebugString());
    i++;
  }

  if (document_cf_sst_meta_sql_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentCfSstMetaSqlDataSstName,
                            document_cf_sst_meta_sql_data_sst2.DebugString());
    i++;
  }

  if (store_region_sdk_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreRegionSdkDataSstName,
                            store_region_sdk_data_sst2.DebugString());
    i++;
  }

  if (store_cf_sst_meta_sdk_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreCfSstMetaSdkDataSstName,
                            store_cf_sst_meta_sdk_data_sst2.DebugString());
    i++;
  }

  if (index_region_sdk_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexRegionSdkDataSstName,
                            index_region_sdk_data_sst2.DebugString());
    i++;
  }

  if (index_cf_sst_meta_sdk_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexCfSstMetaSdkDataSstName,
                            index_cf_sst_meta_sdk_data_sst2.DebugString());
    i++;
  }

  if (document_region_sdk_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentRegionSdkDataSstName,
                            document_region_sdk_data_sst2.DebugString());
    i++;
  }

  if (document_cf_sst_meta_sdk_data_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentCfSstMetaSdkDataSstName,
                            document_cf_sst_meta_sdk_data_sst2.DebugString());
    i++;
  }

  is_same = true;

  // compare backup_param
  if (store_region_sql_data_sst_exist1 && !store_region_sql_data_sst_exist2) {
    is_same = false;
  }

  if (!store_region_sql_data_sst_exist1 && store_region_sql_data_sst_exist2) {
    is_same = false;
  }

  // compare backup_meta store_region_sql_data.sst
  if (store_region_sql_data_sst_exist1 && store_region_sql_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("store_region_sql_data.sst", store_region_sql_data_sst1, store_region_sql_data_sst2,
                                  is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta store_cf_sst_meta_sql_data.sst
  if (store_cf_sst_meta_sql_data_sst_exist1 && store_cf_sst_meta_sql_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("store_cf_sst_meta_sql_data.sst", store_cf_sst_meta_sql_data_sst1,
                                  store_cf_sst_meta_sql_data_sst2, is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta index_region_sql_data.sst
  if (index_region_sql_data_sst_exist1 && index_region_sql_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("index_region_sql_data.sst", index_region_sql_data_sst1, index_region_sql_data_sst2,
                                  is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta index_cf_sst_meta_sql_data.sst
  if (index_cf_sst_meta_sql_data_sst_exist1 && index_cf_sst_meta_sql_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("index_cf_sst_meta_sql_data.sst", index_cf_sst_meta_sql_data_sst1,
                                  index_cf_sst_meta_sql_data_sst2, is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta document_region_sql_data.sst
  if (document_region_sql_data_sst_exist1 && document_region_sql_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("document_region_sql_data.sst", document_region_sql_data_sst1,
                                  document_region_sql_data_sst2, is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta document_cf_sst_meta_sql_data.sst
  if (document_cf_sst_meta_sql_data_sst_exist1 && document_cf_sst_meta_sql_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("document_cf_sst_meta_sql_data.sst", document_cf_sst_meta_sql_data_sst1,
                                  document_cf_sst_meta_sql_data_sst2, is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta store_region_sdk_data.sst
  if (store_region_sdk_data_sst_exist1 && store_region_sdk_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("store_region_sdk_data.sst", store_region_sdk_data_sst1, store_region_sdk_data_sst2,
                                  is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta store_cf_sst_meta_sdk_data.sst
  if (store_cf_sst_meta_sdk_data_sst_exist1 && store_cf_sst_meta_sdk_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("store_cf_sst_meta_sdk_data.sst", store_cf_sst_meta_sdk_data_sst1,
                                  store_cf_sst_meta_sdk_data_sst2, is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta index_region_sdk_data.sst
  if (index_region_sdk_data_sst_exist1 && index_region_sdk_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("index_region_sdk_data.sst", index_region_sdk_data_sst1, index_region_sdk_data_sst2,
                                  is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta index_cf_sst_meta_sdk_data.sst
  if (index_cf_sst_meta_sdk_data_sst_exist1 && index_cf_sst_meta_sdk_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("index_cf_sst_meta_sdk_data.sst", index_cf_sst_meta_sdk_data_sst1,
                                  index_cf_sst_meta_sdk_data_sst2, is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta document_region_sdk_data.sst
  if (document_region_sdk_data_sst_exist1 && document_region_sdk_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("document_region_sdk_data.sst", document_region_sdk_data_sst1,
                                  document_region_sdk_data_sst2, is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta document_cf_sst_meta_sdk_data.sst
  if (document_cf_sst_meta_sdk_data_sst_exist1 && document_cf_sst_meta_sdk_data_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("document_cf_sst_meta_sdk_data.sst", document_cf_sst_meta_sdk_data_sst1,
                                  document_cf_sst_meta_sdk_data_sst2, is_same, compare_content);
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::ReadSstForBackupMetaDataFile(
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
    bool& document_cf_sst_meta_sdk_data_sst_exist) {
  butil::Status status;
  std::map<std::string, std::string> kvs;

  std::shared_ptr<SstFileReader> sst_reader = std::make_shared<SstFileReader>();

  status = sst_reader->ReadFile(path, kvs);
  if (!status.ok()) {
    std::string s = fmt::format("read sst file failed. {}", status.error_str());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  // find store_region_sql_data.sst
  store_region_sql_data_sst_exist = false;
  auto iter = kvs.find(dingodb::Constant::kStoreRegionSqlDataSstName);
  if (iter != kvs.end()) {
    auto ret = store_region_sql_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kStoreRegionSqlDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    store_region_sql_data_sst_exist = true;
  }

  // find store_cf_sst_meta_sql_data.sst
  store_cf_sst_meta_sql_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kStoreCfSstMetaSqlDataSstName);
  if (iter != kvs.end()) {
    auto ret = store_cf_sst_meta_sql_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kStoreCfSstMetaSqlDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    store_cf_sst_meta_sql_data_sst_exist = true;
  }

  // find index_region_sql_data.sst
  index_region_sql_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kIndexRegionSqlDataSstName);
  if (iter != kvs.end()) {
    auto ret = index_region_sql_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kIndexRegionSqlDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    index_region_sql_data_sst_exist = true;
  }

  // find index_cf_sst_meta_sql_data.sst
  index_cf_sst_meta_sql_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kIndexCfSstMetaSqlDataSstName);
  if (iter != kvs.end()) {
    auto ret = index_cf_sst_meta_sql_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kIndexCfSstMetaSqlDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    index_cf_sst_meta_sql_data_sst_exist = true;
  }

  // find document_region_sql_data.sst
  document_region_sql_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kDocumentRegionSqlDataSstName);
  if (iter != kvs.end()) {
    auto ret = document_region_sql_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kDocumentRegionSqlDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    document_region_sql_data_sst_exist = true;
  }

  // find document_cf_sst_meta_sql_data.sst
  document_cf_sst_meta_sql_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kDocumentCfSstMetaSqlDataSstName);
  if (iter != kvs.end()) {
    auto ret = document_cf_sst_meta_sql_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kDocumentCfSstMetaSqlDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    document_cf_sst_meta_sql_data_sst_exist = true;
  }

  // find store_region_sdk_data.sst
  store_region_sdk_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kStoreRegionSdkDataSstName);
  if (iter != kvs.end()) {
    auto ret = store_region_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kStoreRegionSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    store_region_sdk_data_sst_exist = true;
  }

  // find store_cf_sst_meta_sdk_data.sst
  store_cf_sst_meta_sdk_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
  if (iter != kvs.end()) {
    auto ret = store_cf_sst_meta_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    store_cf_sst_meta_sdk_data_sst_exist = true;
  }

  // find index_region_sdk_data.sst
  index_region_sdk_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kIndexRegionSdkDataSstName);
  if (iter != kvs.end()) {
    auto ret = index_region_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kIndexRegionSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    index_region_sdk_data_sst_exist = true;
  }

  // find index_cf_sst_meta_sdk_data.sst
  index_cf_sst_meta_sdk_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kIndexCfSstMetaSdkDataSstName);
  if (iter != kvs.end()) {
    auto ret = index_cf_sst_meta_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kIndexCfSstMetaSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    index_cf_sst_meta_sdk_data_sst_exist = true;
  }

  // find document_region_sdk_data.sst
  document_region_sdk_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kDocumentRegionSdkDataSstName);
  if (iter != kvs.end()) {
    auto ret = document_region_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kDocumentRegionSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    document_region_sdk_data_sst_exist = true;
  }

  // find document_cf_sst_meta_sdk_data.sst
  document_cf_sst_meta_sdk_data_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kDocumentCfSstMetaSdkDataSstName);
  if (iter != kvs.end()) {
    auto ret = document_cf_sst_meta_sdk_data_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kDocumentCfSstMetaSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    document_cf_sst_meta_sdk_data_sst_exist = true;
  }

  return butil::Status::OK();
}

void ToolDiff::CompareBackupMetaPb(const std::string& sst_name, const dingodb::pb::common::BackupMeta& backup_meta_1,
                                   const dingodb::pb::common::BackupMeta& backup_meta_2, bool& is_same,
                                   std::string& compare_content) {
  if (backup_meta_1.remark() != backup_meta_2.remark()) {
    is_same = false;
    compare_content += fmt::format("\n ≠ {} dingodb::pb::common::BackupMeta.remark L:{} | R:{}", sst_name,
                                   backup_meta_1.remark(), backup_meta_2.remark());
  } else {
    compare_content += fmt::format("\n = {} dingodb::pb::common::BackupMeta L:{} | R:{}", sst_name,
                                   backup_meta_1.remark(), backup_meta_2.remark());
  }

  if (backup_meta_1.exec_node() != backup_meta_2.exec_node()) {
    is_same = false;
    compare_content += fmt::format("\n ≠ {} dingodb::pb::common::BackupMeta.exec_node L:{} | R:{}", sst_name,
                                   backup_meta_1.exec_node(), backup_meta_2.exec_node());
  } else {
    compare_content += fmt::format("\n = {} dingodb::pb::common::BackupMeta.backup_type L:{} | R:{}", sst_name,
                                   backup_meta_1.exec_node(), backup_meta_2.exec_node());
  }

  if (backup_meta_1.dir_name() != backup_meta_2.dir_name()) {
    is_same = false;
    compare_content += fmt::format("\n ≠ {} dingodb::pb::common::BackupMeta.dir_name L:{} | R:{}", sst_name,
                                   backup_meta_1.dir_name(), backup_meta_2.dir_name());
  } else {
    compare_content += fmt::format("\n = {} dingodb::pb::common::BackupMeta.dir_name L:{} | R:{}", sst_name,
                                   backup_meta_1.dir_name(), backup_meta_2.dir_name());
  }

  if (backup_meta_1.file_size() != backup_meta_2.file_size()) {
    is_same = false;
    compare_content += fmt::format("\n ≠ {} dingodb::pb::common::BackupMeta.file_size L:{} | R:{}", sst_name,
                                   backup_meta_1.file_size(), backup_meta_2.file_size());
  } else {
    compare_content += fmt::format("\n = {} dingodb::pb::common::BackupMeta.file_size L:{} | R:{}", sst_name,
                                   backup_meta_1.file_size(), backup_meta_2.file_size());
  }

  if (backup_meta_1.encryption() != backup_meta_2.encryption()) {
    is_same = false;
    compare_content += fmt::format("\n ≠ {} dingodb::pb::common::BackupMeta.encryption L:{} | R:{}", sst_name,
                                   backup_meta_1.encryption(), backup_meta_2.encryption());
  } else {
    compare_content += fmt::format("\n = {} dingodb::pb::common::BackupMeta.encryption L:{} | R:{}", sst_name,
                                   backup_meta_1.encryption(), backup_meta_2.encryption());
  }

  if (backup_meta_1.file_name() != backup_meta_2.file_name()) {
    is_same = false;
    compare_content += fmt::format("\n ≠ {} dingodb::pb::common::BackupMeta.file_name L:{} | R:{}", sst_name,
                                   backup_meta_1.file_name(), backup_meta_2.file_name());
  } else {
    compare_content += fmt::format("\n = {} dingodb::pb::common::BackupMeta.file_name L:{} | R:{}", sst_name,
                                   backup_meta_1.file_name(), backup_meta_2.file_name());
  }
}

butil::Status ToolDiff::CompareBackupMetaSchemaFunction(const std::string& path1, const std::string& path2,
                                                        bool& is_same, std::string& compare_content,
                                                        std::string& content1, std::string& content2) {
  butil::Status status;

  dingodb::pb::common::BackupMeta store_cf_sst_meta_sql_meta_sst1;
  bool store_cf_sst_meta_sql_meta_sst_exist1 = false;

  dingodb::pb::common::BackupMeta store_region_sql_meta_sst1;
  bool store_region_sql_meta_sst_exist1 = false;

  dingodb::pb::common::BackupMeta coordinator_sdk_meta_sst1;
  bool coordinator_sdk_meta_sst_exist1 = false;

  status = ToolDiff::ReadSstForBackupMetaSchema(
      path1, store_cf_sst_meta_sql_meta_sst1, store_cf_sst_meta_sql_meta_sst_exist1, store_region_sql_meta_sst1,
      store_region_sql_meta_sst_exist1, coordinator_sdk_meta_sst1, coordinator_sdk_meta_sst_exist1);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  dingodb::pb::common::BackupMeta store_cf_sst_meta_sql_meta_sst2;
  bool store_cf_sst_meta_sql_meta_sst_exist2 = false;

  dingodb::pb::common::BackupMeta store_region_sql_meta_sst2;
  bool store_region_sql_meta_sst_exist2 = false;

  dingodb::pb::common::BackupMeta coordinator_sdk_meta_sst2;
  bool coordinator_sdk_meta_sst_exist2 = false;

  status = ToolDiff::ReadSstForBackupMetaSchema(
      path2, store_cf_sst_meta_sql_meta_sst2, store_cf_sst_meta_sql_meta_sst_exist2, store_region_sql_meta_sst2,
      store_region_sql_meta_sst_exist2, coordinator_sdk_meta_sst2, coordinator_sdk_meta_sst_exist2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  int32_t i = 0;
  if (store_cf_sst_meta_sql_meta_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreCfSstMetaSqlMetaSstName,
                            store_cf_sst_meta_sql_meta_sst1.DebugString());
    i++;
  }
  if (store_region_sql_meta_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreRegionSqlMetaSstName,
                            store_region_sql_meta_sst1.DebugString());
    i++;
  }
  if (coordinator_sdk_meta_sst_exist1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kCoordinatorSdkMetaSstName,
                            coordinator_sdk_meta_sst1.DebugString());
    i++;
  }

  i = 0;
  if (store_cf_sst_meta_sql_meta_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreCfSstMetaSqlMetaSstName,
                            store_cf_sst_meta_sql_meta_sst2.DebugString());
    i++;
  }

  if (store_region_sql_meta_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreRegionSqlMetaSstName,
                            store_region_sql_meta_sst2.DebugString());
    i++;
  }

  if (coordinator_sdk_meta_sst_exist2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kCoordinatorSdkMetaSstName,
                            coordinator_sdk_meta_sst2.DebugString());
    i++;
  }

  // compare backup_meta store_cf_sst_meta_sql_meta.sst
  if (store_cf_sst_meta_sql_meta_sst_exist1 && store_cf_sst_meta_sql_meta_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("store_cf_sst_meta_sql_meta.sst", store_cf_sst_meta_sql_meta_sst1,
                                  store_cf_sst_meta_sql_meta_sst2, is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta store_region_sql_meta.sst
  if (store_region_sql_meta_sst_exist1 && store_region_sql_meta_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("store_region_sql_meta.sst", store_region_sql_meta_sst1, store_region_sql_meta_sst2,
                                  is_same, compare_content);
    compare_content += "\n";
  }

  // compare backup_meta coordinator_sdk_meta.sst
  if (coordinator_sdk_meta_sst_exist1 && coordinator_sdk_meta_sst_exist2) {
    ToolDiff::CompareBackupMetaPb("coordinator_sdk_meta.sst", coordinator_sdk_meta_sst1, coordinator_sdk_meta_sst2,
                                  is_same, compare_content);
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::ReadSstForBackupMetaSchema(const std::string& path,
                                                   dingodb::pb::common::BackupMeta& store_cf_sst_meta_sql_meta_sst,
                                                   bool& store_cf_sst_meta_sql_meta_sst_exist,
                                                   dingodb::pb::common::BackupMeta& store_region_sql_meta_sst,
                                                   bool& store_region_sql_meta_sst_exist,
                                                   dingodb::pb::common::BackupMeta& coordinator_sdk_meta_sst,
                                                   bool& coordinator_sdk_meta_sst_exist) {
  butil::Status status;

  std::map<std::string, std::string> kvs;

  std::shared_ptr<SstFileReader> sst_reader = std::make_shared<SstFileReader>();

  status = sst_reader->ReadFile(path, kvs);
  if (!status.ok()) {
    std::string s = fmt::format("read sst file failed. {}", status.error_str());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  // find store_cf_sst_meta_sql_meta.sst
  store_cf_sst_meta_sql_meta_sst_exist = false;
  auto iter = kvs.find(dingodb::Constant::kStoreCfSstMetaSqlMetaSstName);
  if (iter != kvs.end()) {
    auto ret = store_cf_sst_meta_sql_meta_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kStoreCfSstMetaSqlMetaSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    store_cf_sst_meta_sql_meta_sst_exist = true;
  }

  // find store_region_sql_meta.sst
  store_region_sql_meta_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kStoreRegionSqlMetaSstName);
  if (iter != kvs.end()) {
    auto ret = store_region_sql_meta_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    store_region_sql_meta_sst_exist = true;
  }

  // find coordinator_sdk_meta.sst;
  coordinator_sdk_meta_sst_exist = false;
  iter = kvs.find(dingodb::Constant::kCoordinatorSdkMetaSstName);
  if (iter != kvs.end()) {
    auto ret = coordinator_sdk_meta_sst.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                  dingodb::Constant::kCoordinatorSdkMetaSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    coordinator_sdk_meta_sst_exist = true;
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareCoordinatorSdkMetaFunction(const std::string& path1, const std::string& path2,
                                                          bool& is_same, std::string& compare_content,
                                                          std::string& content1, std::string& content2) {
  butil::Status status;

  dingodb::pb::meta::MetaALL meta_all1;
  bool meta_all_exist1 = false;

  status = ToolDiff::ReadSstForCoordinatorSdkMeta(path1, meta_all1, meta_all_exist1);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  dingodb::pb::meta::MetaALL meta_all2;
  bool meta_all_exist2 = false;
  status = ToolDiff::ReadSstForCoordinatorSdkMeta(path2, meta_all2, meta_all_exist2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  int32_t i = 0;
  if (meta_all_exist1) {
    content1 +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kCoordinatorSdkMetaKeyName, meta_all1.DebugString());
    i++;
  }

  i = 0;
  if (meta_all_exist2) {
    content2 +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kCoordinatorSdkMetaKeyName, meta_all2.DebugString());
    i++;
  }

  // compare MetaALL coordinator_sdk_meta.sst
  is_same = true;
  if (meta_all_exist1 && !meta_all_exist2) {
    is_same = false;
  }

  if (!meta_all_exist1 && meta_all_exist2) {
    is_same = false;
  }

  if (meta_all_exist1 && meta_all_exist2) {
    ToolDiff::CompareCoordinatorSdkMetaPb("coordinator_sdk_meta.sst", meta_all1, meta_all2, is_same, compare_content);
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::ReadSstForCoordinatorSdkMeta(const std::string& path, dingodb::pb::meta::MetaALL& meta_all,
                                                     bool& meta_all_exist) {
  butil::Status status;
  std::map<std::string, std::string> kvs;

  std::shared_ptr<SstFileReader> sst_reader = std::make_shared<SstFileReader>();

  status = sst_reader->ReadFile(path, kvs);
  if (!status.ok()) {
    std::string s = fmt::format("read sst file failed. {}", status.error_str());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  // find dingodb::Constant::kCoordinatorSdkMetaKeyName
  meta_all_exist = false;
  auto iter = kvs.find(dingodb::Constant::kCoordinatorSdkMetaKeyName);
  if (iter != kvs.end()) {
    auto ret = meta_all.ParseFromString(iter->second);
    if (!ret) {
      std::string s =
          fmt::format("parse dingodb::pb::meta::MetaALL failed : {}", dingodb::Constant::kCoordinatorSdkMetaKeyName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    meta_all_exist = true;
  }
  return butil::Status::OK();
}

void ToolDiff::CompareCoordinatorSdkMetaPb(const std::string& sst_name, const dingodb::pb::meta::MetaALL& meta_all_1,
                                           const dingodb::pb::meta::MetaALL& meta_all_2, bool& is_same,
                                           std::string& compare_content) {
  // compare tenants
  std::map<int64_t, dingodb::pb::meta::Tenant> tenants_map_1;
  for (int i = 0; i < meta_all_1.tenants_size(); i++) {
    const auto& tenant = meta_all_1.tenants(i);
    tenants_map_1[tenant.id()] = tenant;
  }

  std::map<int64_t, dingodb::pb::meta::Tenant> tenants_map_2;
  for (int i = 0; i < meta_all_2.tenants_size(); i++) {
    const auto& tenant = meta_all_2.tenants(i);
    tenants_map_2[tenant.id()] = tenant;
  }

  if (tenants_map_1.size() != tenants_map_2.size()) {
    is_same = false;
  }

  for (const auto& [id, tenant] : tenants_map_1) {
    auto iter = tenants_map_2.find(id);
    if (iter == tenants_map_2.end()) {
      is_same = false;
      ToolDiff::CompareTenantPb("tenants", tenant, std::nullopt, is_same, compare_content);

      compare_content += "\n";
    } else {
      ToolDiff::CompareTenantPb("tenants", tenant, iter->second, is_same, compare_content);
      compare_content += "\n";
    }
  }

  for (const auto& [id, tenant] : tenants_map_2) {
    auto iter = tenants_map_1.find(id);
    if (iter == tenants_map_1.end()) {
      is_same = false;
      ToolDiff::CompareTenantPb("tenants", std::nullopt, tenant, is_same, compare_content);
      compare_content += "\n";
    }
  }

  // compare schemas
  std::map<int64_t, dingodb::pb::meta::Schemas> schemas_map_1;
  for (const auto& [id, schemas] : meta_all_1.schemas()) {
    schemas_map_1[id] = schemas;
  }

  std::map<int64_t, dingodb::pb::meta::Schemas> schemas_map_2;
  for (const auto& [id, schemas] : meta_all_2.schemas()) {
    schemas_map_2[id] = schemas;
  }

  if (schemas_map_1.size() != schemas_map_2.size()) {
    is_same = false;
  }

  for (const auto& [id, schemas] : schemas_map_1) {
    auto iter = schemas_map_2.find(id);
    if (iter == schemas_map_2.end()) {
      is_same = false;
      ToolDiff::CompareSchemasPb("schemas", schemas, std::nullopt, is_same, compare_content);

    } else {
      ToolDiff::CompareSchemasPb("schemas", schemas, iter->second, is_same, compare_content);
    }
    compare_content += "\n";
  }

  for (const auto& [id, schemas] : schemas_map_2) {
    auto iter = schemas_map_1.find(id);
    if (iter == schemas_map_1.end()) {
      is_same = false;
      ToolDiff::CompareSchemasPb("schemas", std::nullopt, schemas, is_same, compare_content);
      compare_content += "\n";
    }
  }

  // compare tables_and_indexes
  std::map<std::string, dingodb::pb::meta::TablesAndIndexes> tables_and_indexes_map_1;
  for (const auto& [id, tables_and_index] : meta_all_1.tables_and_indexes()) {
    tables_and_indexes_map_1[id] = tables_and_index;
  }

  std::map<std::string, dingodb::pb::meta::TablesAndIndexes> tables_and_indexes_map_2;
  for (const auto& [id, tables_and_index] : meta_all_2.tables_and_indexes()) {
    tables_and_indexes_map_2[id] = tables_and_index;
  }

  if (tables_and_indexes_map_1.size() != tables_and_indexes_map_2.size()) {
    is_same = false;
  }

  for (const auto& [id, tables_and_index] : tables_and_indexes_map_1) {
    auto iter = tables_and_indexes_map_2.find(id);
    if (iter == tables_and_indexes_map_2.end()) {
      is_same = false;
      ToolDiff::CompareTablesAndIndexesPb("tables_and_indexes", tables_and_index, std::nullopt, is_same,
                                          compare_content);
    } else {
      ToolDiff::CompareTablesAndIndexesPb("tables_and_indexes", tables_and_index, iter->second, is_same,
                                          compare_content);
    }
    compare_content += "\n";
  }
  for (const auto& [id, tables_and_index] : tables_and_indexes_map_2) {
    auto iter = tables_and_indexes_map_1.find(id);
    if (iter == tables_and_indexes_map_1.end()) {
      is_same = false;
      ToolDiff::CompareTablesAndIndexesPb("tables_and_indexes", std::nullopt, tables_and_index, is_same,
                                          compare_content);
      compare_content += "\n";
    }
  }
}

void ToolDiff::CompareTenantPb(const std::string& name, const std::optional<dingodb::pb::meta::Tenant>& tenant_1,
                               const std::optional<dingodb::pb::meta::Tenant>& tenant_2, bool& is_same,
                               std::string& compare_content) {
  std::string is_same_str;

  if (tenant_1.has_value() && !tenant_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
    compare_content +=
        fmt::format("\n {} {} dingodb::pb::meta::Tenant.id L:{} | R:{}", is_same_str, name, tenant_1->id(), "");
    compare_content +=
        fmt::format("\n {} {} dingodb::pb::meta::Tenant.name L:{} | R:{}", is_same_str, name, tenant_1->name(), "");
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.comment L:{} | R:{}", is_same_str, name,
                                   tenant_1->comment(), "");
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.create_timestamp L:{} | R:{}", is_same_str, name,
                                   tenant_1->create_timestamp(), "");
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.update_timestamp L:{} | R:{}", is_same_str, name,
                                   tenant_1->update_timestamp(), "");
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.delete_timestamp L:{} | R:{}", is_same_str, name,
                                   tenant_1->delete_timestamp(), "");
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.safe_point_ts L:{} | R:{}", is_same_str, name,
                                   tenant_1->safe_point_ts(), "");
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.revision L:{} | R:{}", is_same_str, name,
                                   tenant_1->revision(), "");
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.resolve_lock_safe_point_ts L:{} | R:{}",
                                   is_same_str, name, tenant_1->resolve_lock_safe_point_ts(), "");
  } else if (!tenant_1.has_value() && tenant_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
    compare_content +=
        fmt::format("\n {} {} dingodb::pb::meta::Tenant.id L:{} | R:{}", is_same_str, name, "", tenant_2->id());
    compare_content +=
        fmt::format("\n {} {} dingodb::pb::meta::Tenant.name L:{} | R:{}", is_same_str, name, "", tenant_2->name());
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.comment L:{} | R:{}", is_same_str, name, "",
                                   tenant_2->comment());
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.create_timestamp L:{} | R:{}", is_same_str, name,
                                   "", tenant_2->create_timestamp());
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.update_timestamp L:{} | R:{}", is_same_str, name,
                                   "", tenant_2->update_timestamp());
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.delete_timestamp L:{} | R:{}", is_same_str, name,
                                   "", tenant_2->delete_timestamp());
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.safe_point_ts L:{} | R:{}", is_same_str, name,
                                   "", tenant_2->safe_point_ts());
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.revision L:{} | R:{}", is_same_str, name, "",
                                   tenant_2->revision());
    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.resolve_lock_safe_point_ts L:{} | R:{}",
                                   is_same_str, name, "", tenant_2->resolve_lock_safe_point_ts());
  } else {  // both has value

    {
      if (tenant_1->id() != tenant_2->id()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.id L:{} | R:{}", is_same_str, name,
                                     tenant_1->id(), tenant_2->id());
    }

    {
      if (tenant_1->name() != tenant_2->name()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.name L:{} | R:{}", is_same_str, name,
                                     tenant_1->name(), tenant_2->name());
    }

    {
      if (tenant_1->comment() != tenant_2->comment()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }

      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.comment L:{} | R:{}", is_same_str, name,
                                     tenant_1->comment(), tenant_2->comment());
    }

    {
      if (tenant_1->create_timestamp() != tenant_2->create_timestamp()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.create_timestamp L:{} | R:{}", is_same_str,
                                     name, tenant_1->create_timestamp(), tenant_2->create_timestamp());
    }

    {
      if (tenant_1->update_timestamp() != tenant_2->update_timestamp()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.update_timestamp L:{} | R:{}", is_same_str,
                                     name, tenant_1->update_timestamp(), tenant_2->update_timestamp());
    }

    {
      if (tenant_1->delete_timestamp() != tenant_2->delete_timestamp()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.delete_timestamp L:{} | R:{}", is_same_str,
                                     name, tenant_1->delete_timestamp(), tenant_2->delete_timestamp());
    }

    {
      if (tenant_1->safe_point_ts() != tenant_2->safe_point_ts()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.safe_point_ts L:{} | R:{}", is_same_str, name,
                                     tenant_1->safe_point_ts(), tenant_2->safe_point_ts());
    }

    {
      if (tenant_1->revision() != tenant_2->revision()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Tenant.revision L:{} | R:{}", is_same_str, name,
                                     tenant_1->revision(), tenant_2->revision());
    }

    {
      if (tenant_1->resolve_lock_safe_point_ts() != tenant_2->resolve_lock_safe_point_ts()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content +=
          fmt::format("\n {} {} dingodb::pb::meta::Tenant.resolve_lock_safe_point_ts L:{} | R:{}", is_same_str, name,
                      tenant_1->resolve_lock_safe_point_ts(), tenant_2->resolve_lock_safe_point_ts());
    }
  }
}

void ToolDiff::CompareSchemasPb(const std::string& name, const std::optional<dingodb::pb::meta::Schemas>& schemas_1,
                                const std::optional<dingodb::pb::meta::Schemas>& schemas_2, bool& is_same,
                                std::string& compare_content) {
  std::map<std::string, dingodb::pb::meta::Schema> schemas_map_1;
  for (int i = 0; i < schemas_1->schemas_size(); i++) {
    const auto& schema = schemas_1->schemas(i);
    std::string common_id_string =
        fmt::format("{}|{}|{}|{}", dingodb::pb::meta::EntityType_Name(schema.id().entity_type()),
                    schema.id().parent_entity_id(), schema.id().parent_entity_id(), schema.name());
    schemas_map_1[common_id_string] = schema;
  }

  std::map<std::string, dingodb::pb::meta::Schema> schemas_map_2;
  for (int i = 0; i < schemas_2->schemas_size(); i++) {
    const auto& schema = schemas_2->schemas(i);
    std::string common_id_string =
        fmt::format("{}|{}|{}|{}", dingodb::pb::meta::EntityType_Name(schema.id().entity_type()),
                    schema.id().parent_entity_id(), schema.id().parent_entity_id(), schema.name());
    schemas_map_2[common_id_string] = schema;
  }

  if (schemas_map_1.size() != schemas_map_2.size()) {
    is_same = false;
  }

  for (const auto& [id, schema] : schemas_map_1) {
    auto iter = schemas_map_2.find(id);
    if (iter == schemas_map_2.end()) {
      is_same = false;
      ToolDiff::CompareSchemaPb(name + " schemas", schema, std::nullopt, is_same, compare_content);
    } else {
      ToolDiff::CompareSchemaPb(name + " schemas", schema, iter->second, is_same, compare_content);
    }
    compare_content += "\n";
  }

  for (const auto& [id, schema] : schemas_map_2) {
    auto iter = schemas_map_1.find(id);
    if (iter == schemas_map_1.end()) {
      is_same = false;
      ToolDiff::CompareSchemaPb(name + " schemas", std::nullopt, schema, is_same, compare_content);
      compare_content += "\n";
    }
  }
}

void ToolDiff::CompareSchemaPb(const std::string& name, const std::optional<dingodb::pb::meta::Schema>& schema_1,
                               const std::optional<dingodb::pb::meta::Schema>& schema_2, bool& is_same,
                               std::string& compare_content) {
  std::string is_same_str;

  if (schema_1.has_value() && !schema_2.has_value()) {
    is_same = false;
    is_same_str = "≠";

    ToolDiff::CompareDingoCommonIdPb(name, "dingodb::pb::meta::Schema.id", schema_1->id(), std::nullopt, is_same,
                                     compare_content);

    compare_content +=
        fmt::format("\n {} {} dingodb::pb::meta::Schema.name L:{} | R:{}", is_same_str, name, schema_1->name(), "");
    // compare table_ids
    ToolDiff::CompareDingoCommonIdsPb(name, "dingodb::pb::meta::Schema.table_ids",
                                      dingodb::Helper::PbRepeatedToVector(schema_1->table_ids()), std::nullopt, is_same,
                                      compare_content);

    // compare index_ids
    ToolDiff::CompareDingoCommonIdsPb(name, "dingodb::pb::meta::Schema.index_ids",
                                      dingodb::Helper::PbRepeatedToVector(schema_1->index_ids()), std::nullopt, is_same,
                                      compare_content);

    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Schema.revision L:{} | R:{}", is_same_str, name,
                                   schema_1->revision(), "");

    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Schema.tenant_id L:{} | R:{}", is_same_str, name,
                                   schema_1->tenant_id(), "");

  } else if (!schema_1.has_value() && schema_2.has_value()) {
    is_same = false;
    is_same_str = "≠";

    ToolDiff::CompareDingoCommonIdPb(name, "dingodb::pb::meta::Schema.id", std::nullopt, schema_2->id(), is_same,
                                     compare_content);

    compare_content +=
        fmt::format("\n {} {} dingodb::pb::meta::Schema.name L:{} | R:{}", is_same_str, name, "", schema_2->name());
    // compare table_ids
    ToolDiff::CompareDingoCommonIdsPb(name, "dingodb::pb::meta::Schema.table_ids", std::nullopt,
                                      dingodb::Helper::PbRepeatedToVector(schema_2->table_ids()), is_same,
                                      compare_content);
    // compare index_ids
    ToolDiff::CompareDingoCommonIdsPb(name, "dingodb::pb::meta::Schema.index_ids", std::nullopt,
                                      dingodb::Helper::PbRepeatedToVector(schema_2->index_ids()), is_same,
                                      compare_content);

    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Schema.revision L:{} | R:{}", is_same_str, name, "",
                                   schema_2->revision());

    compare_content += fmt::format("\n {} {} dingodb::pb::meta::Schema.tenant_id L:{} | R:{}", is_same_str, name, "",
                                   schema_2->tenant_id());
  } else {
    ToolDiff::CompareDingoCommonIdPb(name, "dingodb::pb::meta::Schema.id", schema_1->id(), schema_2->id(), is_same,
                                     compare_content);

    {
      if (schema_1->name() != schema_2->name()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Schema.name L:{} | R:{}", is_same_str, name,
                                     schema_1->name(), schema_2->name());
    }

    // compare table_ids
    ToolDiff::CompareDingoCommonIdsPb(
        name, "dingodb::pb::meta::Schema.table_ids", dingodb::Helper::PbRepeatedToVector(schema_1->table_ids()),
        dingodb::Helper::PbRepeatedToVector(schema_2->table_ids()), is_same, compare_content);

    // compare index_ids
    ToolDiff::CompareDingoCommonIdsPb(
        name, "dingodb::pb::meta::Schema.index_ids", dingodb::Helper::PbRepeatedToVector(schema_1->index_ids()),
        dingodb::Helper::PbRepeatedToVector(schema_2->index_ids()), is_same, compare_content);

    {
      if (schema_1->revision() != schema_2->revision()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Schema.revision L:{} | R:{}", is_same_str, name,
                                     schema_1->revision(), schema_2->revision());
    }

    {
      if (schema_1->tenant_id() != schema_2->tenant_id()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} dingodb::pb::meta::Schema.tenant_id L:{} | R:{}", is_same_str, name,
                                     schema_1->tenant_id(), schema_2->tenant_id());
    }
  }
  compare_content += "\n";
}

void ToolDiff::CompareDingoCommonIdsPb(const std::string& name, const std::string& name2,
                                       const std::optional<std::vector<dingodb::pb::meta::DingoCommonId>>& common_ids_1,
                                       const std::optional<std::vector<dingodb::pb::meta::DingoCommonId>>& common_ids_2,
                                       bool& is_same, std::string& compare_content) {
  std::map<std::string, dingodb::pb::meta::DingoCommonId> common_ids_map_1;
  std::map<std::string, dingodb::pb::meta::DingoCommonId> common_ids_map_2;

  if (common_ids_1.has_value()) {
    for (const auto& common_id : common_ids_1.value()) {
      std::string common_id_string =
          fmt::format("{}|{}|{}", dingodb::pb::meta::EntityType_Name(common_id.entity_type()),
                      common_id.parent_entity_id(), common_id.parent_entity_id());
      common_ids_map_1[common_id_string] = common_id;
    }
  }

  if (common_ids_2.has_value()) {
    for (const auto& common_id : common_ids_2.value()) {
      std::string common_id_string =
          fmt::format("{}|{}|{}", dingodb::pb::meta::EntityType_Name(common_id.entity_type()),
                      common_id.parent_entity_id(), common_id.parent_entity_id());
      common_ids_map_2[common_id_string] = common_id;
    }
  }

  if (common_ids_map_1.size() != common_ids_map_2.size()) {
    is_same = false;
  }

  for (const auto& [id, common_id] : common_ids_map_1) {
    auto iter = common_ids_map_2.find(id);
    if (iter == common_ids_map_2.end()) {
      is_same = false;
      ToolDiff::CompareDingoCommonIdPb(name, name2, common_id, std::nullopt, is_same, compare_content);
    } else {
      ToolDiff::CompareDingoCommonIdPb(name, name2, common_id, iter->second, is_same, compare_content);
    }
  }

  for (const auto& [id, common_id] : common_ids_map_2) {
    auto iter = common_ids_map_1.find(id);
    if (iter == common_ids_map_1.end()) {
      is_same = false;
      ToolDiff::CompareDingoCommonIdPb(name, name2, std::nullopt, common_id, is_same, compare_content);
    }
  }
}

void ToolDiff::CompareDingoCommonIdPb(const std::string& name, const std::string& name2,
                                      const std::optional<dingodb::pb::meta::DingoCommonId>& common_id_1,
                                      const std::optional<dingodb::pb::meta::DingoCommonId>& common_id_2, bool& is_same,
                                      std::string& compare_content) {
  std::string is_same_str;

  if (common_id_1.has_value() && !common_id_2.has_value()) {
    is_same = false;
    is_same_str = "≠";

    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::DingoCommonId.entity_type L:{} | R:{}", is_same_str,
                                   name, name2, dingodb::pb::meta::EntityType_Name(common_id_1->entity_type()), "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::DingoCommonId.parent_entity_id L:{} | R:{}",
                                   is_same_str, name, name2, common_id_1->parent_entity_id(), "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::DingoCommonId.entity_id L:{} | R:{}", is_same_str,
                                   name, name2, common_id_1->entity_id(), "");

  } else if (!common_id_1.has_value() && common_id_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::DingoCommonId.entity_type L:{} | R:{}", is_same_str,
                                   name, name2, "", dingodb::pb::meta::EntityType_Name(common_id_2->entity_type()));
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::DingoCommonId.parent_entity_id L:{} | R:{}",
                                   is_same_str, name, name2, "", common_id_2->parent_entity_id());
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::DingoCommonId.entity_id L:{} | R:{}", is_same_str,
                                   name, name2, "", common_id_2->entity_id());
  } else {  // both compare
    {
      if (common_id_1->entity_type() != common_id_2->entity_type()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content +=
          fmt::format("\n {} {} {}.dingodb::pb::meta::DingoCommonId.entity_type L:{} | R:{}", is_same_str, name, name2,
                      dingodb::pb::meta::EntityType_Name(common_id_1->entity_type()),
                      dingodb::pb::meta::EntityType_Name(common_id_2->entity_type()));
    }

    {
      if (common_id_1->entity_id() != common_id_2->entity_id()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content +=
          fmt::format("\n {} {} {}.dingodb::pb::meta::DingoCommonId.parent_entity_id L:{} | R:{}", is_same_str, name,
                      name2, common_id_1->parent_entity_id(), common_id_2->parent_entity_id());
    }

    {
      if (common_id_1->entity_id() != common_id_2->entity_id()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::DingoCommonId.entity_id L:{} | R:{}", is_same_str,
                                     name, name2, common_id_1->entity_id(), common_id_2->entity_id());
    }
  }
}

void ToolDiff::CompareTablesAndIndexesPb(const std::string& name,
                                         const std::optional<dingodb::pb::meta::TablesAndIndexes>& tables_and_indexes_1,
                                         const std::optional<dingodb::pb::meta::TablesAndIndexes>& tables_and_indexes_2,
                                         bool& is_same, std::string& compare_content) {
  // TODO : ignore  tables compare
  std::map<std::string, dingodb::pb::meta::IndexDefinitionWithId> indexes_map_1;

  if (tables_and_indexes_1.has_value()) {
    for (int i = 0; i < tables_and_indexes_1->indexes_size(); i++) {
      const auto& index = tables_and_indexes_1->indexes(i);
      std::string common_id_string =
          fmt::format("{}|{}|{}", dingodb::pb::meta::EntityType_Name(index.index_id().entity_type()),
                      index.index_id().parent_entity_id(), index.index_id().entity_id());
      indexes_map_1[common_id_string] = index;
    }
  }

  std::map<std::string, dingodb::pb::meta::IndexDefinitionWithId> indexes_map_2;
  if (tables_and_indexes_2.has_value()) {
    for (int i = 0; i < tables_and_indexes_2->indexes_size(); i++) {
      const auto& index = tables_and_indexes_2->indexes(i);
      std::string common_id_string =
          fmt::format("{}|{}|{}", dingodb::pb::meta::EntityType_Name(index.index_id().entity_type()),
                      index.index_id().parent_entity_id(), index.index_id().entity_id());
      indexes_map_2[common_id_string] = index;
    }
  }

  if (indexes_map_1.size() != indexes_map_2.size()) {
    is_same = false;
  }

  for (const auto& [id, index] : indexes_map_1) {
    auto iter = indexes_map_2.find(id);
    if (iter == indexes_map_2.end()) {
      is_same = false;
      ToolDiff::CompareIndexDefinitionWithIdPb(name, "indexes", index, std::nullopt, is_same, compare_content);
    } else {
      ToolDiff::CompareIndexDefinitionWithIdPb(name, "indexes", index, iter->second, is_same, compare_content);
    }
    compare_content += "\n";
  }
  for (const auto& [id, index] : indexes_map_2) {
    auto iter = indexes_map_1.find(id);
    if (iter == indexes_map_1.end()) {
      is_same = false;
      ToolDiff::CompareIndexDefinitionWithIdPb(name, " indexes", std::nullopt, index, is_same, compare_content);
      compare_content += "\n";
    }
  }
}

void ToolDiff::CompareIndexDefinitionWithIdPb(
    const std::string& name, const std::string& name2,
    const std::optional<dingodb::pb::meta::IndexDefinitionWithId>& index_definition_with_id_1,
    const std::optional<dingodb::pb::meta::IndexDefinitionWithId>& index_definition_with_id_2, bool& is_same,
    std::string& compare_content) {
  std::string is_same_str;
  if (index_definition_with_id_1.has_value() && !index_definition_with_id_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
  } else if (!index_definition_with_id_1.has_value() && index_definition_with_id_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
  } else {
    {
      ToolDiff::CompareDingoCommonIdPb(name + " " + name2, "index_id", index_definition_with_id_1->index_id(),
                                       index_definition_with_id_2->index_id(), is_same, compare_content);
    }

    {
      ToolDiff::CompareIndexDefinitionPb(name + " " + name2, "index_definition",
                                         index_definition_with_id_1->index_definition(),
                                         index_definition_with_id_2->index_definition(), is_same, compare_content);
    }

    {
      if (index_definition_with_id_1->tenant_id() != index_definition_with_id_2->tenant_id()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }

      compare_content += fmt::format("\n {} {} {} tenant_id L:{} | R:{}", is_same_str, name, name2,
                                     index_definition_with_id_1->tenant_id(), index_definition_with_id_2->tenant_id());
    }
  }
}

void ToolDiff::CompareIndexDefinitionPb(const std::string& name, const std::string& name2,
                                        const std::optional<dingodb::pb::meta::IndexDefinition>& index_definition_1,
                                        const std::optional<dingodb::pb::meta::IndexDefinition>& index_definition_2,
                                        bool& is_same, std::string& compare_content) {
  std::string is_same_str;
  if (index_definition_1.has_value() && !index_definition_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.name L:{} | R:{}", is_same_str, name,
                                   name2, index_definition_1->name(), "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.version L:{} | R:{}", is_same_str,
                                   name, name2, index_definition_1->version(), "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.index_partition L:{} | R:{}",
                                   is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.replica L:{} | R:{}", is_same_str,
                                   name, name2, index_definition_1->replica(), "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.index_parameter L:{} | R:{}",
                                   is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.with_auto_incrment L:{} | R:{}",
                                   is_same_str, name, name2, index_definition_1->with_auto_incrment(), "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.auto_increment L:{} | R:{}",
                                   is_same_str, name, name2, index_definition_1->auto_increment(), "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.revision L:{} | R:{}", is_same_str,
                                   name, name2, index_definition_1->revision(), "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.engine L:{} | R:{}", is_same_str,
                                   name, name2, dingodb::pb::common::Engine_Name(index_definition_1->engine()), "");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.revision L:{} | R:{}", is_same_str,
                                   name, name2, index_definition_1->revision(), "");

  } else if (!index_definition_1.has_value() && index_definition_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.name L:{} | R:{}", is_same_str, name,
                                   name2, "", index_definition_2->name());
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.version L:{} | R:{}", is_same_str,
                                   name, name2, "", index_definition_2->version());
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.index_partition L:{} | R:{}",
                                   is_same_str, name, name2, "", "...");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.replica L:{} | R:{}", is_same_str,
                                   name, name2, "", index_definition_2->replica());
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.index_parameter L:{} | R:{}",
                                   is_same_str, name, name2, "", "...");
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.with_auto_incrment L:{} | R:{}",
                                   is_same_str, name, name2, "", index_definition_2->with_auto_incrment());
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.auto_increment L:{} | R:{}",
                                   is_same_str, name, name2, "", index_definition_2->auto_increment());
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.revision L:{} | R:{}", is_same_str,
                                   name, name2, "", index_definition_2->revision());
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.engine L:{} | R:{}", is_same_str,
                                   name, name2, "", dingodb::pb::common::Engine_Name(index_definition_2->engine()));
    compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.revision L:{} | R:{}", is_same_str,
                                   name, name2, "", index_definition_2->revision());
  } else {
    {
      if (index_definition_1->name() != index_definition_2->name()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.name L:{} | R:{}", is_same_str,
                                     name, name2, index_definition_1->name(), index_definition_2->name());
    }

    {
      if (index_definition_1->version() != index_definition_2->version()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.version L:{} | R:{}", is_same_str,
                                     name, name2, index_definition_1->version(), index_definition_2->version());
    }

    {
      bool is_same_internal = google::protobuf::util::MessageDifferencer::Equals(index_definition_1->index_partition(),
                                                                                 index_definition_2->index_partition());

      if (!is_same_internal) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.index_partition L:{} | R:{}",
                                     is_same_str, name, name2, "...", "...");
    }

    {
      if (index_definition_1->replica() != index_definition_2->replica()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.replica L:{} | R:{}", is_same_str,
                                     name, name2, index_definition_1->replica(), index_definition_2->replica());
    }

    {
      bool is_same_internal = google::protobuf::util::MessageDifferencer::Equals(index_definition_1->index_parameter(),
                                                                                 index_definition_2->index_parameter());
      if (!is_same_internal) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.index_parameter L:{} | R:{}",
                                     is_same_str, name, name2, "...", "...");
    }
    {
      if (index_definition_1->with_auto_incrment() != index_definition_2->with_auto_incrment()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content +=
          fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.with_auto_incrment L:{} | R:{}", is_same_str,
                      name, name2, index_definition_1->with_auto_incrment(), index_definition_2->with_auto_incrment());
    }

    {
      if (index_definition_1->auto_increment() != index_definition_2->auto_increment()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content +=
          fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.auto_increment L:{} | R:{}", is_same_str, name,
                      name2, index_definition_1->auto_increment(), index_definition_2->auto_increment());
    }

    {
      if (index_definition_1->engine() != index_definition_2->engine()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.engine L:{} | R:{}", is_same_str,
                                     name, name2, dingodb::pb::common::Engine_Name(index_definition_1->engine()),
                                     dingodb::pb::common::Engine_Name(index_definition_2->engine()));
    }

    {
      if (index_definition_1->revision() != index_definition_2->revision()) {
        is_same = false;
        is_same_str = "≠";
      } else {
        is_same_str = "=";
      }
      compare_content += fmt::format("\n {} {} {}.dingodb::pb::meta::IndexDefinition.revision L:{} | R:{}", is_same_str,
                                     name, name2, index_definition_1->revision(), index_definition_2->revision());
    }
  }
}

butil::Status ToolDiff::CompareRegionCfSst(
    const std::string& path1, const std::string& file_name1, const std::string& path2, const std::string& file_name2,
    const std::string& desc,
    std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&, std::string&,
                                std::string&, std::string&)>
        handler) {
  butil::Status status;

  bool is_same;
  std::string compare_content;
  std::string content1;
  std::string content2;
  std::string same_content;
  std::string diff_content;

  status = handler(path1, path2, is_same, compare_content, content1, content2, same_content, diff_content);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path1          ", path1);
  ToolUtils::PrintDumpItem("path2          ", path2);

  ToolUtils::PrintDumpItem("name1          ", file_name1);
  ToolUtils::PrintDumpItem("name2          ", file_name2);

  ToolUtils::PrintDumpItem("desc           ", desc);

  ToolUtils::PrintDumpItem("is_same        ", is_same ? "true" : "false");

  ToolUtils::PrintDumpItem("same_content   ", (same_content.empty() ? "empty" : same_content));

  ToolUtils::PrintDumpItem("diff_content   ", (diff_content.empty() ? "empty" : diff_content));

  ToolUtils::PrintDumpItem("compare_content", compare_content);

  ToolUtils::PrintDumpItem("content1       ", content1);

  ToolUtils::PrintDumpItem("content2       ", content2);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareRegionDefinitionSst(
    const std::string& path1, const std::string& file_name1, const std::string& path2, const std::string& file_name2,
    const std::string& desc,
    std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&, std::string&,
                                std::string&, std::string&)>
        handler) {
  butil::Status status;

  bool is_same;
  std::string compare_content;
  std::string content1;
  std::string content2;
  std::string same_content;
  std::string diff_content;

  status = handler(path1, path2, is_same, compare_content, content1, content2, same_content, diff_content);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path1          ", path1);
  ToolUtils::PrintDumpItem("path2          ", path2);

  ToolUtils::PrintDumpItem("name1          ", file_name1);
  ToolUtils::PrintDumpItem("name2          ", file_name2);

  ToolUtils::PrintDumpItem("desc           ", desc);

  ToolUtils::PrintDumpItem("is_same        ", is_same ? "true" : "false");

  ToolUtils::PrintDumpItem("same_content   ", (same_content.empty() ? "empty" : same_content));

  ToolUtils::PrintDumpItem("diff_content   ", (diff_content.empty() ? "empty" : diff_content));

  ToolUtils::PrintDumpItem("compare_content", compare_content);

  ToolUtils::PrintDumpItem("content1       ", content1);

  ToolUtils::PrintDumpItem("content2       ", content2);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareRegionCfFunction(const std::string& path1, const std::string& path2, bool& is_same,
                                                std::string& compare_content, std::string& content1,
                                                std::string& content2, std::string& same_content,
                                                std::string& diff_content) {
  butil::Status status;

  std::map<std::string, dingodb::pb::common::BackupDataFileValueSstMetaGroup> groups1;

  status = ToolDiff::ReadSstForBackupDataFileValueSstMetaGroup(path1, groups1);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::map<std::string, dingodb::pb::common::BackupDataFileValueSstMetaGroup> groups2;
  status = ToolDiff::ReadSstForBackupDataFileValueSstMetaGroup(path2, groups2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  int32_t i = 0;

  for (const auto& [id, group] : groups1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, id, group.DebugString());
    i++;
  }

  i = 0;
  for (const auto& [id, group] : groups2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, id, group.DebugString());
    i++;
  }

  // compare MetaALL coordinator_sdk_meta.sst
  is_same = true;
  if (groups2.size() != groups2.size()) {
    is_same = false;
  }

  for (const auto& [id, group] : groups1) {
    auto iter = groups2.find(id);
    if (iter == groups2.end()) {
      is_same = false;
      ToolDiff::CompareBackupDataFileValueSstMetaGroupPb("", "", group, std::nullopt, is_same, compare_content,
                                                         same_content, diff_content);

    } else {
      ToolDiff::CompareBackupDataFileValueSstMetaGroupPb("", "", group, iter->second, is_same, compare_content,
                                                         same_content, diff_content);
    }
  }

  for (const auto& [id, group] : groups2) {
    auto iter = groups1.find(id);
    if (iter == groups1.end()) {
      is_same = false;
      ToolDiff::CompareBackupDataFileValueSstMetaGroupPb("", "", std::nullopt, group, is_same, compare_content,
                                                         same_content, diff_content);
      compare_content += "\n";
    }
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::ReadSstForBackupDataFileValueSstMetaGroup(
    const std::string& path, std::map<std::string, dingodb::pb::common::BackupDataFileValueSstMetaGroup>& groups) {
  butil::Status status;

  std::map<std::string, std::string> kvs;
  std::shared_ptr<SstFileReader> sst_reader = std::make_shared<SstFileReader>();

  status = sst_reader->ReadFile(path, kvs);
  if (!status.ok()) {
    std::string s = fmt::format("read sst file failed. {}", status.error_str());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  for (const auto& [region_string, backup_data_file_value_sst_meta_group_string] : kvs) {
    dingodb::pb::common::BackupDataFileValueSstMetaGroup backup_data_file_value_sst_meta_group;
    auto ret = backup_data_file_value_sst_meta_group.ParseFromString(backup_data_file_value_sst_meta_group_string);
    if (!ret) {
      std::string s =
          fmt::format("parse dingodb::pb::common::BackupDataFileValueSstMetaGroup failed : {}", region_string);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    groups[region_string] = backup_data_file_value_sst_meta_group;
  }

  return butil::Status::OK();
}

void ToolDiff::CompareBackupDataFileValueSstMetaGroupPb(
    const std::string& name, const std::string& name2,
    const std::optional<dingodb::pb::common::BackupDataFileValueSstMetaGroup>& group_1,
    const std::optional<dingodb::pb::common::BackupDataFileValueSstMetaGroup>& group_2, bool& is_same,
    std::string& compare_content, std::string& same_content, std::string& diff_content) {
  std::string is_same_str;
  if (group_1.has_value() && !group_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
  } else if (!group_1.has_value() && group_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
  } else {
    std::map<std::string, dingodb::pb::common::BackupDataFileValueSstMeta> backup_data_file_value_sst_meta_map_1;
    for (int i = 0; i < group_1->backup_data_file_value_sst_metas_size(); i++) {
      const auto& backup_data_file_value_sst_meta = group_1->backup_data_file_value_sst_metas(i);
      std::string common_id_string =
          fmt::format("{}-{}", backup_data_file_value_sst_meta.region_id(), backup_data_file_value_sst_meta.cf());
      backup_data_file_value_sst_meta_map_1[common_id_string] = backup_data_file_value_sst_meta;
    }

    std::map<std::string, dingodb::pb::common::BackupDataFileValueSstMeta> backup_data_file_value_sst_meta_map_2;
    for (int i = 0; i < group_2->backup_data_file_value_sst_metas_size(); i++) {
      const auto& backup_data_file_value_sst_meta = group_2->backup_data_file_value_sst_metas(i);
      std::string common_id_string =
          fmt::format("{}-{}", backup_data_file_value_sst_meta.region_id(), backup_data_file_value_sst_meta.cf());
      backup_data_file_value_sst_meta_map_2[common_id_string] = backup_data_file_value_sst_meta;
    }

    bool is_same_content = true;
    if (backup_data_file_value_sst_meta_map_1.size() != backup_data_file_value_sst_meta_map_2.size()) {
      is_same = false;
      is_same_content = false;
    }

    bool is_first = false;
    for (const auto& [id, backup_data_file_value_sst_meta] : backup_data_file_value_sst_meta_map_1) {
      if (!is_first) {
        is_first = true;
        compare_content += "\n{";
      }
      auto iter = backup_data_file_value_sst_meta_map_2.find(id);
      if (iter == backup_data_file_value_sst_meta_map_2.end()) {
        is_same = false;
        is_same_content = false;
        ToolDiff::CompareBackupDataFileValueSstMetaPb(name, name2, backup_data_file_value_sst_meta, std::nullopt,
                                                      is_same, compare_content, is_same_content);

      } else {
        ToolDiff::CompareBackupDataFileValueSstMetaPb(name, name2, backup_data_file_value_sst_meta, iter->second,
                                                      is_same, compare_content, is_same_content);
      }
      compare_content += "\n";
    }

    for (const auto& [id, backup_data_file_value_sst_meta] : backup_data_file_value_sst_meta_map_2) {
      auto iter = backup_data_file_value_sst_meta_map_1.find(id);
      if (iter == backup_data_file_value_sst_meta_map_1.end()) {
        is_same = false;
        is_same_content = false;
        ToolDiff::CompareBackupDataFileValueSstMetaPb(name, name2, std::nullopt, backup_data_file_value_sst_meta,
                                                      is_same, compare_content, is_same_content);
        compare_content += "\n";
      }
    }

    if (is_first) {
      compare_content += "}\n";
    }

    int64_t id = 0;
    if (group_1->backup_data_file_value_sst_metas_size() > 0) {
      id = group_1->backup_data_file_value_sst_metas(0).region_id();
    }

    if (id == 0) {
      if (group_2->backup_data_file_value_sst_metas_size() > 0) {
        id = group_2->backup_data_file_value_sst_metas(0).region_id();
      }
    }

    if (is_same_content) {
      if (id > 0) {
        same_content += std::to_string(id) + " ";
      }

    } else {
      if (id > 0) {
        diff_content += std::to_string(id) + " ";
      }
    }
  }
}

void ToolDiff::CompareBackupDataFileValueSstMetaPb(
    const std::string& name, const std::string& name2,
    const std::optional<dingodb::pb::common::BackupDataFileValueSstMeta>& group_1,
    const std::optional<dingodb::pb::common::BackupDataFileValueSstMeta>& group_2, bool& is_same,
    std::string& compare_content, bool& is_same_content) {
  std::string is_same_str;
  if (group_1.has_value() && !group_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
    is_same_content = false;
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.cf L:{} | R:{}", is_same_str, name, name2,
                                   group_1->cf(), "");
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.region_id L:{} | R:{}", is_same_str, name,
                                   name2, group_1->region_id(), "");
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.dir_name L:{} | R:{}", is_same_str, name,
                                   name2, group_1->dir_name(), "");
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.file_size L:{} | R:{}", is_same_str, name,
                                   name2, group_1->file_size(), "");
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.encryption L:{} | R:{}", is_same_str, name,
                                   name2, group_1->encryption(), "");
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.file_name L:{} | R:{}", is_same_str, name,
                                   name2, group_1->file_name(), "");

  } else if (!group_1.has_value() && group_2.has_value()) {
    is_same = false;
    is_same_str = "≠";
    is_same_content = false;
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.cf L:{} | R:{}", is_same_str, name, name2,
                                   "", group_2->cf());
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.region_id L:{} | R:{}", is_same_str, name,
                                   name2, "", group_2->region_id());
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.dir_name L:{} | R:{}", is_same_str, name,
                                   name2, "", group_2->dir_name());
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.file_size L:{} | R:{}", is_same_str, name,
                                   name2, "", group_2->file_size());
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.encryption L:{} | R:{}", is_same_str, name,
                                   name2, "", group_2->encryption());
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.file_name L:{} | R:{}", is_same_str, name,
                                   name2, "", group_2->file_name());
  } else {  // both
    if (group_1->cf() != group_2->cf()) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.cf L:{} | R:{}", is_same_str, name, name2,
                                   group_1->cf(), group_2->cf());

    if (group_1->region_id() != group_2->region_id()) {
      is_same = false;
      is_same_content = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.region_id L:{} | R:{}", is_same_str, name,
                                   name2, group_1->region_id(), group_2->region_id());

    if (group_1->dir_name() != group_2->dir_name()) {
      is_same = false;
      is_same_content = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.dir_name L:{} | R:{}", is_same_str, name,
                                   name2, group_1->dir_name(), group_2->dir_name());

    if (group_1->file_size() != group_2->file_size()) {
      is_same = false;
      is_same_content = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.file_size L:{} | R:{}", is_same_str, name,
                                   name2, group_1->file_size(), group_2->file_size());

    if (group_1->encryption() != group_2->encryption()) {
      is_same = false;
      is_same_content = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.encryption L:{} | R:{}", is_same_str, name,
                                   name2, group_1->encryption(), group_2->encryption());

    if (group_1->file_name() != group_2->file_name()) {
      is_same = false;
      is_same_content = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.BackupDataFileValueSstMeta.file_name L:{} | R:{}", is_same_str, name,
                                   name2, group_1->file_name(), group_2->file_name());
  }
}

butil::Status ToolDiff::CompareRegionDefintionSst(const std::string& path1, const std::string& path2, bool& is_same,
                                                  std::string& compare_content, std::string& content1,
                                                  std::string& content2, std::string& same_content,
                                                  std::string& diff_content) {
  return butil::Status();
}

butil::Status ToolDiff::CompareRegionDefinitionFunction(const std::string& path1, const std::string& path2,
                                                        bool& is_same, std::string& compare_content,
                                                        std::string& content1, std::string& content2,
                                                        std::string& same_content, std::string& diff_content) {
  butil::Status status;

  std::map<std::string, dingodb::pb::common::Region> regions1;

  status = ToolDiff::ReadSstForRegion(path1, regions1);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::map<std::string, dingodb::pb::common::Region> regions2;
  status = ToolDiff::ReadSstForRegion(path2, regions2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  int32_t i = 0;

  for (const auto& [id, region] : regions1) {
    content1 += fmt::format("\n[{}] [{}] :\n{}", i, id, region.DebugString());
    i++;
  }

  i = 0;
  for (const auto& [id, region] : regions2) {
    content2 += fmt::format("\n[{}] [{}] :\n{}", i, id, region.DebugString());
    i++;
  }

  // compare MetaALL coordinator_sdk_meta.sst
  is_same = true;
  if (regions1.size() != regions2.size()) {
    is_same = false;
  }

  for (const auto& [id, region] : regions1) {
    auto iter = regions2.find(id);
    if (iter == regions2.end()) {
      is_same = false;
      ToolDiff::CompareRegionPb("", "", region, std::nullopt, is_same, compare_content, same_content, diff_content);

    } else {
      ToolDiff::CompareRegionPb("", "", region, iter->second, is_same, compare_content, same_content, diff_content);
    }
    compare_content += "\n";
  }

  for (const auto& [id, region] : regions2) {
    auto iter = regions1.find(id);
    if (iter == regions1.end()) {
      is_same = false;
      ToolDiff::CompareRegionPb("", "", std::nullopt, region, is_same, compare_content, same_content, diff_content);
      compare_content += "\n";
    }
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::ReadSstForRegion(const std::string& path,
                                         std::map<std::string, dingodb::pb::common::Region>& regions) {
  butil::Status status;

  std::map<std::string, std::string> kvs;

  std::shared_ptr<SstFileReader> sst_reader = std::make_shared<SstFileReader>();

  status = sst_reader->ReadFile(path, kvs);
  if (!status.ok()) {
    std::string s = fmt::format("read sst file failed. {}", status.error_str());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  int32_t i = 0;

  for (const auto& [region_key_string, region_value_string] : kvs) {
    dingodb::pb::common::Region region;
    auto ret = region.ParseFromString(region_value_string);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::Region failed : {}", region_key_string);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    regions[region_key_string] = region;
  }

  return butil::Status::OK();
}

void ToolDiff::CompareRegionPb(const std::string& name, const std::string& name2,
                               const std::optional<dingodb::pb::common::Region>& region_1,
                               const std::optional<dingodb::pb::common::Region>& region_2, bool& is_same,
                               std::string& compare_content, std::string& same_content, std::string& diff_content) {
  std::string is_same_str;
  bool is_same_content = true;

  // empty ignore
  if (!region_1 && !region_2) {
    return;
  }

  if (region_1 && !region_2) {
    is_same = false;
    is_same_str = "≠";
    compare_content +=
        fmt::format("\n {} {} {}.Region.region_id L:{} | R:{}", is_same_str, name, name2, region_1->id(), "");
    compare_content +=
        fmt::format("\n {} {} {}.Region.epoch L:{} | R:{}", is_same_str, name, name2, region_1->epoch(), "");
    compare_content += fmt::format("\n {} {} {}.Region.region_type L:{} | R:{}", is_same_str, name, name2,
                                   dingodb::pb::common::RegionType_Name(region_1->region_type()), "");
    compare_content += fmt::format("\n {} {} {}.Region.definition L:{} | R:{}", is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.Region.state L:{} | R:{}", is_same_str, name, name2,
                                   dingodb::pb::common::RegionState_Name(region_1->state()), "");
    compare_content += fmt::format("\n {} {} {}.Region.status L:{} | R:{}", is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.Region.leader_store_id L:{} | R:{}", is_same_str, name, name2,
                                   region_1->leader_store_id(), "");
    compare_content += fmt::format("\n {} {} {}.Region.metrics L:{} | R:{}", is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.Region.create_timestamp L:{} | R:{}", is_same_str, name, name2,
                                   region_1->create_timestamp(), "");
    compare_content += fmt::format("\n {} {} {}.Region.deleted_timestamp L:{} | R:{}", is_same_str, name, name2,
                                   region_1->deleted_timestamp(), "");
    diff_content += std::to_string(region_1->id());

  } else if (!region_1 && region_2) {
    is_same = false;
    is_same_str = "≠";
    compare_content +=
        fmt::format("\n {} {} {}.Region.region_id L:{} | R:{}", is_same_str, name, name2, "", region_2->id());
    compare_content +=
        fmt::format("\n {} {} {}.Region.epoch L:{} | R:{}", is_same_str, name, name2, "", region_2->epoch());
    compare_content += fmt::format("\n {} {} {}.Region.region_type L:{} | R:{}", is_same_str, name, name2, "",
                                   dingodb::pb::common::RegionType_Name(region_2->region_type()));
    compare_content += fmt::format("\n {} {} {}.Region.definition L:{} | R:{}", is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.Region.state L:{} | R:{}", is_same_str, name, name2, "",
                                   dingodb::pb::common::RegionState_Name(region_2->state()));
    compare_content += fmt::format("\n {} {} {}.Region.status L:{} | R:{}", is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.Region.leader_store_id L:{} | R:{}", is_same_str, name, name2, "",
                                   region_2->leader_store_id());
    compare_content += fmt::format("\n {} {} {}.Region.metrics L:{} | R:{}", is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.Region.create_timestamp L:{} | R:{}", is_same_str, name, name2, "",
                                   region_2->create_timestamp());
    compare_content += fmt::format("\n {} {} {}.Region.deleted_timestamp L:{} | R:{}", is_same_str, name, name2, "",
                                   region_2->deleted_timestamp());
    diff_content += std::to_string(region_2->id());
  } else {  // both
    if (region_1->id() != region_2->id()) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.Region.region_id L:{} | R:{}", is_same_str, name, name2, region_1->id(),
                                   region_2->id());

    if (region_1->epoch() != region_2->epoch()) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.Region.epoch L:{} | R:{}", is_same_str, name, name2, region_1->epoch(),
                                   region_2->epoch());

    if (region_1->region_type() != region_2->region_type()) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.Region.region_type L:{} | R:{}", is_same_str, name, name2,
                                   dingodb::pb::common::RegionType_Name(region_1->region_type()),
                                   dingodb::pb::common::RegionType_Name(region_2->region_type()));
    // definition
    ToolDiff::CompareRegionDefinitionPb(name + name2, ".Region.definition", region_1->definition(),
                                        region_2->definition(), is_same, compare_content, same_content, diff_content);

    if (region_1->state() != region_2->state()) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.Region.state L:{} | R:{}", is_same_str, name, name2,
                                   dingodb::pb::common::RegionState_Name(region_1->state()),
                                   dingodb::pb::common::RegionState_Name(region_2->state()));

    bool is_same_internal = google::protobuf::util::MessageDifferencer::Equals(region_1->status(), region_2->status());
    if (!is_same_internal) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.Region.state L:{} | R:{}", is_same_str, name, name2, "...", "...");

    if (region_1->leader_store_id() != region_2->leader_store_id()) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.Region.leader_store_id L:{} | R:{}", is_same_str, name, name2,
                                   region_1->leader_store_id(), region_2->leader_store_id());

    is_same_internal = google::protobuf::util::MessageDifferencer::Equals(region_1->metrics(), region_2->metrics());
    if (!is_same_internal) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.Region.metrics L:{} | R:{}", is_same_str, name, name2, "...", "...");

    if (region_1->create_timestamp() != region_2->create_timestamp()) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.Region.create_timestamp L:{} | R:{}", is_same_str, name, name2,
                                   region_1->create_timestamp(), region_2->create_timestamp());

    if (region_1->deleted_timestamp() != region_2->deleted_timestamp()) {
      is_same = false;
      is_same_str = "≠";
      is_same_content = false;
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.Region.deleted_timestamp L:{} | R:{}", is_same_str, name, name2,
                                   region_1->deleted_timestamp(), region_2->deleted_timestamp());
    if (is_same_content) {
      same_content += std::to_string(region_1->id()) + " ";
    } else {
      diff_content += std::to_string(region_1->id()) + " ";
    }
  }
}

void ToolDiff::CompareRegionDefinitionPb(const std::string& name, const std::string& name2,
                                         const std::optional<dingodb::pb::common::RegionDefinition>& definition_1,
                                         const std::optional<dingodb::pb::common::RegionDefinition>& definition_2,
                                         bool& is_same, std::string& compare_content, std::string& same_content,
                                         std::string& diff_content) {
  std::string is_same_str;

  // empty ignore
  if (!definition_1 && !definition_2) {
    return;
  }

  if (definition_1 && !definition_2) {
    is_same = false;
    is_same_str = "≠";

    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.id L:{} | R:{}", is_same_str, name, name2, definition_1->id(), "");
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.epoch L:{} | R:{}", is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.name L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->name(), "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.name L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->name(), "");
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.range L:{} | R:{}", is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.raw_engine L:{} | R:{}", is_same_str, name, name2,
                                   dingodb::pb::common::RawEngine_Name(definition_1->raw_engine()), "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.store_engine L:{} | R:{}", is_same_str, name, name2,
                                   dingodb::pb::common::StorageEngine_Name(definition_1->store_engine()), "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.schema_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->schema_id(), "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.table_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->table_id(), "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.index_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->index_id(), "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.part_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->part_id(), "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.tenant_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->tenant_id(), "");
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.index_parameter L:{} | R:{}", is_same_str, name, name2, "...", "");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.revision L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->revision(), "");

  } else if (!definition_1 && definition_2) {
    is_same = false;
    is_same_str = "≠";
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.id L:{} | R:{}", is_same_str, name, name2, "", definition_2->id());
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.epoch L:{} | R:{}", is_same_str, name, name2, "", "...");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.name L:{} | R:{}", is_same_str, name, name2, "",
                                   definition_2->name());
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.range L:{} | R:{}", is_same_str, name, name2, "", "...");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.raw_engine L:{} | R:{}", is_same_str, name, name2, "",
                                   dingodb::pb::common::RawEngine_Name(definition_2->raw_engine()));
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.store_engine L:{} | R:{}", is_same_str, name, name2,
                                   "", dingodb::pb::common::StorageEngine_Name(definition_2->store_engine()));
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.schema_id L:{} | R:{}", is_same_str, name, name2, "",
                                   definition_2->schema_id());
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.table_id L:{} | R:{}", is_same_str, name, name2, "",
                                   definition_2->table_id());
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.index_id L:{} | R:{}", is_same_str, name, name2, "",
                                   definition_2->index_id());
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.part_id L:{} | R:{}", is_same_str, name, name2, "",
                                   definition_2->part_id());
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.tenant_id L:{} | R:{}", is_same_str, name, name2, "",
                                   definition_2->tenant_id());
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.index_parameter L:{} | R:{}", is_same_str, name, name2, "", "...");
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.revision L:{} | R:{}", is_same_str, name, name2, "",
                                   definition_2->revision());

  } else {
    if (definition_1->id() != definition_2->id()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->id(), definition_2->id());
    bool is_same_internal =
        google::protobuf::util::MessageDifferencer::Equals(definition_1->epoch(), definition_2->epoch());
    if (!is_same_internal) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.epoch L:{} | R:{}", is_same_str, name, name2, "...", "...");

    if (definition_1->name() != definition_2->name()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.name L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->name(), definition_2->name());

    // ignore compare peers

    is_same_internal = google::protobuf::util::MessageDifferencer::Equals(definition_1->range(), definition_2->range());
    if (!is_same_internal) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.range L:{} | R:{}", is_same_str, name, name2, "...", "...");

    if (definition_1->raw_engine() != definition_2->raw_engine()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.raw_engine L:{} | R:{}", is_same_str, name, name2,
                                   dingodb::pb::common::RawEngine_Name(definition_1->raw_engine()),
                                   dingodb::pb::common::RawEngine_Name(definition_2->raw_engine()));

    if (definition_1->store_engine() != definition_2->store_engine()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.store_engine L:{} | R:{}", is_same_str, name, name2,
                                   dingodb::pb::common::StorageEngine_Name(definition_1->store_engine()),
                                   dingodb::pb::common::StorageEngine_Name(definition_2->store_engine()));

    if (definition_1->schema_id() != definition_2->schema_id()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.schema_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->schema_id(), definition_2->schema_id());

    if (definition_1->table_id() != definition_2->table_id()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.table_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->table_id(), definition_2->table_id());

    if (definition_1->index_id() != definition_2->index_id()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.index_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->index_id(), definition_2->index_id());

    if (definition_1->part_id() != definition_2->part_id()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.part_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->part_id(), definition_2->part_id());

    if (definition_1->tenant_id() != definition_2->tenant_id()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.tenant_id L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->tenant_id(), definition_2->tenant_id());

    is_same_internal = google::protobuf::util::MessageDifferencer::Equals(definition_1->index_parameter(),
                                                                          definition_2->index_parameter());
    if (!is_same_internal) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content +=
        fmt::format("\n {} {} {}.RegionDefinition.index_parameter L:{} | R:{}", is_same_str, name, name2, "...", "...");

    if (definition_1->revision() != definition_2->revision()) {
      is_same = false;
      is_same_str = "≠";
    } else {
      is_same_str = "=";
    }
    compare_content += fmt::format("\n {} {} {}.RegionDefinition.revision L:{} | R:{}", is_same_str, name, name2,
                                   definition_1->revision(), definition_2->revision());
  }
}

butil::Status ToolDiff::ParseNode(const std::string& local_path, const std::string& path, const std::string& param_flag,
                                  std::string& node) {
  butil::Status status;
  std::string dir_internal;
  std::string file_name;
  std::string node_name;

  // split file name
  size_t pos = path.find_last_of('/');
  if (pos == std::string::npos) {
    file_name = path;
    dir_internal = "";
  } else {
    file_name = path.substr(pos + 1);
    dir_internal = path.substr(0, pos);
  }

  pos = dir_internal.find_last_of('/');
  if (pos == std::string::npos) {
    node_name = dir_internal;
  } else {
    node_name = dir_internal.substr(pos + 1);
  }

  // node_name like document-33001; index-31001;store-30001
  if (node_name.empty()) {
    std::string s = fmt::format("file node_name is empty, please check parameter --{}={}", param_flag, local_path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  pos = node_name.find_last_of('-');
  if (pos == std::string::npos) {
    node = node_name;
  } else {
    node = node_name.substr(0, pos);
  }

  // node like document; index;store
  if (node.empty()) {
    std::string s = fmt::format("file node is empty, please check parameter --{}={}", param_flag, local_path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareRegionDataSst(
    const std::string& path1, const std::string& file_name1, const std::string& path2, const std::string& file_name2,
    const std::string& desc,
    std::function<butil::Status(const std::string&, const std::string&, bool&, std::string&, std::string&, std::string&,
                                std::string&, std::string&)>
        handler) {
  butil::Status status;

  bool is_same;
  std::string compare_content;
  std::string content1;
  std::string content2;
  std::string same_content;
  std::string diff_content;

  std::string region_id1;
  std::string region_id2;

  std::string region_cf1;
  std::string region_cf2;

  std::string file_name_summary1;
  std::string file_name_summary2;

  status = ToolDiff::ParseFileName(file_name1, region_id1, region_cf1, file_name_summary1);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ToolDiff::ParseFileName(file_name2, region_id2, region_cf2, file_name_summary2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (region_id1 != region_id2) {
    std::string s = fmt::format("region id is not same. {} {}", region_id1, region_id2);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  if (region_cf1 != region_cf2) {
    std::string s = fmt::format("region cf is not same. {} {}", file_name1, file_name2);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  compare_content += "\n";
  compare_content += file_name_summary1 + "\n";
  compare_content += file_name_summary2;

  status = handler(path1, path2, is_same, compare_content, content1, content2, same_content, diff_content);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path1          ", path1);
  ToolUtils::PrintDumpItem("path2          ", path2);

  ToolUtils::PrintDumpItem("name1          ", file_name1);
  ToolUtils::PrintDumpItem("name2          ", file_name2);

  ToolUtils::PrintDumpItem("desc           ", desc);

  ToolUtils::PrintDumpItem("is_same        ", is_same ? "true" : "false");

  // ToolUtils::PrintDumpItem("same_content   ", (same_content.empty() ? "empty" : same_content));

  // ToolUtils::PrintDumpItem("diff_content   ", (diff_content.empty() ? "empty" : diff_content));

  ToolUtils::PrintDumpItem("compare_content", compare_content);

  ToolUtils::PrintDumpItem("content1       ", content1);

  ToolUtils::PrintDumpItem("content2       ", content2);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDiff::CompareRegionDataFunction(const std::string& path1, const std::string& path2, bool& is_same,
                                                  std::string& compare_content, std::string& content1,
                                                  std::string& content2, std::string& same_content,
                                                  std::string& diff_content) {
  butil::Status status;

  std::map<std::string, std::string> kvs1;

  status = ToolDiff::ReadSstForRegionData(path1, kvs1);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::map<std::string, std::string> kvs2;
  status = ToolDiff::ReadSstForRegionData(path2, kvs2);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  int32_t i = 0;
  int32_t size = kvs1.size();
  int32_t j = 0;
  for (const auto& [key, value] : kvs1) {
    std::string key_hex;
    std::string value_hex;
    std::string key_hex_brief;
    std::string value_hex_brief;

    if (i < 3 || i >= (size - 3)) {
      key_hex = dingodb::Helper::StringToHex(key);
      value_hex = dingodb::Helper::StringToHex(value);

      key_hex_brief = key_hex.size() > 64 ? key_hex.substr(0, 64) + "..." : key_hex;
      value_hex_brief = value_hex.size() > 64 ? value_hex.substr(0, 64) + "..." : value_hex;

      content1 += fmt::format("\n[{}] [{}] : {}", i, key_hex_brief, value_hex_brief);
    } else {
      if (j++ == 0) content1 += fmt::format("\n............");
    }
    i++;
  }

  i = 0;
  size = kvs2.size();
  j = 0;
  for (const auto& [key, value] : kvs2) {
    std::string key_hex;
    std::string value_hex;
    std::string key_hex_brief;
    std::string value_hex_brief;

    if (i < 3 || i >= (size - 3)) {
      key_hex = dingodb::Helper::StringToHex(key);
      value_hex = dingodb::Helper::StringToHex(value);

      key_hex_brief = key_hex.size() > 64 ? key_hex.substr(0, 64) + "..." : key_hex;
      value_hex_brief = value_hex.size() > 64 ? value_hex.substr(0, 64) + "..." : value_hex;

      content2 += fmt::format("\n[{}] [{}] : {}", i, key_hex_brief, value_hex_brief);
    } else {
      if (j++ == 0) content1 += fmt::format("\n............");
    }
    i++;
  }

  // compare
  is_same = true;
  if (kvs1.size() != kvs2.size()) {
    is_same = false;
    compare_content += fmt::format("\n {} total number of key values  L:{} | R:{}", "≠", kvs1.size(), kvs2.size());
    compare_content += "\n";

    return butil::Status::OK();
  }

  for (const auto& [key, value] : kvs1) {
    auto iter = kvs2.find(key);
    if (iter == kvs2.end()) {
      is_same = false;
    } else {
      if (value != iter->second) {
        is_same = false;
      }
    }
  }

  if (is_same) {
    for (const auto& [key, value] : kvs2) {
      auto iter = kvs1.find(key);
      if (iter == kvs1.end()) {
        is_same = false;
      } else {
        if (value != iter->second) {
          is_same = false;
        }
      }
    }
  }

  if (is_same) {
    compare_content += fmt::format("\n {} total number of key values  L:{} | R:{}", "=", kvs1.size(), kvs2.size());
  } else {
    compare_content += fmt::format("\n {} total number of key values  L:{} | R:{}", "≠", kvs1.size(), kvs2.size());
  }

  compare_content += "\n";

  return butil::Status::OK();
}

butil::Status ToolDiff::ParseFileName(const std::string& file_name, std::string& region_id, std::string& region_cf,
                                      std::string& file_name_summary) {
  butil::Status status;

  std::vector<std::string_view> parts1 = ToolUtils::Split(file_name, '.');

  if (parts1.size() != 2) {
    std::string s = fmt::format("file name is invalid. {}", file_name);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  std::vector<std::string_view> parts2 = ToolUtils::Split(parts1[0], '_');
  if (parts2.size() < 5 && parts2.size() > 9) {
    std::string s = fmt::format("file name is invalid. {}", file_name);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  region_id = std::string(parts2[0]);
  std::string_view region_epoch = parts2[1];
  std::string_view region_start_key_hash = parts2[2];
  std::string_view region_time_s = parts2[3];
  region_cf = std::string(parts2[4]);

  for (size_t i = 5; i < parts2.size(); i++) {
    region_cf += "_" + std::string(parts2[i]);
  }

  std::string region_time = dingodb::Helper::FormatTime(std::stoll(std::string(region_time_s)), "%Y-%m-%d %H:%M:%S");

  file_name_summary +=
      fmt::format("region_id: {}, region_epoch: {}, region_start_key_hash: {}, region_time: {}, region_cf: {}. ",
                  std::string(region_id), std::string(region_epoch), std::string(region_start_key_hash),
                  std::string(region_time), region_cf);
  return butil::Status::OK();
}

butil::Status ToolDiff::ReadSstForRegionData(const std::string& path, std::map<std::string, std::string>& kvs) {
  butil::Status status;

  std::shared_ptr<SstFileReader> sst_reader = std::make_shared<SstFileReader>();

  status = sst_reader->ReadFile(path, kvs);
  if (!status.ok()) {
    std::string s = fmt::format("read sst file failed. {}", status.error_str());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  return butil::Status::OK();
}

}  // namespace br