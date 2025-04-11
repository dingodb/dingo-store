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

#include "br/tool_dump.h"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "br/sst_file_reader.h"
#include "br/tool_utils.h"
#include "br/utils.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "proto/meta.pb.h"

namespace br {

ToolDump::ToolDump(const ToolDumpParams& params) : tool_dump_params_(params) {}

ToolDump::~ToolDump() = default;

std::shared_ptr<ToolDump> ToolDump::GetSelf() { return shared_from_this(); }

butil::Status ToolDump::Init() {
  butil::Status status;

  status =
      ToolUtils::CheckParameterAndHandle(tool_dump_params_.br_dump_file, "br_dump_file", path_internal_, file_name_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  return butil::Status::OK();
}

butil::Status ToolDump::Run() {
  butil::Status status;
  std::string desc;

  // dump direct.  backup.lock;  backupmeta.encryption;  backupmeta.debug;
  if (kBackupFileLock == file_name_ || dingodb::Constant::kBackupMetaDebugName == file_name_ ||
      dingodb::Constant::kBackupMetaEncryptionName == file_name_) {
    if (kBackupFileLock == file_name_) {
      desc =
          "This file is used as a backup file, just to prevent multiple backup programs from backing up to the same "
          "directory";
    }

    if (dingodb::Constant::kBackupMetaDebugName == file_name_) {
      desc = "This file is used to store the backup meta debug information";
    }

    if (dingodb::Constant::kBackupMetaEncryptionName == file_name_) {
      desc = "This file is used to store the backup meta encryption information";
    }

    status = ToolDump::DumpDirect(path_internal_, file_name_, desc, ToolDump::DumpDirectFunction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }
  // dump sst. backupmeta; backupmeta.datafile; backupmeta.schema; coordinator_sdk_meta.sst;
  else if (dingodb::Constant::kBackupMetaName == file_name_ ||
           dingodb::Constant::kBackupMetaDataFileName == file_name_ ||
           dingodb::Constant::kBackupMetaSchemaName == file_name_ ||
           dingodb::Constant::kCoordinatorSdkMetaSstName == file_name_) {
    std::function<butil::Status(const std::string&, std::string&)> handler = nullptr;
    if (dingodb::Constant::kBackupMetaName == file_name_) {
      desc = "This file is used to store the backup meta information";
      handler = ToolDump::DumpBackupMetaFunction;
    }

    if (dingodb::Constant::kBackupMetaDataFileName == file_name_) {
      desc = "This file is used to store the backup meta data information";
      handler = ToolDump::DumpBackupMetaDataFileFunction;
    }

    if (dingodb::Constant::kBackupMetaSchemaName == file_name_) {
      desc = "This file is used to store the backup meta schema information";
      handler = ToolDump::DumpBackupMetaSchemaFunction;
    }

    if (dingodb::Constant::kCoordinatorSdkMetaSstName == file_name_) {
      desc = "This file is used to store the coordinator sdk meta information";
      handler = ToolDump::DumpCoordinatorSdkMetaFunction;
    }

    status = ToolDump::DumpSst(path_internal_, file_name_, desc, handler);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }
  // dump region cf sst.
  // store_cf_sst_meta_sql_meta.sst;
  // store_cf_sst_meta_sdk_data.sst; store_cf_sst_meta_sql_data.sst;
  // index_cf_sst_meta_sdk_data.sst; index_cf_sst_meta_sql_data.sst;
  // document_cf_sst_meta_sdk_data.sst; document_cf_sst_meta_sql_data.sst;
  else if (dingodb::Constant::kStoreCfSstMetaSqlMetaSstName == file_name_ ||
           dingodb::Constant::kStoreCfSstMetaSdkDataSstName == file_name_ ||
           dingodb::Constant::kIndexCfSstMetaSdkDataSstName == file_name_ ||
           dingodb::Constant::kDocumentCfSstMetaSdkDataSstName == file_name_ ||
           dingodb::Constant::kStoreCfSstMetaSqlDataSstName == file_name_ ||
           dingodb::Constant::kIndexCfSstMetaSqlDataSstName == file_name_ ||
           dingodb::Constant::kDocumentCfSstMetaSqlDataSstName == file_name_) {
    std::function<butil::Status(const std::string&, std::string&, std::string&, std::string&)> handler = nullptr;

    if (dingodb::Constant::kStoreCfSstMetaSqlMetaSstName == file_name_) {
      desc = "This file is used to store the store cf sst meta sql meta information";
      handler = ToolDump::DumpRegionCfFunction;
    }

    if (dingodb::Constant::kStoreCfSstMetaSdkDataSstName == file_name_) {
      desc = "This file is used to store the store cf sst meta sdk data information";
      handler = ToolDump::DumpRegionCfFunction;
    }

    if (dingodb::Constant::kIndexCfSstMetaSdkDataSstName == file_name_) {
      desc = "This file is used to store the index cf sst meta sdk data information";
      handler = ToolDump::DumpRegionCfFunction;
    }

    if (dingodb::Constant::kDocumentCfSstMetaSdkDataSstName == file_name_) {
      desc = "This file is used to store the document cf sst meta sdk data information";
      handler = ToolDump::DumpRegionCfFunction;
    }

    if (dingodb::Constant::kStoreCfSstMetaSqlDataSstName == file_name_) {
      desc = "This file is used to store the store cf sst meta sql data information";
      handler = ToolDump::DumpRegionCfFunction;
    }

    if (dingodb::Constant::kIndexCfSstMetaSqlDataSstName == file_name_) {
      desc = "This file is used to store the index cf sst meta sql data information";
      handler = ToolDump::DumpRegionCfFunction;
    }

    if (dingodb::Constant::kDocumentCfSstMetaSqlDataSstName == file_name_) {
      desc = "This file is used to store the document cf sst meta sql data information";
      handler = ToolDump::DumpRegionCfFunction;
    }

    status = ToolDump::DumpRegionCfSst(path_internal_, file_name_, desc, handler);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }
  // dump region defintion sst.
  // store_region_sql_meta.sst;
  // store_region_sdk_data.sst; store_region_sql_data.sst;
  // index_region_sdk_data.sst; index_region_sql_data.sst;
  // document_region_sql_data.sst; document_region_sdk_data.sst;
  else if (dingodb::Constant::kStoreRegionSqlMetaSstName == file_name_ ||
           dingodb::Constant::kStoreRegionSdkDataSstName == file_name_ ||
           dingodb::Constant::kIndexRegionSdkDataSstName == file_name_ ||
           dingodb::Constant::kDocumentRegionSdkDataSstName == file_name_ ||
           dingodb::Constant::kStoreRegionSqlDataSstName == file_name_ ||
           dingodb::Constant::kIndexRegionSqlDataSstName == file_name_ ||
           dingodb::Constant::kDocumentRegionSqlDataSstName == file_name_) {
    std::function<butil::Status(const std::string&, std::string&, std::string&, std::string&)> handler = nullptr;

    if (dingodb::Constant::kStoreRegionSqlMetaSstName == file_name_) {
      desc = "This file is used to store the store region sql meta information";
      handler = ToolDump::DumpRegionDefinitionFunction;
    }

    if (dingodb::Constant::kStoreRegionSdkDataSstName == file_name_) {
      desc = "This file is used to store the store region sdk data information";
      handler = ToolDump::DumpRegionDefinitionFunction;
    }

    if (dingodb::Constant::kIndexRegionSdkDataSstName == file_name_) {
      desc = "This file is used to store the index region sdk data information";
      handler = ToolDump::DumpRegionDefinitionFunction;
    }

    if (dingodb::Constant::kDocumentRegionSdkDataSstName == file_name_) {
      desc = "This file is used to store the document region sdk data information";
      handler = ToolDump::DumpRegionDefinitionFunction;
    }

    if (dingodb::Constant::kStoreRegionSqlDataSstName == file_name_) {
      desc = "This file is used to store the store region sql data information";
      handler = ToolDump::DumpRegionDefinitionFunction;
    }

    if (dingodb::Constant::kIndexRegionSqlDataSstName == file_name_) {
      desc = "This file is used to store the index region sql data information";
      handler = ToolDump::DumpRegionDefinitionFunction;
    }

    if (dingodb::Constant::kDocumentRegionSqlDataSstName == file_name_) {
      desc = "This file is used to store the document region sql data information";
      handler = ToolDump::DumpRegionDefinitionFunction;
    }

    status = ToolDump::DumpRegionDefintionSst(path_internal_, file_name_, desc, handler);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }
  // dump region data sst.
  // like 80049_1-1_d7afb3ab102cfe5e7a8a66d3ec4800efb33ab2fc_1742800412_write.sst;
  else {
    std::function<butil::Status(const std::string&, std::string&, std::string&, std::string&)> handler = nullptr;

    std::string dir_internal;
    std::string file_name;
    std::string node_name;
    std::string node;

    // split file name
    size_t pos = path_internal_.find_last_of('/');
    if (pos == std::string::npos) {
      file_name = path_internal_;
      dir_internal = "";
    } else {
      file_name = path_internal_.substr(pos + 1);
      dir_internal = path_internal_.substr(0, pos);
    }

    pos = dir_internal.find_last_of('/');
    if (pos == std::string::npos) {
      node_name = dir_internal;
    } else {
      node_name = dir_internal.substr(pos + 1);
    }

    // node_name like document-33001; index-31001;store-30001
    if (node_name.empty()) {
      std::string s =
          fmt::format("file node_name is empty, please check parameter --file={}", tool_dump_params_.br_dump_file);
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
      std::string s =
          fmt::format("file node is empty, please check parameter --file={}", tool_dump_params_.br_dump_file);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    if (dingodb::Constant::kStoreRegionName == node) {
      desc = "This file is used to store the store region data information";
      handler = ToolDump::DumpRegionDataFunction;
    }

    if (dingodb::Constant::kIndexRegionName == node) {
      desc = "This file is used to store the index region data information";
      handler = ToolDump::DumpRegionDataFunction;
    }

    if (dingodb::Constant::kDocumentRegionName == node) {
      desc = "This file is used to store the document region data information";
      handler = ToolDump::DumpRegionDataFunction;
    }

    status = ToolDump::DumpRegionDataSst(path_internal_, file_name_, desc, handler);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status ToolDump::Finish() {
  butil::Status status;

  return butil::Status::OK();
}

butil::Status ToolDump::DumpDirect(const std::string& path, const std::string& file_name, const std::string& desc,
                                   std::function<butil::Status(const std::string&, std::string&)> handler) {
  butil::Status status;

  std::ifstream reader(path, std::ifstream::in);

  std::string content;

  status = handler(path, content);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path   ", path);

  ToolUtils::PrintDumpItem("name   ", file_name);

  ToolUtils::PrintDumpItem("desc   ", desc);

  ToolUtils::PrintDumpItem("content", content);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDump::DumpSst(const std::string& path, const std::string& file_name, const std::string& desc,
                                std::function<butil::Status(const std::string&, std::string&)> handler) {
  butil::Status status;

  std::string content;

  status = handler(path, content);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path   ", path);

  ToolUtils::PrintDumpItem("name   ", file_name);

  ToolUtils::PrintDumpItem("desc   ", desc);

  ToolUtils::PrintDumpItem("content", content);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDump::DumpRegionCfSst(
    const std::string& path, const std::string& file_name, const std::string& desc,
    std::function<butil::Status(const std::string&, std::string&, std::string&, std::string&)> handler) {
  butil::Status status;

  std::string content_summary;
  std::string content_brief;
  std::string content_detail;

  status = handler(path, content_summary, content_brief, content_detail);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path           ", path);

  ToolUtils::PrintDumpItem("name           ", file_name);

  ToolUtils::PrintDumpItem("desc           ", desc);

  ToolUtils::PrintDumpItem("content_summary", content_summary);

  ToolUtils::PrintDumpItem("content_brief  ", content_brief);

  ToolUtils::PrintDumpItem("content_detail ", content_detail);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDump::DumpRegionDefintionSst(
    const std::string& path, const std::string& file_name, const std::string& desc,
    std::function<butil::Status(const std::string&, std::string&, std::string&, std::string&)> handler) {
  butil::Status status;

  std::string content_summary;
  std::string content_brief;
  std::string content_detail;

  status = handler(path, content_summary, content_brief, content_detail);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path           ", path);

  ToolUtils::PrintDumpItem("name           ", file_name);

  ToolUtils::PrintDumpItem("desc           ", desc);

  ToolUtils::PrintDumpItem("content_summary", content_summary);

  ToolUtils::PrintDumpItem("content_brief  ", content_brief);

  ToolUtils::PrintDumpItem("content_detail ", content_detail);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDump::DumpRegionDataSst(
    const std::string& path, const std::string& file_name, const std::string& desc,
    std::function<butil::Status(const std::string&, std::string&, std::string&, std::string&)> handler) {
  butil::Status status;

  std::string content_summary;
  std::string content_brief;
  std::string content_detail;

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

  std::string_view region_id = parts2[0];
  std::string_view region_epoch = parts2[1];
  std::string_view region_start_key_hash = parts2[2];
  std::string_view region_time_s = parts2[3];
  std::string region_cf = std::string(parts2[4]);

  for (size_t i = 5; i < parts2.size(); i++) {
    region_cf += "_" + std::string(parts2[i]);
  }

  std::string region_time = dingodb::Helper::FormatTime(std::stoll(std::string(region_time_s)), "%Y-%m-%d %H:%M:%S");

  content_summary +=
      fmt::format("region_id: {}, region_epoch: {}, region_start_key_hash: {}, region_time: {}, region_cf: {}. ",
                  std::string(region_id), std::string(region_epoch), std::string(region_start_key_hash),
                  std::string(region_time), region_cf);

  status = handler(path, content_summary, content_brief, content_detail);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  ToolUtils::PrintDumpHead();

  ToolUtils::PrintDumpItem("path           ", path);

  ToolUtils::PrintDumpItem("name           ", file_name);

  ToolUtils::PrintDumpItem("desc           ", desc);

  ToolUtils::PrintDumpItem("content_summary", content_summary);

  ToolUtils::PrintDumpItem("content_brief  ", content_brief);

  ToolUtils::PrintDumpItem("content_detail ", content_detail);

  ToolUtils::PrintDumpTail();

  return butil::Status::OK();
}

butil::Status ToolDump::DumpDirectFunction(const std::string& path, std::string& content) {
  butil::Status status;

  std::ifstream reader(path, std::ifstream::in);

  std::string line;

  while (std::getline(reader, line)) {
    content += "\n" + line;
  }

  if (reader.is_open()) {
    reader.close();
  }

  return butil::Status::OK();
}

butil::Status ToolDump::DumpBackupMetaFunction(const std::string& path, std::string& content) {
  butil::Status status;

  std::map<std::string, std::string> kvs;

  std::shared_ptr<SstFileReader> sst_reader = std::make_shared<SstFileReader>();

  status = sst_reader->ReadFile(path, kvs);
  if (!status.ok()) {
    std::string s = fmt::format("read sst file failed. {}", status.error_str());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  dingodb::pb::common::BackupParam backup_param;
  bool backup_param_exist = false;
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

  dingodb::pb::common::BackupMeta backup_meta_schema;
  bool backup_meta_schema_exist = false;
  iter = kvs.find(dingodb::Constant::kBackupMetaSchemaName);
  if (iter != kvs.end()) {
    auto ret = backup_meta_schema.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupMeta failed");
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    backup_meta_schema_exist = true;
  }

  dingodb::pb::common::BackupMeta backup_meta_data_file;
  bool backup_meta_data_file_exist = false;
  iter = kvs.find(dingodb::Constant::kBackupMetaDataFileName);
  if (iter != kvs.end()) {
    auto ret = backup_meta_data_file.ParseFromString(iter->second);
    if (!ret) {
      std::string s = fmt::format("parse dingodb::pb::common::BackupMeta failed");
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
    backup_meta_data_file_exist = true;
  }

  // find IdEpochTypeAndValueKey
  dingodb::pb::meta::IdEpochTypeAndValue id_epoch_type_and_value;
  bool id_epoch_type_and_value_exist = false;
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
  dingodb::pb::meta::TableIncrementGroup table_increment_group;
  bool table_increment_group_exist = false;
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

  int32_t i = 0;
  if (backup_param_exist) {
    content +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kBackupBackupParamKey, backup_param.DebugString());
    i++;
  }

  if (backup_meta_schema_exist) {
    content +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kBackupMetaSchemaName, backup_meta_schema.DebugString());
    i++;
  }

  if (backup_meta_data_file_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kBackupMetaDataFileName,
                           backup_meta_data_file.DebugString());
    i++;
  }

  if (id_epoch_type_and_value_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIdEpochTypeAndValueKey,
                           id_epoch_type_and_value.DebugString());
    i++;
  }
  if (table_increment_group_exist) {
    content +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kTableIncrementKey, table_increment_group.DebugString());
    i++;
  }

  return butil::Status::OK();
}

butil::Status ToolDump::DumpBackupMetaDataFileFunction(const std::string& path, std::string& content) {
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
  dingodb::pb::common::BackupMeta store_region_sql_data_sst;
  bool store_region_sql_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta store_cf_sst_meta_sql_data_sst;
  bool store_cf_sst_meta_sql_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta index_region_sql_data_sst;
  bool index_region_sql_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta index_cf_sst_meta_sql_data_sst;
  bool index_cf_sst_meta_sql_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta document_region_sql_data_sst;
  bool document_region_sql_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta document_cf_sst_meta_sql_data_sst;
  bool document_cf_sst_meta_sql_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta store_region_sdk_data_sst;
  bool store_region_sdk_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta store_cf_sst_meta_sdk_data_sst;
  bool store_cf_sst_meta_sdk_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta index_region_sdk_data_sst;
  bool index_region_sdk_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta index_cf_sst_meta_sdk_data_sst;
  bool index_cf_sst_meta_sdk_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta document_region_sdk_data_sst;
  auto document_region_sdk_data_sst_exist = false;
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
  dingodb::pb::common::BackupMeta document_cf_sst_meta_sdk_data_sst;
  bool document_cf_sst_meta_sdk_data_sst_exist = false;
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

  int32_t i = 0;

  if (store_region_sql_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreRegionSqlDataSstName,
                           store_region_sql_data_sst.DebugString());
    i++;
  }

  if (store_cf_sst_meta_sql_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreCfSstMetaSqlDataSstName,
                           store_cf_sst_meta_sql_data_sst.DebugString());
    i++;
  }

  if (index_region_sql_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexRegionSqlDataSstName,
                           index_region_sql_data_sst.DebugString());
    i++;
  }

  if (index_cf_sst_meta_sql_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexCfSstMetaSqlDataSstName,
                           index_cf_sst_meta_sql_data_sst.DebugString());
    i++;
  }

  if (document_region_sql_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentRegionSqlDataSstName,
                           document_region_sql_data_sst.DebugString());
    i++;
  }

  if (document_cf_sst_meta_sql_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentCfSstMetaSqlDataSstName,
                           document_cf_sst_meta_sql_data_sst.DebugString());
    i++;
  }

  if (store_region_sdk_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreRegionSdkDataSstName,
                           store_region_sdk_data_sst.DebugString());
    i++;
  }

  if (store_cf_sst_meta_sdk_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreCfSstMetaSdkDataSstName,
                           store_cf_sst_meta_sdk_data_sst.DebugString());
    i++;
  }

  if (index_region_sdk_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexRegionSdkDataSstName,
                           index_region_sdk_data_sst.DebugString());
    i++;
  }

  if (index_cf_sst_meta_sdk_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kIndexCfSstMetaSdkDataSstName,
                           index_cf_sst_meta_sdk_data_sst.DebugString());
    i++;
  }

  if (document_region_sdk_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentRegionSdkDataSstName,
                           document_region_sdk_data_sst.DebugString());
    i++;
  }

  if (document_cf_sst_meta_sdk_data_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kDocumentCfSstMetaSdkDataSstName,
                           document_cf_sst_meta_sdk_data_sst.DebugString());
    i++;
  }

  return butil::Status::OK();
}

butil::Status ToolDump::DumpBackupMetaSchemaFunction(const std::string& path, std::string& content) {
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
  dingodb::pb::common::BackupMeta store_cf_sst_meta_sql_meta_sst;
  bool store_cf_sst_meta_sql_meta_sst_exist = false;
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
  dingodb::pb::common::BackupMeta store_region_sql_meta_sst;
  bool store_region_sql_meta_sst_exist = false;
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
  dingodb::pb::common::BackupMeta coordinator_sdk_meta_sst;
  bool coordinator_sdk_meta_sst_exist = false;
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

  int32_t i = 0;

  if (store_cf_sst_meta_sql_meta_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreCfSstMetaSqlMetaSstName,
                           store_cf_sst_meta_sql_meta_sst.DebugString());
    i++;
  }

  if (store_region_sql_meta_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kStoreRegionSqlMetaSstName,
                           store_region_sql_meta_sst.DebugString());
    i++;
  }

  if (coordinator_sdk_meta_sst_exist) {
    content += fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kCoordinatorSdkMetaSstName,
                           coordinator_sdk_meta_sst.DebugString());
    i++;
  }

  return butil::Status::OK();
}

butil::Status ToolDump::DumpCoordinatorSdkMetaFunction(const std::string& path, std::string& content) {
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
  dingodb::pb::meta::MetaALL meta_all;
  bool meta_all_exist = false;
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

  int32_t i = 0;

  if (meta_all_exist) {
    content +=
        fmt::format("\n[{}] [{}] :\n{}", i, dingodb::Constant::kCoordinatorSdkMetaKeyName, meta_all.DebugString());
    i++;
  }

  return butil::Status::OK();
}

butil::Status ToolDump::DumpRegionCfFunction(const std::string& path, std::string& content_summary,
                                             std::string& content_brief, std::string& content_detail) {
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
  for (const auto& [region_string, backup_data_file_value_sst_meta_group_string] : kvs) {
    dingodb::pb::common::BackupDataFileValueSstMetaGroup backup_data_file_value_sst_meta_group;
    auto ret = backup_data_file_value_sst_meta_group.ParseFromString(backup_data_file_value_sst_meta_group_string);
    if (!ret) {
      std::string s =
          fmt::format("parse dingodb::pb::common::BackupDataFileValueSstMetaGroup failed : {}", region_string);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    int32_t j = 0;
    std::string internal_backup_data_file_value_sst_metas;
    for (const auto& backup_data_file_value_sst_meta :
         backup_data_file_value_sst_meta_group.backup_data_file_value_sst_metas()) {
      if (0 != j) {
        internal_backup_data_file_value_sst_metas += "-";
      }
      internal_backup_data_file_value_sst_metas += "[" + backup_data_file_value_sst_meta.cf() + "]";
      j++;
    }

    content_brief += fmt::format("\n[{}] [{}]-{}", i, region_string, internal_backup_data_file_value_sst_metas);

    content_detail +=
        fmt::format("\n[{}] [{}] :\n{}", i, region_string, backup_data_file_value_sst_meta_group.DebugString());
    i++;
  }

  content_summary += fmt::format("region cf nums = {}", std::to_string(i));

  return butil::Status::OK();
}

butil::Status ToolDump::DumpRegionDefinitionFunction(const std::string& path, std::string& content_summary,
                                                     std::string& content_brief, std::string& content_detail) {
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

    std::string internal_region;

    internal_region += fmt::format("region_name : {}\n", region.definition().name());
    internal_region += fmt::format("region_type : {}\n", dingodb::pb::common::RegionType_Name(region.region_type()));

    internal_region += fmt::format("region_range : start_key: {}  end_key : {}\n",
                                   dingodb::Helper::StringToHex(region.definition().range().start_key()),
                                   dingodb::Helper::StringToHex(region.definition().range().end_key()));

    internal_region +=
        fmt::format("region_raw_engine : {}\n", dingodb::pb::common::RawEngine_Name(region.definition().raw_engine()));
    internal_region += fmt::format("region_storage_engine : {}\n",
                                   dingodb::pb::common::StorageEngine_Name(region.definition().store_engine()));

    content_brief += fmt::format("\n[{}] [{}] : \n{}", i, region_key_string, internal_region);

    content_detail += fmt::format("\n[{}] [{}] :\n{}", i, region_key_string, region.DebugString());
    i++;
  }

  content_summary += fmt::format("region definition nums = {}", std::to_string(i));

  return butil::Status::OK();
}

butil::Status ToolDump::DumpRegionDataFunction(const std::string& path, std::string& content_summary,
                                               std::string& content_brief, std::string& /*content_detail*/) {
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
  int32_t size = kvs.size();
  int32_t j = 0;
  for (const auto& [key, value] : kvs) {
    std::string key_hex;
    std::string value_hex;
    std::string key_hex_brief;
    std::string value_hex_brief;

    if (i < 3 || i >= (size - 3)) {
      key_hex = dingodb::Helper::StringToHex(key);
      value_hex = dingodb::Helper::StringToHex(value);

      key_hex_brief = key_hex.size() > 64 ? key_hex.substr(0, 64) + "..." : key_hex;
      value_hex_brief = value_hex.size() > 64 ? value_hex.substr(0, 64) + "..." : value_hex;

      content_brief += fmt::format("\n[{}] [{}] : {}", i, key_hex_brief, value_hex_brief);
    } else {
      if (j++ == 0) content_brief += fmt::format("\n............");
    }

    // content_detail += fmt::format("\n[{}] [{}] : {}", i, key_hex, value_hex);
    i++;
  }

  content_summary += fmt::format("region data key_value pairs = {}", std::to_string(i));

  return butil::Status::OK();
}

}  // namespace br