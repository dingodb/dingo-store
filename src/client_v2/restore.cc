
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

#include "client_v2/restore.h"

#include <cstdint>
#include <filesystem>
#include <iostream>

#include "client_v2/dump.h"
#include "client_v2/helper.h"
#include "client_v2/meta.h"
#include "client_v2/pretty.h"
#include "client_v2/store.h"
#include "common/helper.h"
#include "common/logging.h"
#include "document/codec.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_reader.h"

namespace client_v2 {

void SetUpRestoreSubCommands(CLI::App &app) {
  // SetUpRestoreRegion(app);
  SetUpRestoreRegionData(app);
  SetUpCheckRestoreRegionData(app);
}

static bool SetUpStore(const std::string &url, const std::vector<std::string> &addrs, int64_t region_id) {
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

static butil::Status QueryRegion(int64_t region_id, dingodb::pb::common::Region &region) {
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  request.set_region_id(region_id);
  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("QueryRegion", request, response);
  if (!status.ok()) {
    std::cout << fmt::format("Error: {} {}", status.error_code(), status.error_str()) << '\n';
    return status;
  }
  region = response.region();
  return status;
}

static butil::Status QuerySstMetas(std::string file_path, int64_t region_id,
                                   dingodb::pb::common::BackupDataFileValueSstMetaGroup &group) {
  rocksdb::Options options;
  options.env = rocksdb::Env::Default();
  if (options.env == nullptr) {
    return butil::Status(1, "Internal restore data sst file error.");
  }

  rocksdb::SstFileReader reader(options);

  if (!std::filesystem::exists(file_path)) {
    return butil::Status(1, "File not exist, please check file path.");
  }

  if (!std::filesystem::is_regular_file(file_path)) {
    return butil::Status(1, fmt::format("file({}) is not regular file", file_path));
  }

  auto status = reader.Open(file_path);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return butil::Status(1, "rocksdb open file failed");
  }

  std::map<std::string, std::string> internal_id_and_sst_meta_group_kvs;

  std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(rocksdb::ReadOptions()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    internal_id_and_sst_meta_group_kvs[std::string(iter->key().data(), iter->key().size())] =
        std::string(iter->value().data(), iter->value().size());
  }
  for (const auto &[internal_id, group_str] : internal_id_and_sst_meta_group_kvs) {
    if (std::stoll(internal_id) != region_id) {
      continue;
    }
    if (!group.ParseFromString(group_str)) {
      std::string s =
          fmt::format("Parse dingodb::pb::common::BackupDataFileValueSstMetaGroup failed : {}", internal_id);
      std::cout << s << std::endl;
      return butil::Status(1, s);
    }
    return butil::Status();
  }
  return butil::Status(1, "not found region in sst file");
}

static butil::Status CompareData(const std::vector<dingodb::pb::common::KeyValue> &db_kvs,
                                 const std::vector<dingodb::pb::common::KeyValue> &sst_kvs) {
  if (db_kvs.size() != sst_kvs.size()) {
    return butil::Status(1, fmt::format("kvs size not equal.{}/{}", db_kvs.size(), sst_kvs.size()));
  }
  for (int i = 0; i < db_kvs.size(); i++) {
    const auto &db_kv = db_kvs.at(i);
    const auto &sst_kv = sst_kvs.at(i);
    if (db_kv.key() != sst_kv.key()) {
      return butil::Status(1, fmt::format("kvs size not equal.{}/{}", db_kvs.size(), sst_kvs.size()));
    }
    if (db_kv.value() != sst_kv.value()) {
      return butil::Status(1, fmt::format("value not equal.{}/{}", db_kv.value(), sst_kv.value()));
    }
  }
  return butil::Status();
}

static butil::Status ReadSstDataAndCompare(CheckRestoreRegionDataOptions const &opt,
                                           const dingodb::pb::common::Region &region, const std::string backend_path,
                                           const dingodb::pb::common::BackupDataFileValueSstMetaGroup &sst_meta_group) {
  rocksdb::Options options;
  options.env = rocksdb::Env::Default();
  if (options.env == nullptr) {
    return butil::Status(1, "Internal restore data sst file error");
  }

  rocksdb::SstFileReader reader(options);
  std::string dir_path;
  std::string file_path;
  rocksdb::Status status;
  butil::Status butil_status;

  for (const auto &sst_meta : sst_meta_group.backup_data_file_value_sst_metas()) {
    dir_path = fmt::format("{}/{}", backend_path, sst_meta.dir_name());
    file_path = fmt::format("{}/{}", dir_path, sst_meta.file_name());

    status = reader.Open(file_path);
    if (BAIDU_UNLIKELY(!status.ok())) {
      return butil::Status(1, "rocksdb open file failed");
    }

    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(rocksdb::ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key(iter->key().data(), iter->key().size());
      kv.set_value(iter->value().data(), iter->value().size());
      kvs.emplace_back(kv);
    }
    std::vector<dingodb::pb::common::KeyValue> db_kvs;
    butil_status = client_v2::DumpRegion(opt, region, sst_meta.cf(), db_kvs);
    if (!butil_status.ok()) {
      return butil_status;
    }
    std::cout << fmt::format("cf:{}, db_kvs_size:{}, sst_kvs_size:{}", sst_meta.cf(), db_kvs.size(), kvs.size())
              << std::endl;
    butil_status = CompareData(db_kvs, kvs);
    if (!butil_status.ok()) {
      return butil_status;
    }
  }
  return butil::Status();
}

void SetUpRestoreRegion(CLI::App &app) {
  auto opt = std::make_shared<RestoreRegionOptions>();
  auto *cmd = app.add_subcommand("RestoreRegion", "Restore region from sst file")->group("Restore Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->region_id, "Restore region id")->required();
  cmd->add_option("--file_path", opt->file_path, "Sst file path")->required();
  cmd->callback([opt]() { RunRestoreRegion(*opt); });
}

void RunRestoreRegion(RestoreRegionOptions const &opt) {
  if (SetUpStore(opt.coor_url, {}, opt.region_id) < 0) {
    exit(-1);
  }
  rocksdb::Options options;
  options.env = rocksdb::Env::Default();
  if (options.env == nullptr) {
    std::cout << "Internal restore data sst file error." << std::endl;
    return;
  }

  rocksdb::SstFileReader reader(options);
  // std::string file_path = "/home/work/data/backup2/store_region_sql_meta.sst";
  if (!std::filesystem::exists(opt.file_path)) {
    std::cout << "File not exist, please check file path." << std::endl;
    return;
  }

  if (!std::filesystem::is_regular_file(opt.file_path)) {
    std::cout << "File is not regular file" << std::endl;
    return;
  }

  auto status = reader.Open(opt.file_path);
  if (BAIDU_UNLIKELY(!status.ok())) {
    std::cout << "Internal open sst file failed." << std::endl;
    return;
  }

  std::map<std::string, std::string> internal_id_and_region_kvs;
  std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(rocksdb::ReadOptions()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    internal_id_and_region_kvs[std::string(iter->key().data(), iter->key().size())] =
        std::string(iter->value().data(), iter->value().size());
  }
  for (const auto &[internal_id, region_str] : internal_id_and_region_kvs) {
    auto region_id = std::stoll(internal_id);
    if (region_id != opt.region_id) {
      continue;
    }

    dingodb::pb::common::Region region;
    if (!region.ParseFromString(region_str)) {
      std::string s = fmt::format("Parse dingodb::pb::common::Region failed : {}", internal_id);
      std::cout << s << std::endl;
      return;
    }
    std::cout << "region_id:" << std::stoll(internal_id) << ", region_info:" << region.DebugString() << std::endl;
    dingodb::pb::coordinator::CreateRegionRequest request;
    dingodb::pb::coordinator::CreateRegionResponse response;

    request.set_region_name(region.definition().name());
    // ignore resource_tag
    request.set_replica_num(0);
    request.mutable_range()->CopyFrom(region.definition().range());
    request.set_raw_engine(region.definition().raw_engine());
    request.set_store_engine(region.definition().store_engine());
    // need to do
    // request.set_region_id(region.id());

    request.set_schema_id(region.definition().schema_id());
    request.set_table_id(region.definition().table_id());
    request.set_index_id(region.definition().index_id());
    request.set_part_id(region.definition().part_id());
    request.set_tenant_id(region.definition().tenant_id());
    // ignore store_ids
    // ignore split_from_region_id
    request.set_region_type(region.region_type());
    // if (region_->definition().has_index_parameter()) {
    //   request.mutable_index_parameter()->CopyFrom(region_->definition().index_parameter());
    // }
    std::cout << "req:" << request.DebugString() << std::endl;
    auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CreateRegion", request,
                                                                                                response);
    if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
      std::cout << "Create executor  failed, error: "
                << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
                << response.error().errmsg();
      return;
    }
    std::cout << "Create region success." << std::endl;
    return;
  }
}

void SetUpRestoreRegionData(CLI::App &app) {
  auto opt = std::make_shared<RestoreRegionDataOptions>();
  auto *cmd = app.add_subcommand("RestoreRegionData", "Restore region data from sst file")->group("Restore Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->region_id, "Restore region id")->required();
  cmd->add_option("--file_path", opt->file_path, "Sst file path")->required();
  cmd->callback([opt]() { RunRestoreRegionData(*opt); });
}

void RunRestoreRegionData(RestoreRegionDataOptions const &opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }
  rocksdb::Options options;
  options.env = rocksdb::Env::Default();
  if (options.env == nullptr) {
    std::cout << "Internal restore data sst file error." << std::endl;
    return;
  }

  rocksdb::SstFileReader reader(options);
  // std::string file_path = "/home/work/data/backup2/store_cf_sst_meta_sql_data.sst";
  dingodb::pb::common::Region region;
  auto status = QueryRegion(opt.region_id, region);
  if (!status.ok()) {
    std::cout << fmt::format("Error: {} {}", status.error_code(), status.error_str()) << '\n';
    return;
  }

  dingodb::pb::common::BackupDataFileValueSstMetaGroup group;
  status = QuerySstMetas(opt.file_path, opt.region_id, group);
  if (!status.ok()) {
    std::cout << fmt::format("Error: {} {}", status.error_code(), status.error_str()) << '\n';
    return;
  }

  dingodb::pb::store::RestoreDataRequest request;
  dingodb::pb::store::RestoreDataResponse response;

  request.mutable_context()->set_region_id(opt.region_id);
  request.set_start_key(region.definition().range().start_key());
  request.set_end_key(region.definition().range().end_key());
  request.set_need_leader(true);
  request.set_region_type(region.region_type());
  auto *storage_backend = request.mutable_storage_backend();
  auto *local = storage_backend->mutable_local();
  local->set_path("/home/work/data/backup2");
  auto *req_group = request.mutable_sst_metas();
  *req_group = group;

  std::cout << "req:" << request.DebugString() << std::endl;
  std::string service_name = GetServiceName(region);

  status = InteractionManager::GetInstance().SendRequestWithContext(service_name, "RestoreData", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Restore region data  failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Restore region data success" << std::endl;

  return;
}

bool QueryRegionFromSst(int64_t region_id, std::string file_path, dingodb::pb::common::Region &region) {
  rocksdb::Options options;
  rocksdb::SstFileReader reader(options);
  // std::string file_path = "/home/work/data/backup2/store_region_sql_meta.sst";
  if (!std::filesystem::exists(file_path)) {
    std::cout << "File not exist, please check file path." << std::endl;
    return false;
  }

  if (!std::filesystem::is_regular_file(file_path)) {
    std::cout << "File is not regular file" << std::endl;
    return false;
  }

  auto status = reader.Open(file_path);
  if (BAIDU_UNLIKELY(!status.ok())) {
    std::cout << "Internal open sst file failed." << std::endl;
    return false;
  }

  std::map<std::string, std::string> internal_id_and_region_kvs;
  std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(rocksdb::ReadOptions()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    internal_id_and_region_kvs[std::string(iter->key().data(), iter->key().size())] =
        std::string(iter->value().data(), iter->value().size());
  }
  for (const auto &[internal_id, region_str] : internal_id_and_region_kvs) {
    if (std::stoll(internal_id) != region_id) {
      continue;
    }

    if (!region.ParseFromString(region_str)) {
      std::string s = fmt::format("Parse dingodb::pb::common::Region failed : {}", internal_id);
      std::cout << s << std::endl;
      return false;
    }
    std::cout << "region_id:" << std::stoll(internal_id) << ", region_info:" << region.DebugString() << std::endl;
    return true;
  }
  return false;
}

void SetUpCheckRestoreRegionData(CLI::App &app) {
  auto opt = std::make_shared<CheckRestoreRegionDataOptions>();
  auto *cmd = app.add_subcommand("CheckRestoreData", "Dump region")->group("Restore Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--region_id", opt->region_id, "region id")->required();
  cmd->add_option("--db_path", opt->db_path, "rocksdb path")->required();
  cmd->add_option("--file_path", opt->file_path, "Sst file path")->required();
  cmd->add_option("--base_dir", opt->base_dir, "base dir back dir")->required();
  cmd->callback([opt]() { RunCheckRestoreRegionData(*opt); });
}

void RunCheckRestoreRegionData(CheckRestoreRegionDataOptions const &opt) {
  if (!SetUpStore(opt.coor_url, {}, opt.region_id)) {
    exit(-1);
  }

  dingodb::pb::common::Region region;
  auto status = QueryRegion(opt.region_id, region);
  if (!status.ok()) {
    std::cout << fmt::format("Error: {} {}", status.error_code(), status.error_str()) << '\n';
    return;
  }

  dingodb::pb::common::BackupDataFileValueSstMetaGroup group;
  status = QuerySstMetas(opt.file_path, opt.region_id, group);
  if (!status.ok()) {
    std::cout << fmt::format("Error: {} {}", status.error_code(), status.error_str()) << '\n';
    return;
  };

  status = ReadSstDataAndCompare(opt, region, opt.base_dir, group);
  if (!status.ok()) {
    std::cout << fmt::format("Error: {} {}", status.error_code(), status.error_str()) << '\n';
    return;
  };
  std::cout << "The data comparison is completely consistent." << std::endl;
}

}  // namespace client_v2