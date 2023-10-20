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

#include "handler/raft_snapshot_handler.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/failpoint.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "google/protobuf/message.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store_internal.pb.h"
#include "server/server.h"
#include "store/region_controller.h"
#include "vector/codec.h"

namespace dingodb {

struct SaveRaftSnapshotArg {
  store::RegionPtr region;
  braft::SnapshotWriter* writer;
  braft::Closure* done;
  RaftSnapshot* raft_snapshot;
  int64_t region_version;
  int64_t term;
  int64_t log_index;
};

// Scan region, generate sst snapshot file
butil::Status RaftSnapshot::GenSnapshotFileByScan(const std::string& checkpoint_path, store::RegionPtr region,
                                                  std::vector<pb::store_internal::SstFileInfo>& sst_files) {
  if (!std::filesystem::create_directories(checkpoint_path)) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Create directory failed, path: {} ", region->Id(),
                                    checkpoint_path);
    return butil::Status(pb::error::EINTERNAL, "Create directory failed");
  }
  auto raw_engine = std::dynamic_pointer_cast<RawRocksEngine>(engine_);
  auto range = region->Range();
  // Build Iterator
  IteratorOptions options;
  options.upper_bound = range.end_key();

  auto iter = raw_engine->NewIterator(Constant::kStoreDataCF, engine_snapshot_, options);
  iter->Seek(range.start_key());

  // Build sst name and path
  std::string sst_name = std::to_string(region->Id()) + ".sst";
  const std::string sst_path = checkpoint_path + "/" + sst_name;

  auto status = RawRocksEngine::NewSstFileWriter()->SaveFile(iter, sst_path);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENO_ENTRIES) {
      DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] save file failed, path: {} error: {} {}",
                                      region->Id(), sst_path, status.error_code(), status.error_str());
    }
    return status;
  }

  // Set sst file info
  pb::store_internal::SstFileInfo sst_file;
  sst_file.set_level(0);
  sst_file.set_name(sst_name);
  sst_file.set_path(sst_path);
  sst_file.set_start_key(range.start_key());
  sst_file.set_end_key(range.end_key());

  DINGO_LOG(INFO) << "sst file info: " << sst_file.ShortDebugString();
  sst_files.push_back(sst_file);

  return butil::Status();
}

// Filter sst file by range
std::vector<pb::store_internal::SstFileInfo> FilterSstFile(  // NOLINT
    std::vector<pb::store_internal::SstFileInfo>& sst_files, const pb::common::Range& range) {
  std::vector<pb::store_internal::SstFileInfo> filter_sst_files;
  for (auto& sst_file : sst_files) {
    if (sst_file.level() == -1) {
      DINGO_LOG(INFO) << fmt::format("[raft.snapshot] sst file level is -1, add sst file info: {}",
                                     sst_file.ShortDebugString());
      filter_sst_files.push_back(sst_file);
      continue;
    }

    if (sst_file.start_key() < range.end_key() && range.start_key() < sst_file.end_key()) {
      DINGO_LOG(INFO) << fmt::format("[raft.snapshot] add sst file info: {}", sst_file.ShortDebugString());
      filter_sst_files.push_back(sst_file);
    }
  }

  return filter_sst_files;
}

// Do Checkpoint and hard link, generate sst snapshot file
butil::Status RaftSnapshot::GenSnapshotFileByCheckpoint(const std::string& checkpoint_path, store::RegionPtr region,
                                                        std::vector<pb::store_internal::SstFileInfo>& sst_files) {
  auto raw_engine = std::dynamic_pointer_cast<RawRocksEngine>(engine_);

  std::vector<pb::store_internal::SstFileInfo> tmp_sst_files;
  auto checkpoint = raw_engine->NewCheckpoint();
  std::vector<std::shared_ptr<RawRocksEngine::ColumnFamily>> column_families;
  auto cf_names = Helper::GetColumnFamilyNames();
  column_families.reserve(cf_names.size());
  for (const auto& cf_name : cf_names) {
    column_families.push_back(raw_engine->GetColumnFamily(cf_name));
  }

  auto status = checkpoint->Create(checkpoint_path, column_families, tmp_sst_files);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Create checkpoint failed, path: {} error: {} {}",
                                    region->Id(), checkpoint_path, status.error_code(), status.error_str());
    return butil::Status();
  }

  // Get region actual range
  sst_files = FilterSstFile(tmp_sst_files, region->Range());
  for (const auto& sst_file : sst_files) {
    DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] sst file info: {}", region->Id(),
                                   sst_file.ShortDebugString());
  }

  return butil::Status();
}

// Add region meta to snapshot
bool AddRegionMetaFile(braft::SnapshotWriter* writer, store::RegionPtr region, int64_t term, int64_t log_index) {
  std::string filepath = writer->get_path() + "/" + Constant::kRaftSnapshotRegionMetaFileName;
  std::ofstream file(filepath);
  if (!file.is_open()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Open file {} failed", region->Id(), filepath);
    return false;
  }

  pb::store_internal::RaftSnapshotRegionMeta meta;
  *(meta.mutable_epoch()) = region->Epoch();
  *(meta.mutable_range()) = region->Range();
  meta.set_term(term);
  meta.set_log_index(log_index);

  DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] region meta epoch({}_{}) range[{}-{})", region->Id(),
                                 meta.epoch().conf_version(), meta.epoch().version(),
                                 Helper::StringToHex(region->Range().start_key()),
                                 Helper::StringToHex(region->Range().end_key()));

  file << meta.SerializeAsString();
  file.close();
  writer->add_file(Constant::kRaftSnapshotRegionMetaFileName);

  return true;
}

bool RaftSnapshot::SaveSnapshot(braft::SnapshotWriter* writer, store::RegionPtr region,  // NOLINT
                                GenSnapshotFileFunc func, int64_t region_version, int64_t term, int64_t log_index) {
  if (region->Range().start_key().empty() || region->Range().end_key().empty()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Save snapshot failed, range is invalid", region->Id());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] Save snapshot range[{}-{})", region->Id(),
                                 Helper::StringToHex(region->Range().start_key()),
                                 Helper::StringToHex(region->Range().end_key()));

  // Add region meta to snapshot
  if (!AddRegionMetaFile(writer, region, term, log_index)) {
    return false;
  }

  std::string region_checkpoint_path =
      fmt::format("{}/{}_{}", Server::GetInstance().GetCheckpointPath(), region->Id(), Helper::TimestampNs());

  std::vector<pb::store_internal::SstFileInfo> sst_files;
  auto status = func(region_checkpoint_path, region, sst_files);
  if (!status.ok() && status.error_code() != pb::error::ENO_ENTRIES) {
    // Clean temp checkpoint file
    Helper::RemoveAllFileOrDirectory(region_checkpoint_path);
    return false;
  }

  for (auto& sst_file : sst_files) {
    std::string filename = Helper::CleanFirstSlash(sst_file.name());
    std::string snapshot_path = writer->get_path() + "/" + filename;
    DINGO_LOG(DEBUG) << fmt::format("snapshot_path: {} to {}", sst_file.path(), snapshot_path);
    if (!Helper::Link(sst_file.path(), snapshot_path) || !Helper::IsExistPath(snapshot_path)) {
      DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] link file failed, path: {}", region->Id(),
                                      snapshot_path);
      // Clean temp checkpoint file
      Helper::RemoveAllFileOrDirectory(region_checkpoint_path);
      return false;
    }

    auto filemeta = std::make_unique<braft::LocalFileMeta>();
    filemeta->set_user_meta(sst_file.SerializeAsString());
    filemeta->set_source(braft::FileSource::FILE_SOURCE_LOCAL);
    writer->add_file(filename, static_cast<google::protobuf::Message*>(filemeta.get()));
  }

  // Clean temp checkpoint file
  Helper::RemoveAllFileOrDirectory(region_checkpoint_path);

  // update snapshot epoch to store meta
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();
  if (!store_region_meta) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] get store region meta failed", region->Id());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] update snapshot_epoch_version, from: {} to: {}",
                                 region->Id(), region->SnapshotEpochVersion(), region_version);
  store_region_meta->UpdateSnapshotEpochVersion(region, region_version);

  return true;
}

// Check snapshot region meta, especially region version.
butil::Status RaftSnapshot::HandleRaftSnapshotRegionMeta(braft::SnapshotReader* reader, store::RegionPtr region) {
  pb::store_internal::RaftSnapshotRegionMeta meta;
  auto status = Helper::ParseRaftSnapshotRegionMeta(reader->get_path(), meta);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] parse business snapshot meta failed, error: {}",
                                    region->Id(), status.error_str());
    return status;
  }

  DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] current region version({}) snapshot region version({})",
                                 region->Id(), region->Epoch().version(), meta.epoch().version());

  if (meta.epoch().version() < region->Epoch().version()) {
    DINGO_LOG(WARNING) << fmt::format("[raft.snapshot][region({})] snapshot version abnormal, abandon load snapshot",
                                      region->Id());
    return butil::Status(pb::error::EREGION_VERSION, "snapshot version abnormal, abandon load snapshot");

  } else if (meta.epoch().version() > region->Epoch().version()) {
    auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();
    store_region_meta->UpdateEpochVersionAndRange(region, meta.epoch().version(), meta.range());
  }

  // Delete old region data
  auto cf_names = Helper::GetColumnFamilyNames();
  auto writer = engine_->NewMultiCfWriter(cf_names);

  std::map<uint32_t, std::vector<pb::common::Range>> ranges_with_cf;
  for (const auto& cf_name : cf_names) {
    if (kTxnCf2Id.count(cf_name) > 0) {
      ranges_with_cf.insert_or_assign(kTxnCf2Id.at(cf_name), std::vector<pb::common::Range>{region->Range()});
    } else {
      DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] invalid cf name: {}", region->Id(), cf_name);
      return butil::Status(pb::error::EINTERNAL, fmt::format("invalid cf name: {}", cf_name));
    }
  }

  status = writer->KvBatchDeleteRange(ranges_with_cf);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] delete old region data failed, error: {}",
                                    region->Id(), status.error_str());
    return status;
  }

  return butil::Status();
}

// Load snapshot by ingest sst files
bool RaftSnapshot::LoadSnapshot(braft::SnapshotReader* reader, store::RegionPtr region) {
  DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] load snapshot...", region->Id());
  std::vector<std::string> files;
  reader->list_files(&files);
  if (files.empty()) {
    DINGO_LOG(WARNING) << fmt::format("[raft.snapshot][region({})] snapshot not include file", region->Id());
  }

  auto status = HandleRaftSnapshotRegionMeta(reader, region);
  if (!status.ok()) {
    if (status.error_code() == pb::error::EREGION_VERSION) {
      return true;
    }
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] handle region meta failed, error: {}", region->Id(),
                                    status.error_str());
    return false;
  }

  // Ingest sst to region
  if (files.empty()) {
    DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] snapshot not include file", region->Id());
    return true;
  }

  for (const auto& file : files) {
    DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] snapshot file: {}", region->Id(), file);
  }

  auto raw_engine = std::dynamic_pointer_cast<RawRocksEngine>(engine_);
  std::vector<std::string> sst_files;
  std::string current_path = reader->get_path() + "/" + "CURRENT";

  auto cf_names = Helper::GetColumnFamilyNames();

  // The snapshot is generated by use checkpoint.
  if (Helper::IsExistPath(current_path)) {
    int count = 0;

    std::vector<std::string> merge_sst_file_paths;

    for (const auto& cf_name : cf_names) {
      std::string merge_sst_path = fmt::format("{}/merge_{}.sst", reader->get_path(), cf_name);
      merge_sst_file_paths.push_back(merge_sst_path);
    }

    DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] merge sst file paths: {}", region->Id(),
                                   merge_sst_file_paths.size());

    auto status =
        RawRocksEngine::MergeCheckpointFiles(reader->get_path(), region->Range(), cf_names, merge_sst_file_paths);
    if (!status.ok()) {
      // Clean temp file
      for (const auto& merge_file_path : merge_sst_file_paths) {
        if (std::filesystem::exists(merge_file_path)) {
          Helper::RemoveFileOrDirectory(merge_file_path);
        }
      }

      DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] merge checkpoint file failed, error: {} {}",
                                      region->Id(), status.error_code(), status.error_str())
                       << ", path: " << reader->get_path();

      return false;
    }

    for (const auto& merge_file_path : merge_sst_file_paths) {
      sst_files.push_back(merge_file_path);
    }
  } else {  // The snapshot is generated by use scan.
    DINGO_LOG(ERROR) << fmt::format(
        "[raft.snapshot][region({})] snapshot not include CURRENT file, snapshot by scan is not support now",
        region->Id());
    return false;

    for (auto& file : files) {
      if (file == Constant::kRaftSnapshotRegionMetaFileName) {
        continue;
      }
      std::string filepath = reader->get_path() + "/" + file;
      sst_files.push_back(filepath);
    }
  }

  FAIL_POINT("load_snapshot_suspend");

  for (int i = 0; i < cf_names.size(); i++) {
    const auto& cf_name = cf_names[i];
    const auto& sst_path = sst_files[i];

    if (sst_path.empty()) {
      DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] sst file is empty, skip ingest, cf_name: {}",
                                     region->Id(), cf_name);
      continue;
    }

    std::vector<std::string> sst_files_to_ingest{sst_path};

    auto status = raw_engine->IngestExternalFile(cf_name, sst_files_to_ingest);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] ingest sst file failed, error: {} {}", region->Id(),
                                      status.error_code(), status.error_str())
                       << ", sst file: " << sst_path;
      return false;
    }

    DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] successfully ingest sst file: {}", region->Id(),
                                   sst_path);
  }

  for (const auto& sst_file : sst_files) {
    // Clean merge temp file
    if (sst_file.empty()) {
      continue;
    }
    Helper::RemoveFileOrDirectory(sst_file);
  }

  DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] load snapshot success", region->Id());

  return true;
}

// Use scan async save snapshot.
void AsyncSaveSnapshotByScan(store::RegionPtr region, std::shared_ptr<RawEngine> engine, int64_t term,
                             int64_t log_index, braft::SnapshotWriter* writer, braft::Closure* done) {
  SaveRaftSnapshotArg* arg = new SaveRaftSnapshotArg();
  arg->region = region;
  arg->writer = writer;
  arg->done = done;
  arg->raft_snapshot = new RaftSnapshot(engine, true);
  arg->region_version = region->Epoch().version();
  arg->term = term;
  arg->log_index = log_index;

  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        SaveRaftSnapshotArg* snapshot_arg = static_cast<SaveRaftSnapshotArg*>(arg);
        brpc::ClosureGuard done_guard(snapshot_arg->done);
        auto region = snapshot_arg->region;

        auto gen_snapshot_file_func =
            std::bind(&RaftSnapshot::GenSnapshotFileByScan, snapshot_arg->raft_snapshot,  // NOLINT
                      std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        if (!snapshot_arg->raft_snapshot->SaveSnapshot(snapshot_arg->writer, region, gen_snapshot_file_func,
                                                       snapshot_arg->region_version, snapshot_arg->term,
                                                       snapshot_arg->log_index)) {
          LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Save snapshot failed", region->Id());
          if (snapshot_arg->done != nullptr) {
            snapshot_arg->done->status().set_error(pb::error::ERAFT_SAVE_SNAPSHOT, "save snapshot failed");
          }
        }

        delete snapshot_arg->raft_snapshot;
        snapshot_arg->raft_snapshot = nullptr;
        delete snapshot_arg;
        snapshot_arg = nullptr;
        return nullptr;
      },
      arg);
}

// Use checkpoint save snapshot
void SaveSnapshotByCheckpoint(store::RegionPtr region, std::shared_ptr<RawEngine> engine, int64_t term,
                              int64_t log_index, braft::SnapshotWriter* writer, braft::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto raft_snapshot = std::make_shared<RaftSnapshot>(engine, false);
  auto gen_snapshot_file_func = std::bind(&RaftSnapshot::GenSnapshotFileByCheckpoint, raft_snapshot,  // NOLINT
                                          std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
  if (!raft_snapshot->SaveSnapshot(writer, region, gen_snapshot_file_func, region->Epoch().version(), term,
                                   log_index)) {
    LOG(ERROR) << fmt::format("[raft.snapshot][region({})] save snapshot failed.", region->Id());
    if (done != nullptr) {
      done->status().set_error(pb::error::ERAFT_SAVE_SNAPSHOT, "save snapshot failed");
    }
  }
}

std::string GetSnapshotPolicy(std::shared_ptr<dingodb::Config> config) {
  std::string policy = config->GetString("raft.snapshot_policy");
  return policy = policy.empty() ? Constant::kDefaultRaftSnapshotPolicy : policy;
}

int RaftSaveSnapshotHandler::Handle(store::RegionPtr region, std::shared_ptr<RawEngine> engine, int64_t term,
                                    int64_t log_index, braft::SnapshotWriter* writer, braft::Closure* done) {
  auto config = ConfigManager::GetInstance().GetConfig();
  std::string policy = GetSnapshotPolicy(config);
  if (policy == "checkpoint") {
    SaveSnapshotByCheckpoint(region, engine, term, log_index, writer, done);
  } else if (policy == "scan") {
    AsyncSaveSnapshotByScan(region, engine, term, log_index, writer, done);
  }

  return 0;
}

int RaftLoadSnapshotHanler::Handle(store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                                   braft::SnapshotReader* reader) {
  auto raft_snapshot = std::make_unique<RaftSnapshot>(engine);
  if (!raft_snapshot->LoadSnapshot(reader, region)) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] load snapshot failed.", region->Id());
    return -1;
  }

  return 0;
}

}  // namespace dingodb