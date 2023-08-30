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

#include <filesystem>
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
#include "server/server.h"
#include "vector/codec.h"

namespace dingodb {

struct SaveRaftSnapshotArg {
  uint64_t region_id;
  braft::SnapshotWriter* writer;
  braft::Closure* done;
  RaftSnapshot* raft_snapshot;
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
  auto raw_range = region->RawRange();
  // Build Iterator
  IteratorOptions options;
  options.upper_bound = raw_range.end_key();

  auto iter = raw_engine->NewIterator(Constant::kStoreDataCF, engine_snapshot_, options);
  iter->Seek(raw_range.start_key());

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
  sst_file.set_start_key(raw_range.start_key());
  sst_file.set_end_key(raw_range.end_key());

  DINGO_LOG(INFO) << "sst file info: " << sst_file.ShortDebugString();
  sst_files.push_back(sst_file);

  return butil::Status();
}

// Filter sst file by range
std::vector<pb::store_internal::SstFileInfo> FilterSstFile(  // NOLINT
    std::vector<pb::store_internal::SstFileInfo>& sst_files, const std::vector<pb::common::Range>& ranges) {
  std::vector<pb::store_internal::SstFileInfo> filter_sst_files;
  for (auto& sst_file : sst_files) {
    if (sst_file.level() == -1) {
      filter_sst_files.push_back(sst_file);
      continue;
    }

    for (const auto& range : ranges) {
      if (sst_file.start_key() < range.end_key() && range.start_key() < sst_file.end_key()) {
        filter_sst_files.push_back(sst_file);
        break;
      }
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
  auto status = checkpoint->Create(checkpoint_path, raw_engine->GetColumnFamily(Constant::kStoreDataCF), tmp_sst_files);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Create checkpoint failed, path: {} error: {} {}",
                                    region->Id(), checkpoint_path, status.error_code(), status.error_str());
    return butil::Status();
  }

  // Get region actual range
  sst_files = FilterSstFile(tmp_sst_files, region->PhysicsRange());
  for (const auto& sst_file : sst_files) {
    DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] sst file info: {}", region->Id(),
                                   sst_file.ShortDebugString());
  }

  return butil::Status();
}

bool RaftSnapshot::SaveSnapshot(braft::SnapshotWriter* writer, store::RegionPtr region,  // NOLINT
                                GenSnapshotFileFunc func) {
  if (region->RawRange().start_key().empty() || region->RawRange().end_key().empty()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Save snapshot failed, range is invalid", region->Id());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] Save snapshot range[{}-{})", region->Id(),
                                 Helper::StringToHex(region->RawRange().start_key()),
                                 Helper::StringToHex(region->RawRange().end_key()));

  std::string region_checkpoint_path =
      fmt::format("{}/{}_{}", Server::GetInstance()->GetCheckpointPath(), region->Id(), Helper::TimestampNs());

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
      // Helper::RemoveAllFileOrDirectory(region_checkpoint_path);
      return false;
    }

    auto filemeta = std::make_unique<braft::LocalFileMeta>();
    filemeta->set_user_meta(sst_file.SerializeAsString());
    filemeta->set_source(braft::FileSource::FILE_SOURCE_LOCAL);
    // fixup
    writer->add_file(filename, static_cast<google::protobuf::Message*>(filemeta.get()));
  }

  // Clean temp checkpoint file
  Helper::RemoveAllFileOrDirectory(region_checkpoint_path);

  return true;
}

// Merge multiple sst file to one sst
static bool MergeCheckpointFile(std::string path, std::string merge_file_path, const pb::common::Range& range) {
  // Already exist merge.sst file, remove it.
  if (std::filesystem::exists(merge_file_path)) {
    Helper::RemoveFileOrDirectory(merge_file_path);
  }

  // Merge multiple file to one sst.
  // Origin checkpoint sst file cant't ingest rocksdb,
  // Just use rocksdb::SstFileWriter generate sst file can ingest rocksdb.
  auto status = RawRocksEngine::MergeCheckpointFile(path, range, merge_file_path);
  if (!status.ok()) {
    // Clean temp file
    if (std::filesystem::exists(merge_file_path)) {
      Helper::RemoveFileOrDirectory(merge_file_path);
    }

    if (status.error_code() == pb::error::ENO_ENTRIES) {
      return true;
    }
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region()] Merge checkpoint file failed, error: {} {}",
                                    pb::error::Errno_Name(status.error_code()), status.error_str());
    return false;
  }

  return true;
}

// Load snapshot by ingest sst files
bool RaftSnapshot::LoadSnapshot(braft::SnapshotReader* reader, store::RegionPtr region) {
  DINGO_LOG(INFO) << fmt::format("[raft.snapshot][region({})] load snapshot...", region->Id());
  std::vector<std::string> files;
  reader->list_files(&files);
  if (files.empty()) {
    DINGO_LOG(WARNING) << fmt::format("[raft.snapshot][region({})] snapshot not include file", region->Id());
  }

  // Delete old region data
  const auto& region_range = region->RawRange();
  auto status = engine_->NewWriter(Constant::kStoreDataCF)->KvBatchDeleteRange(region->PhysicsRange());
  if (!status.ok()) {
    return false;
  }

  bool has_temp_file = false;
  // Ingest sst to region
  if (files.empty()) {
    return true;
  }

  auto raw_engine = std::dynamic_pointer_cast<RawRocksEngine>(engine_);
  std::vector<std::string> sst_files;
  std::string current_path = reader->get_path() + "/" + "CURRENT";
  // The snapshot is generated by use checkpoint.
  if (Helper::IsExistPath(current_path)) {
    int count = 0;
    for (auto& range : region->PhysicsRange()) {
      std::string merge_sst_path = fmt::format("{}/merge_{}.sst", reader->get_path(), count);

      if (MergeCheckpointFile(reader->get_path(), merge_sst_path, range)) {
        sst_files.push_back(merge_sst_path);
      }
    }

  } else {  // The snapshot is generated by use scan.
    for (auto& file : files) {
      std::string filepath = reader->get_path() + "/" + file;
      sst_files.push_back(filepath);
    }
  }

  FAIL_POINT("load_snapshot_suspend");

  if (!sst_files.empty()) {
    auto status = raw_engine->IngestExternalFile(Constant::kStoreDataCF, sst_files);
    for (auto& sst_file : sst_files) {
      if (sst_file.find("merge") != std::string::npos) {
        // Clean merge temp file
        Helper::RemoveFileOrDirectory(sst_files[0]);
      }
    }
  }

  return true;
}

// Use scan async save snapshot.
void AsyncSaveSnapshotByScan(uint64_t region_id, std::shared_ptr<RawEngine> engine, braft::SnapshotWriter* writer,
                             braft::Closure* done) {
  SaveRaftSnapshotArg* arg = new SaveRaftSnapshotArg();
  arg->region_id = region_id;
  arg->writer = writer;
  arg->done = done;
  arg->raft_snapshot = new RaftSnapshot(engine, true);

  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        SaveRaftSnapshotArg* snapshot_arg = static_cast<SaveRaftSnapshotArg*>(arg);
        brpc::ClosureGuard done_guard(snapshot_arg->done);
        auto region =
            Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(snapshot_arg->region_id);
        if (region == nullptr) {
          LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Save snapshot failed, region is null.",
                                    snapshot_arg->region_id);
          if (snapshot_arg->done != nullptr) {
            snapshot_arg->done->status().set_error(pb::error::ERAFT_SAVE_SNAPSHOT, "save snapshot failed");
          }
        } else {
          auto gen_snapshot_file_func =
              std::bind(&RaftSnapshot::GenSnapshotFileByScan, snapshot_arg->raft_snapshot,  // NOLINT
                        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
          if (!snapshot_arg->raft_snapshot->SaveSnapshot(snapshot_arg->writer, region, gen_snapshot_file_func)) {
            LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Save snapshot failed", region->Id());
            if (snapshot_arg->done != nullptr) {
              snapshot_arg->done->status().set_error(pb::error::ERAFT_SAVE_SNAPSHOT, "save snapshot failed");
            }
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
void SaveSnapshotByCheckpoint(uint64_t region_id, std::shared_ptr<RawEngine> engine, braft::SnapshotWriter* writer,
                              braft::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  if (region == nullptr) {
    LOG(ERROR) << fmt::format("[raft.snapshot][region({})] Save snapshot failed, region is null.", region_id);
    if (done != nullptr) {
      done->status().set_error(pb::error::ERAFT_SAVE_SNAPSHOT, "save snapshot failed");
    }
    return;
  }

  auto raft_snapshot = std::make_shared<RaftSnapshot>(engine, false);
  auto gen_snapshot_file_func = std::bind(&RaftSnapshot::GenSnapshotFileByCheckpoint, raft_snapshot,  // NOLINT
                                          std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
  if (!raft_snapshot->SaveSnapshot(writer, region, gen_snapshot_file_func)) {
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

void RaftSaveSnapshotHanler::Handle(uint64_t region_id, std::shared_ptr<RawEngine> engine,
                                    braft::SnapshotWriter* writer, braft::Closure* done) {
  auto config = Server::GetInstance()->GetConfig();
  std::string policy = GetSnapshotPolicy(config);
  if (policy == "checkpoint") {
    SaveSnapshotByCheckpoint(region_id, engine, writer, done);
  } else if (policy == "scan") {
    AsyncSaveSnapshotByScan(region_id, engine, writer, done);
  }
}

void RaftLoadSnapshotHanler::Handle(uint64_t region_id, std::shared_ptr<RawEngine> engine,
                                    braft::SnapshotReader* reader) {
  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] load snapshot failed, region is null.", region_id);
    return;
  }
  auto raft_snapshot = std::make_unique<RaftSnapshot>(engine);
  if (!raft_snapshot->LoadSnapshot(reader, region)) {
    DINGO_LOG(ERROR) << fmt::format("[raft.snapshot][region({})] load snapshot failed.", region->Id());
  }
}

}  // namespace dingodb