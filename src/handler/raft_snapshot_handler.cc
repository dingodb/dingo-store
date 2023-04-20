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
#include <string>

#include "butil/strings/stringprintf.h"
#include "common/constant.h"
#include "common/helper.h"
#include "google/protobuf/message.h"
#include "server/server.h"

namespace dingodb {

struct SaveRaftSnapshotArg {
  uint64_t region_id;
  braft::SnapshotWriter* writer;
  braft::Closure* done;
  RaftSnapshot* raft_snapshot;
};

// Scan region, generate sst snapshot file
std::vector<pb::store_internal::SstFileInfo> RaftSnapshot::GenSnapshotFileByScan(
    const std::string& checkpoint_dir, std::shared_ptr<pb::store_internal::Region> region) {
  auto raw_engine = std::dynamic_pointer_cast<RawRocksEngine>(engine_);
  auto range = region->definition().range();
  // Build Iterator
  IteratorOptions options;
  options.upper_bound = range.end_key();

  auto iter = raw_engine->NewIterator(Constant::kStoreDataCF, engine_snapshot_, options);
  iter->Seek(range.start_key());

  // Build checkpoing name and path
  std::string checkpoint_name = std::to_string(region->id()) + ".sst";
  const std::string checkpoint_path = checkpoint_dir + "/" + checkpoint_name;

  auto sst_writer = raw_engine->NewSstFileWriter();
  auto status = sst_writer->SaveFile(iter, checkpoint_path);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << butil::StringPrintf("save file failed, path: %s error: %d %s", checkpoint_path.c_str(),
                                            status.error_code(), status.error_cstr());
    return {};
  }

  // Set sst file info
  pb::store_internal::SstFileInfo sst_file;
  sst_file.set_level(0);
  sst_file.set_name(checkpoint_name);
  sst_file.set_path(checkpoint_path);
  sst_file.set_start_key(range.start_key());
  sst_file.set_end_key(range.end_key());

  DINGO_LOG(INFO) << "sst file info: " << sst_file.ShortDebugString();

  return {sst_file};
}

// Filter sst file by range
std::vector<pb::store_internal::SstFileInfo> FilterSstFile(std::vector<pb::store_internal::SstFileInfo> sst_files,
                                                           const std::string& start_key, const std::string& end_key) {
  std::vector<pb::store_internal::SstFileInfo> filter_sst_files;
  for (auto& sst_file : sst_files) {
    DINGO_LOG(INFO) << "sst file info: " << sst_file.ShortDebugString();
    if ((end_key.empty() || sst_file.start_key() < end_key) && start_key < sst_file.end_key()) {
      filter_sst_files.push_back(sst_file);
    }
  }
  return filter_sst_files;
}

// Do Checkpoint and hard link, generate sst snapshot file
std::vector<pb::store_internal::SstFileInfo> RaftSnapshot::GenSnapshotFileByCheckpoint(
    const std::string& checkpoint_dir, std::shared_ptr<dingodb::pb::store_internal::Region> region) {
  auto raw_engine = std::dynamic_pointer_cast<RawRocksEngine>(engine_);

  std::vector<pb::store_internal::SstFileInfo> sst_files;
  auto checkpoint = raw_engine->NewCheckpoint();
  auto status = checkpoint->Create(checkpoint_dir, raw_engine->GetColumnFamily(Constant::kStoreDataCF), sst_files);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << butil::StringPrintf("Create checkpoint failed, path: %s error: %d %s", checkpoint_dir.c_str(),
                                            status.error_code(), status.error_cstr());
    return {};
  }

  return FilterSstFile(sst_files, region->definition().range().start_key(), region->definition().range().end_key());
}

bool RaftSnapshot::SaveSnapshot(braft::SnapshotWriter* writer,
                                std::shared_ptr<dingodb::pb::store_internal::Region> region, GenSnapshotFileFunc func) {
  if (region->definition().range().start_key().empty() || region->definition().range().end_key().empty()) {
    DINGO_LOG(ERROR) << butil::StringPrintf("Save snapshot region %ld failed, range is invalid", region->id());
    return false;
  }

  DINGO_LOG(INFO) << butil::StringPrintf("Save snapshot region %ld range[%s-%s]", region->id(),
                                         Helper::StringToHex(region->definition().range().start_key()).c_str(),
                                         Helper::StringToHex(region->definition().range().end_key()).c_str());
  auto raw_engine = std::dynamic_pointer_cast<RawRocksEngine>(engine_);

  std::filesystem::path db_path(raw_engine->DbPath());
  std::string checkpoint_dir = butil::StringPrintf("%s/checkpoint_%lu_%lu", db_path.parent_path().string().c_str(),
                                                   region->definition().id(), Helper::TimestampMs());
  if (!std::filesystem::create_directories(checkpoint_dir)) {
    DINGO_LOG(ERROR) << "Create directory failed: " << checkpoint_dir;
    return false;
  }

  auto sst_files = func(checkpoint_dir, region);
  if (sst_files.empty()) {
    DINGO_LOG(INFO) << "Generate sanshot file is empty";
    return false;
  }

  for (auto& sst_file : sst_files) {
    std::string snapshot_path = writer->get_path() + "/" + sst_file.name();
    DINGO_LOG(INFO) << "snapshot_path: " << snapshot_path;
    Helper::Link(sst_file.path(), snapshot_path);

    auto filemeta = std::make_unique<braft::LocalFileMeta>();
    filemeta->set_user_meta(sst_file.SerializeAsString());
    filemeta->set_source(braft::FileSource::FILE_SOURCE_LOCAL);
    // fixup
    writer->add_file(sst_file.name(), static_cast<google::protobuf::Message*>(filemeta.get()));
  }

  // Clean temp checkpoint file
  std::filesystem::remove_all(checkpoint_dir);

  return true;
}

bool RaftSnapshot::ValidateSnapshotFile(std::shared_ptr<dingodb::pb::store_internal::Region>& region,
                                        std::unique_ptr<pb::store_internal::SstFileInfo> filemeta) {
  return true;
}

// Load snapshot by ingest sst files
bool RaftSnapshot::LoadSnapshot(braft::SnapshotReader* reader,
                                std::shared_ptr<dingodb::pb::store_internal::Region> region) {
  DINGO_LOG(INFO) << "LoadSnapshot ...";
  auto filepath_func = [](const std::string& path, std::vector<std::string>& files) -> std::vector<std::string> {
    std::vector<std::string> filepaths;

    for (auto& file : files) {
      std::string filepath = path + "/" + file;
      filepaths.push_back(filepath);
    }

    return filepaths;
  };

  std::vector<std::string> files;
  reader->list_files(&files);
  if (files.empty()) {
    DINGO_LOG(ERROR) << "Snapshot not include file";
    return false;
  }

  for (auto& file : files) {
    auto filemeta = std::make_unique<braft::LocalFileMeta>();
    reader->get_file_meta(file, filemeta.get());
    auto sst_file_info = std::make_unique<pb::store_internal::SstFileInfo>();
    sst_file_info->ParseFromString(filemeta->user_meta());

    if (!ValidateSnapshotFile(region, std::move(sst_file_info))) {
      return false;
    }
  }

  // Delete old region data
  auto status = engine_->NewWriter(Constant::kStoreDataCF)->KvDeleteRange(region->definition().range());
  if (!status.ok()) {
    return false;
  }

  // Ingest sst to region
  auto raw_engine = std::dynamic_pointer_cast<RawRocksEngine>(engine_);
  status = raw_engine->IngestExternalFile(Constant::kStoreDataCF, filepath_func(reader->get_path(), files));
  return status.ok();
}

void RaftSaveSnapshotHanler::Handle(uint64_t region_id, std::shared_ptr<RawEngine> engine,
                                    braft::SnapshotWriter* writer, braft::Closure* done) {
  SaveRaftSnapshotArg* arg = new SaveRaftSnapshotArg();
  arg->region_id = region_id;
  arg->writer = writer;
  arg->done = done;
  arg->raft_snapshot = new RaftSnapshot(engine);

  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        SaveRaftSnapshotArg* snapshot_arg = static_cast<SaveRaftSnapshotArg*>(arg);
        brpc::ClosureGuard done_guard(snapshot_arg->done);
        auto region =
            Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(snapshot_arg->region_id);

        auto gen_snapshot_file_func = std::bind(&RaftSnapshot::GenSnapshotFileByScan, snapshot_arg->raft_snapshot,
                                                std::placeholders::_1, std::placeholders::_2);
        if (!snapshot_arg->raft_snapshot->SaveSnapshot(snapshot_arg->writer, region, gen_snapshot_file_func)) {
          DINGO_LOG(ERROR) << "Save snapshot failed, region: " << region->id();
          if (snapshot_arg->done) {
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

void RaftLoadSnapshotHanler::Handle(uint64_t region_id, std::shared_ptr<RawEngine> engine,
                                    braft::SnapshotReader* reader) {
  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  auto raft_snapshot = std::make_unique<RaftSnapshot>(engine);
  if (!raft_snapshot->LoadSnapshot(reader, region)) {
    DINGO_LOG(ERROR) << "Load snapshot failed, region: " << region->id();
  }
}

}  // namespace dingodb