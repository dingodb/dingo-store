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

#ifndef DINGODB_RAFT_SNAPSHOT_H_
#define DINGODB_RAFT_SNAPSHOT_H_

#include <cstdint>

#include "braft/snapshot.h"
#include "butil/status.h"
#include "engine/raw_engine.h"
#include "engine/raw_rocks_engine.h"
#include "handler/handler.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

// sst writer
// raft snapshot
// rocksdb injest

class RaftSnapshot {
 public:
  RaftSnapshot(std::shared_ptr<RawEngine> engine, bool need_engine_snapshot = false) : engine_(engine) {
    if (need_engine_snapshot) {
      engine_snapshot_ = engine->NewSnapshot();
    }
  }
  ~RaftSnapshot() = default;

  RaftSnapshot(const RaftSnapshot&) = delete;
  const RaftSnapshot& operator=(const RaftSnapshot&) = delete;

  // Generate snapshot file function
  using GenSnapshotFileFunc =
      std::function<butil::Status(const std::string, store::RegionPtr, std::vector<pb::store_internal::SstFileInfo>&)>;

  // Scan region, generate sst snapshot file
  butil::Status GenSnapshotFileByScan(const std::string& checkpoint_path, store::RegionPtr region,
                                      std::vector<pb::store_internal::SstFileInfo>& sst_files);
  // Do Checkpoint and hard link, generate sst snapshot file
  butil::Status GenSnapshotFileByCheckpoint(const std::string& checkpoint_path, store::RegionPtr region,
                                            std::vector<pb::store_internal::SstFileInfo>& sst_files);

  bool SaveSnapshot(braft::SnapshotWriter* writer, store::RegionPtr region, GenSnapshotFileFunc func,
                    int64_t region_version, int64_t term, int64_t log_index);

  bool LoadSnapshot(braft::SnapshotReader* reader, store::RegionPtr region);
  bool LoadSnapshotDingo(braft::SnapshotReader* reader, store::RegionPtr region);

  butil::Status HandleRaftSnapshotRegionMeta(braft::SnapshotReader* reader, store::RegionPtr region);

 private:
  std::shared_ptr<RawEngine> engine_;
  std::shared_ptr<Snapshot> engine_snapshot_;
};

class RaftSaveSnapshotHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kSaveSnapshot; }
  int Handle(store::RegionPtr region, std::shared_ptr<RawEngine> engine, int64_t term, int64_t log_index,
             braft::SnapshotWriter* writer, braft::Closure* done) override;
};

class RaftLoadSnapshotHanler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kLoadSnapshot; }
  int Handle(store::RegionPtr region, std::shared_ptr<RawEngine> engine, braft::SnapshotReader* reader) override;
};

}  // namespace dingodb

#endif