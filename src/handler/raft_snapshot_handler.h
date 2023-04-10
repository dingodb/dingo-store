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

#include "braft/snapshot.h"
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
  RaftSnapshot(std::shared_ptr<RawEngine> engine) : engine_(engine) { engine_snapshot_ = engine->NewSnapshot(); }
  ~RaftSnapshot() = default;

  RaftSnapshot(const RaftSnapshot&) = delete;
  const RaftSnapshot& operator=(const RaftSnapshot&) = delete;

  // Generate snapshot file function
  using GenSnapshotFileFunc = std::function<std::vector<pb::store_internal::SstFileInfo>(
      const std::string checkpoint_dir, std::shared_ptr<dingodb::pb::common::Region> region)>;

  // Scan region, generate sst snapshot file
  std::vector<pb::store_internal::SstFileInfo> GenSnapshotFileByScan(
      const std::string& checkpoint_dir, std::shared_ptr<dingodb::pb::common::Region> region);
  // Do Checkpoint and hard link, generate sst snapshot file
  std::vector<pb::store_internal::SstFileInfo> GenSnapshotFileByCheckpoint(
      const std::string& checkpoint_dir, std::shared_ptr<dingodb::pb::common::Region> region);

  bool SaveSnapshot(braft::SnapshotWriter* writer, std::shared_ptr<dingodb::pb::common::Region> region,
                    GenSnapshotFileFunc func);

  bool LoadSnapshot(braft::SnapshotReader* reader, std::shared_ptr<dingodb::pb::common::Region> region);

 private:
  bool ValidateSnapshotFile(std::shared_ptr<dingodb::pb::common::Region>& region,
                            std::unique_ptr<pb::store_internal::SstFileInfo> filemeta);

  std::shared_ptr<RawEngine> engine_;
  std::shared_ptr<Snapshot> engine_snapshot_;
};

class RaftSaveSnapshotHanler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kSaveSnapshot; }
  void Handle(uint64_t region_id, std::shared_ptr<RawEngine> engine, braft::SnapshotWriter* writer,
              braft::Closure* done) override;
};

class RaftLoadSnapshotHanler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kLoadSnapshot; }
  void Handle(uint64_t region_id, std::shared_ptr<RawEngine> engine, braft::SnapshotReader* reader) override;
};

}  // namespace dingodb

#endif