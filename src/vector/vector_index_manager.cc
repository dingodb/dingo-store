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

#include "vector/vector_index_manager.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "butil/binary_printer.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "config/config_manager.h"
#include "fmt/core.h"
#include "log/segment_log_storage.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/file_service.pb.h"
#include "proto/node.pb.h"
#include "proto/raft.pb.h"
#include "server/file_service.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_index.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_snapshot.h"
#include "vector/vector_index_snapshot_manager.h"

namespace dingodb {

void RebuildVectorIndexTask::Run() {
  if (vector_index_wrapper_->IsStop()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.rebuild][index_id({})] vector index is stop.",
                                   vector_index_wrapper_->Id());
    return;
  }

  if (!force_) {
    if (!vector_index_wrapper_->IsReady()) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.rebuild][index_id({})] vector index is not ready.",
                                     vector_index_wrapper_->Id());
      return;
    }
    if (!vector_index_wrapper_->NeedToRebuild()) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.rebuild][index_id({})] vector index not need rebuild.",
                                     vector_index_wrapper_->Id());
      return;
    }
  }

  auto status = VectorIndexManager::RebuildVectorIndex(vector_index_wrapper_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.rebuild][index_id({}_v{})] rebuild vector index failed, error {}",
                                    vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), status.error_str());
    return;
  }

  status = VectorIndexManager::SaveVectorIndex(vector_index_wrapper_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.save][index_id({}_v{})] save vector index failed, error {}",
                                    vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), status.error_str());
    return;
  }

  if (force_) {
    if (!VectorIndexManager::NeedHoldVectorIndex(vector_index_wrapper_->Id())) {
      vector_index_wrapper_->ClearVectorIndex();
    }

    auto region = Server::GetInstance()->GetRegion(vector_index_wrapper_->Id());
    if (region != nullptr) {
      auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
      store_region_meta->UpdateTemporaryDisableChange(region, false);
    }
  }
}

void SaveVectorIndexTask::Run() {
  if (vector_index_wrapper_->IsStop()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.rebuild][index_id({})] vector index is stop, do not save vector index.",
        vector_index_wrapper_->Id());
    return;
  }
  if (!vector_index_wrapper_->IsReady()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.rebuild][index_id({})] vector index is not ready, do not save vector index.",
        vector_index_wrapper_->Id());
    return;
  }

  auto status = VectorIndexManager::SaveVectorIndex(vector_index_wrapper_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.save][index_id({}_v{})] save vector index failed, error {}",
                                    vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), status.error_str());
    return;
  }
}

bool VectorIndexManager::Init(std::vector<store::RegionPtr> regions) { return true; }  // NOLINT

// Check whether need hold vector index
bool VectorIndexManager::NeedHoldVectorIndex(uint64_t region_id) {
  auto config = Server::GetInstance()->GetConfig();
  if (config == nullptr) {
    return true;
  }

  if (!config->GetBool("vector.enable_follower_hold_index")) {
    // If follower, delete vector index.
    auto raft_store_engine = Server::GetInstance()->GetRaftStoreEngine();
    if (raft_store_engine != nullptr) {
      auto node = raft_store_engine->GetNode(region_id);
      if (node == nullptr) {
        LOG(ERROR) << fmt::format("No found raft node {}.", region_id);
      }

      if (!node->IsLeader()) {
        return false;
      }
    }
  }
  return true;
}

// Load vector index for already exist vector index at bootstrap.
// Priority load from snapshot, if snapshot not exist then load from original data.
butil::Status VectorIndexManager::LoadOrBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper) {
  assert(vector_index_wrapper != nullptr);

  uint64_t start_time = Helper::TimestampMs();
  uint64_t vector_index_id = vector_index_wrapper->Id();

  // try to load vector index from snapshot
  auto new_vector_index = VectorIndexSnapshotManager::LoadVectorIndexSnapshot(vector_index_wrapper);
  if (new_vector_index != nullptr) {
    // replay wal
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.load][index_id({})] Load vector index from snapshot success, will ReplayWal", vector_index_id);
    auto status = ReplayWalToVectorIndex(new_vector_index, new_vector_index->ApplyLogId() + 1, UINT64_MAX);
    if (status.ok()) {
      DINGO_LOG(INFO) << fmt::format(
          "[vector_index.load][index_id({})] ReplayWal success, log_id {} elapsed time({}ms)", vector_index_id,
          new_vector_index->ApplyLogId(), Helper::TimestampMs() - start_time);
      // Switch vector index.
      vector_index_wrapper->UpdateVectorIndex(new_vector_index);

      return status;
    }
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.load][index_id({})] Load vector index from snapshot failed, will build vector_index",
      vector_index_id);

  // Build a new vector_index from original data
  new_vector_index = BuildVectorIndex(vector_index_wrapper);
  if (new_vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format(
        "[vector_index.build][index_id({})] Build vector index failed, elapsed time({}ms).", vector_index_id,
        Helper::TimestampMs() - start_time);

    return butil::Status(pb::error::Errno::EINTERNAL, "Build vector index failed, vector index id %lu",
                         vector_index_id);
  }

  // Switch vector index.
  vector_index_wrapper->UpdateVectorIndex(new_vector_index);

  DINGO_LOG(INFO) << fmt::format("[vector_index.load][index_id({})] Build vector index success, elapsed time({}ms).",
                                 vector_index_id, Helper::TimestampMs() - start_time);

  return butil::Status();
}

butil::Status VectorIndexManager::AsyncLoadOrBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper) {
  assert(vector_index_wrapper != nullptr);

  DINGO_LOG(INFO) << fmt::format("[vector_index.load][index_id({})] async load or build vector index.",
                                 vector_index_wrapper->Id());

  struct Parameter {
    VectorIndexWrapperPtr vector_index_wrapper;
  };

  Parameter* param = new Parameter();
  param->vector_index_wrapper = vector_index_wrapper;

  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        Parameter* param = static_cast<Parameter*>(arg);
        auto vector_index_wrapper = param->vector_index_wrapper;

        LoadOrBuildVectorIndex(vector_index_wrapper);

        vector_index_wrapper->SetNeedBootstrapBuild(false);

        delete param;
        return nullptr;
      },
      param);

  return butil::Status();
}

// Parallel load vector index at server bootstrap.
butil::Status VectorIndexManager::ParallelLoadOrBuildVectorIndex(std::vector<store::RegionPtr> regions,
                                                                 int concurrency) {
  struct Parameter {
    std::vector<store::RegionPtr> regions;
    std::atomic<int> offset;
    std::vector<int> results;
  };

  auto param = std::make_shared<Parameter>();
  param->regions = regions;
  param->offset = 0;
  param->results.resize(regions.size(), 0);

  auto task = [](void* arg) -> void* {
    if (arg == nullptr) {
      return nullptr;
    }
    auto* param = static_cast<Parameter*>(arg);

    for (;;) {
      int offset = param->offset.fetch_add(1, std::memory_order_relaxed);
      if (offset >= param->regions.size()) {
        break;
      }

      auto region = param->regions[offset];
      auto vector_index_wrapper = region->VectorIndexWrapper();

      uint64_t vector_index_id = vector_index_wrapper->Id();
      LOG(INFO) << fmt::format("Init load region {} vector index", vector_index_id);

      auto status = VectorIndexManager::LoadOrBuildVectorIndex(vector_index_wrapper);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("Load region {} vector index failed, ", vector_index_id);
        param->results[offset] = -1;
        break;
      }
    }

    return nullptr;
  };

  if (!Helper::ParallelRunTask(task, param.get(), concurrency)) {
    return butil::Status(pb::error::EINTERNAL, "Create bthread failed.");
  }

  for (auto result : param->results) {
    if (result == -1) {
      return butil::Status(pb::error::EINTERNAL, "Load or build vector index failed.");
    }
  }

  return butil::Status();
}

// Replay vector index from WAL
butil::Status VectorIndexManager::ReplayWalToVectorIndex(VectorIndexPtr vector_index, uint64_t start_log_id,
                                                         uint64_t end_log_id) {
  assert(vector_index != nullptr);
  DINGO_LOG(INFO) << fmt::format("[vector_index.replaywal][index_id({})] replay wal log({}-{})", vector_index->Id(),
                                 start_log_id, end_log_id);

  uint64_t start_time = Helper::TimestampMs();
  auto engine = Server::GetInstance()->GetEngine();
  if (engine->GetID() != pb::common::ENG_RAFT_STORE) {
    return butil::Status(pb::error::Errno::EINTERNAL, "Engine is not raft store.");
  }
  auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(engine);
  auto node = raft_kv_engine->GetNode(vector_index->Id());
  if (node == nullptr) {
    return butil::Status(pb::error::Errno::ERAFT_NOT_FOUND, fmt::format("Not found node {}", vector_index->Id()));
  }

  auto log_stroage = Server::GetInstance()->GetLogStorageManager()->GetLogStorage(vector_index->Id());
  if (log_stroage == nullptr) {
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("Not found log stroage {}", vector_index->Id()));
  }

  uint64_t min_vector_id = VectorCodec::DecodeVectorId(vector_index->Range().start_key());
  uint64_t max_vector_id = VectorCodec::DecodeVectorId(vector_index->Range().end_key());
  max_vector_id = max_vector_id > 0 ? max_vector_id : UINT64_MAX;
  std::vector<pb::common::VectorWithId> vectors;
  vectors.reserve(Constant::kBuildVectorIndexBatchSize);
  std::vector<uint64_t> ids;
  ids.reserve(Constant::kBuildVectorIndexBatchSize);
  uint64_t last_log_id = vector_index->ApplyLogId();
  auto log_entrys = log_stroage->GetEntrys(start_log_id, end_log_id);
  for (const auto& log_entry : log_entrys) {
    auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
    butil::IOBufAsZeroCopyInputStream wrapper(log_entry->data);
    CHECK(raft_cmd->ParseFromZeroCopyStream(&wrapper));
    for (auto& request : *raft_cmd->mutable_requests()) {
      switch (request.cmd_type()) {
        case pb::raft::VECTOR_ADD: {
          if (!ids.empty()) {
            vector_index->Delete(ids);
            ids.clear();
          }

          for (auto& vector : *request.mutable_vector_add()->mutable_vectors()) {
            if (vector.id() >= min_vector_id && vector.id() < max_vector_id) {
              vectors.push_back(vector);
            }
          }

          if (vectors.size() >= Constant::kBuildVectorIndexBatchSize) {
            vector_index->Upsert(vectors);
            vectors.clear();
          }
          break;
        }
        case pb::raft::VECTOR_DELETE: {
          if (!vectors.empty()) {
            vector_index->Upsert(vectors);
            vectors.clear();
          }

          for (auto vector_id : request.vector_delete().ids()) {
            if (vector_id >= min_vector_id && vector_id < max_vector_id) {
              ids.push_back(vector_id);
            }
          }
          if (ids.size() >= Constant::kBuildVectorIndexBatchSize) {
            vector_index->Delete(ids);
            ids.clear();
          }
          break;
        }
        default:
          break;
      }
    }

    last_log_id = log_entry->index;
  }
  if (!vectors.empty()) {
    vector_index->Upsert(vectors);
  } else if (!ids.empty()) {
    vector_index->Delete(ids);
  }

  if (last_log_id > vector_index->ApplyLogId()) {
    vector_index->SetApplyLogId(last_log_id);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.replaywal][index_id({})] replay wal finish, log({}-{}) last_log_id({}) vector_id({}-{}) elapsed "
      "time({}ms)",
      vector_index->Id(), start_log_id, end_log_id, last_log_id, min_vector_id, max_vector_id,
      Helper::TimestampMs() - start_time);

  return butil::Status();
}

// Build vector index with original all data.
VectorIndexPtr VectorIndexManager::BuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper) {
  assert(vector_index_wrapper != nullptr);
  uint64_t vector_index_id = vector_index_wrapper->Id();

  auto region = Server::GetInstance()->GetRegion(vector_index_id);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.build][index_id({})] not found region.", vector_index_id);
    return nullptr;
  }

  auto range = region->RawRange();
  auto vector_index = VectorIndexFactory::New(vector_index_id, vector_index_wrapper->IndexParameter(), range);
  if (!vector_index) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.build][index_id({})] New vector index failed.", vector_index_id);
    return nullptr;
  }

  // Get last applied log id
  auto raft_store_engine = Server::GetInstance()->GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    auto raft_node = raft_store_engine->GetNode(vector_index_id);
    if (raft_node != nullptr) {
      auto raft_status = raft_node->GetStatus();
      if (raft_status->known_applied_index() > 0) {
        vector_index->SetApplyLogId(raft_status->known_applied_index());
      }
    }
  }

  std::string start_key = VectorCodec::FillVectorDataPrefix(range.start_key());
  std::string end_key = VectorCodec::FillVectorDataPrefix(range.end_key());
  DINGO_LOG(INFO) << fmt::format("[vector_index.build][index_id({})] Build vector index, range: [{}({})-{}({}))",
                                 vector_index_id, Helper::StringToHex(start_key),
                                 VectorCodec::DecodeVectorId(start_key), Helper::StringToHex(end_key),
                                 VectorCodec::DecodeVectorId(end_key));

  uint64_t start_time = Helper::TimestampMs();
  // load vector data to vector index
  IteratorOptions options;
  options.upper_bound = end_key;

  auto raw_engine = Server::GetInstance()->GetRawEngine();
  auto iter = raw_engine->NewIterator(Constant::kStoreDataCF, options);
  uint64_t count = 0;
  std::vector<pb::common::VectorWithId> vectors;
  vectors.reserve(Constant::kBuildVectorIndexBatchSize);
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    pb::common::VectorWithId vector;

    std::string key(iter->Key());
    vector.set_id(VectorCodec::DecodeVectorId(key));

    std::string value(iter->Value());
    if (!vector.mutable_vector()->ParseFromString(value)) {
      DINGO_LOG(WARNING) << fmt::format("[vector_index.build][index_id({})] vector with id ParseFromString failed.");
      continue;
    }

    if (vector.vector().float_values_size() <= 0) {
      DINGO_LOG(WARNING) << fmt::format("[vector_index.build][index_id({})] vector values_size error.", vector.id());
      continue;
    }

    ++count;

    vectors.push_back(vector);
    if (count + 1 % Constant::kBuildVectorIndexBatchSize == 0) {
      vector_index->Upsert(vectors);
      vectors.clear();
    }
  }

  if (!vectors.empty()) {
    vector_index->Upsert(vectors);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.build][index_id({})] Build vector index finish, count({}) elapsed time({}ms)", vector_index_id,
      count, Helper::TimestampMs() - start_time);

  return vector_index;
}

void VectorIndexManager::LaunchRebuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, bool force) {
  TaskRunnable* task = new RebuildVectorIndexTask(vector_index_wrapper, force);
  if (!vector_index_wrapper->ExecuteTask(task)) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.rebuild][index_id({})] Launch rebuild vector index failed",
                                    vector_index_wrapper->Id());
  }
}

// Rebuild vector index
butil::Status VectorIndexManager::RebuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper) {
  assert(vector_index_wrapper != nullptr);
  uint64_t vector_index_id = vector_index_wrapper->Id();

  DINGO_LOG(INFO) << fmt::format("[vector_index.rebuild][index_id({}_v{})] Start rebuild vector index.",
                                 vector_index_id, vector_index_wrapper->Version());

  uint64_t start_time = Helper::TimestampMs();
  // Build vector index with original data.
  auto vector_index = BuildVectorIndex(vector_index_wrapper);
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.rebuild][index_id({})] Build vector index failed.",
                                      vector_index_id);
    return butil::Status(pb::error::Errno::EINTERNAL, "Build vector index failed");
  }

  // Check vector index is stop
  if (vector_index_wrapper->IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.rebuild][index_id({})] vector index is stop.", vector_index_id);
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.rebuild][index_id({}_v{})] Build vector index success, log_id {} elapsed time: {}ms",
      vector_index_id, vector_index_wrapper->Version(), vector_index->ApplyLogId(), Helper::TimestampMs() - start_time);

  start_time = Helper::TimestampMs();
  // first ground replay wal
  auto status = ReplayWalToVectorIndex(vector_index, vector_index->ApplyLogId() + 1, UINT64_MAX);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.rebuild][index_id({})] ReplayWal failed first-round, log_id {}",
                                    vector_index_id, vector_index->ApplyLogId());
    return butil::Status(pb::error::Errno::EINTERNAL, "ReplayWal failed first-round");
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.rebuild][index_id({}_v{})] ReplayWal success first-round, log_id {} elapsed time: {}ms",
      vector_index_id, vector_index_wrapper->Version(), vector_index->ApplyLogId(), Helper::TimestampMs() - start_time);

  // Check vector index is stop
  if (vector_index_wrapper->IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.rebuild][index_id({})] vector index is stop.", vector_index_id);
    return butil::Status();
  }

  // switch vector index, stop write vector index.
  vector_index_wrapper->SetIsSwitchingVectorIndex(true);

  {
    ON_SCOPE_EXIT([&]() { vector_index_wrapper->SetIsSwitchingVectorIndex(false); });

    start_time = Helper::TimestampMs();
    // second ground replay wal
    status = ReplayWalToVectorIndex(vector_index, vector_index->ApplyLogId() + 1, UINT64_MAX);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[vector_index.rebuild][index_id({})] ReplayWal failed catch-up round, log_id {}",
                                      vector_index_id, vector_index->ApplyLogId());
      return status;
    }

    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.rebuild][index_id({}_v{})] ReplayWal success catch-up round, log_id {} elapsed time: "
        "{}ms",
        vector_index_id, vector_index_wrapper->Version(), vector_index->ApplyLogId(),
        Helper::TimestampMs() - start_time);

    vector_index_wrapper->UpdateVectorIndex(vector_index);
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.rebuild][index_id({}_v{})] Rebuild vector index success",
                                 vector_index_id, vector_index_wrapper->Version());
  return butil::Status();
}

butil::Status VectorIndexManager::SaveVectorIndex(VectorIndexWrapperPtr vector_index_wrapper) {
  assert(vector_index_wrapper != nullptr);
  uint64_t start_time = Helper::TimestampMs();

  // Check vector index is stop
  if (vector_index_wrapper->IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.save][index_id({})] vector index is stop.",
                                      vector_index_wrapper->Id());
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.save][index_id({}_v{})] Save vector index.", vector_index_wrapper->Id(),
                                 vector_index_wrapper->Version());

  uint64_t snapshot_log_id = 0;
  auto status = VectorIndexSnapshotManager::SaveVectorIndexSnapshot(vector_index_wrapper, snapshot_log_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.save][index_id({})] Save vector index snapshot failed, errno: {}, errstr: {}",
        vector_index_wrapper->Id(), status.error_code(), status.error_str());
    return status;
  } else {
    vector_index_wrapper->SetSnapshotLogId(snapshot_log_id);
  }

  // Update vector index status NORMAL
  DINGO_LOG(INFO) << fmt::format("[vector_index.save][index_id({}_v{})] Save vector index success, elapsed time({}ms)",
                                 vector_index_wrapper->Id(), vector_index_wrapper->Version(),
                                 Helper::TimestampMs() - start_time);

  // Check vector index is stop
  if (vector_index_wrapper->IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.save][index_id({}_v{})] vector index is stop.",
                                      vector_index_wrapper->Id(), vector_index_wrapper->Version());
    return butil::Status();
  }

  return butil::Status();
}

void VectorIndexManager::LaunchSaveVectorIndex(VectorIndexWrapperPtr vector_index_wrapper) {
  assert(vector_index_wrapper != nullptr);

  TaskRunnable* task = new SaveVectorIndexTask(vector_index_wrapper);
  if (!vector_index_wrapper->ExecuteTask(task)) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.rebuild][index_id({})] Launch save vector index failed",
                                    vector_index_wrapper->Id());
  }
}

butil::Status VectorIndexManager::ScrubVectorIndex() {
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  if (store_meta_manager == nullptr) {
    return butil::Status(pb::error::Errno::EINTERNAL, "Get store meta manager failed");
  }

  auto regions = store_meta_manager->GetStoreRegionMeta()->GetAllAliveRegion();
  if (regions.empty()) {
    DINGO_LOG(INFO) << "[vector_index.scrub][index_id()] No alive region, skip scrub vector index";
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << "[vector_index.scrub][index_id()] Scrub vector index start, alive region_count is "
                  << regions.size();

  for (const auto& region : regions) {
    uint64_t vector_index_id = region->Id();
    if (region->State() != pb::common::NORMAL) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] region state is not normal, dont't scrub.",
                                     vector_index_id);
      continue;
    }
    auto vector_index_wrapper = region->VectorIndexWrapper();
    if (!vector_index_wrapper->IsReady()) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] vector index is not ready, dont't scrub.",
                                     vector_index_id);
      continue;
    }
    if (vector_index_wrapper->IsStop()) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] vector index is stop, dont't scrub.",
                                     vector_index_id);
      continue;
    }

    auto last_save_log_behind = vector_index_wrapper->ApplyLogId() - vector_index_wrapper->SnapshotLogId();

    bool need_rebuild = vector_index_wrapper->NeedToRebuild();
    bool need_save = vector_index_wrapper->NeedToSave(last_save_log_behind);
    if (need_rebuild || need_save) {
      auto status = ScrubVectorIndex(region, need_rebuild, need_save);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[vector_index.scrub][index_id({})] scrub vector index failed, error: {}",
                                        vector_index_wrapper->Id(), status.error_str());
        continue;
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexManager::ScrubVectorIndex(store::RegionPtr region, bool need_rebuild, bool need_save) {
  assert(region != nullptr);

  auto vector_index_wrapper = region->VectorIndexWrapper();
  uint64_t vector_index_id = vector_index_wrapper->Id();

  if (need_rebuild) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] need rebuild, do rebuild vector index.",
                                   vector_index_id);
    LaunchRebuildVectorIndex(vector_index_wrapper, false);
  } else if (need_save) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] need save, do save vector index.",
                                   vector_index_id);
    LaunchSaveVectorIndex(vector_index_wrapper);
  }

  return butil::Status::OK();
}

}  // namespace dingodb