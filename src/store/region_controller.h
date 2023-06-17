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

#ifndef DINGODB_STORE_REGION_CONTROL_H_
#define DINGODB_STORE_REGION_CONTROL_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "bthread/execution_queue.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/store_meta_manager.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"

namespace dingodb {

class TaskRunnable {
 public:
  TaskRunnable() = default;
  virtual ~TaskRunnable() = default;

  virtual void Run() = 0;
};

class CreateRegionTask : public TaskRunnable {
 public:
  CreateRegionTask(std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~CreateRegionTask() override = default;

  void Run() override;

  static butil::Status PreValidateCreateRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateCreateRegion(std::shared_ptr<StoreMetaManager> store_meta_manager, uint64_t region_id);
  static butil::Status CreateRegion(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                    uint64_t split_from_region_id);

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::coordinator::RegionCmd> region_cmd_;
};

class DeleteRegionTask : public TaskRunnable {
 public:
  DeleteRegionTask(std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~DeleteRegionTask() override = default;

  void Run() override;

  static butil::Status PreValidateDeleteRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateDeleteRegion(std::shared_ptr<StoreMetaManager> /*store_meta_manager*/,
                                            store::RegionPtr region);
  static butil::Status DeleteRegion(std::shared_ptr<Context> ctx, uint64_t region_id);

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::coordinator::RegionCmd> region_cmd_;
};

class SplitRegionTask : public TaskRunnable {
 public:
  SplitRegionTask(std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~SplitRegionTask() override = default;

  void Run() override;

  static butil::Status PreValidateSplitRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateSplitRegion(std::shared_ptr<StoreRegionMeta> store_region_meta,
                                           const pb::coordinator::SplitRequest& split_request);
  butil::Status SplitRegion();

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::coordinator::RegionCmd> region_cmd_;
};

class ChangeRegionTask : public TaskRunnable {
 public:
  ChangeRegionTask(std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~ChangeRegionTask() override = default;

  void Run() override;

  static butil::Status PreValidateChangeRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateChangeRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                            const pb::common::RegionDefinition& region_definition);

  static butil::Status ChangeRegion(std::shared_ptr<Context> ctx,
                                    const pb::common::RegionDefinition& region_definition);

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::coordinator::RegionCmd> region_cmd_;
};

class TransferLeaderTask : public TaskRunnable {
 public:
  TransferLeaderTask(std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~TransferLeaderTask() override = default;

  void Run() override;

  static butil::Status PreValidateTransferLeader(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateTransferLeader(std::shared_ptr<StoreMetaManager> store_meta_manager, uint64_t region_id,
                                              const pb::common::Peer& peer);
  static butil::Status TransferLeader(std::shared_ptr<Context> ctx, uint64_t region_id, const pb::common::Peer& peer);

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::coordinator::RegionCmd> region_cmd_;
};

class SnapshotRegionTask : public TaskRunnable {
 public:
  SnapshotRegionTask(std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~SnapshotRegionTask() override = default;

  void Run() override;

 private:
  static butil::Status Snapshot(std::shared_ptr<Context> ctx, uint64_t region_id);

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::coordinator::RegionCmd> region_cmd_;
};

// Clean region zombie, free region meta resource.
class PurgeRegionTask : public TaskRunnable {
 public:
  PurgeRegionTask(std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~PurgeRegionTask() override = default;

  void Run() override;

  static butil::Status PreValidatePurgeRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidatePurgeRegion(store::RegionPtr region);
  static butil::Status PurgeRegion(std::shared_ptr<Context> ctx, uint64_t region_id);

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::coordinator::RegionCmd> region_cmd_;
};

// Stop raft peer
class StopRegionTask : public TaskRunnable {
 public:
  StopRegionTask(std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~StopRegionTask() override = default;

  void Run() override;

  static butil::Status PreValidateStopRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateStopRegion(store::RegionPtr region);
  static butil::Status StopRegion(std::shared_ptr<Context> ctx, uint64_t region_id);

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::coordinator::RegionCmd> region_cmd_;
};

// When regoin delete, destroy region executor
class DestroyRegionExecutorTask : public TaskRunnable {
 public:
  DestroyRegionExecutorTask(std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~DestroyRegionExecutorTask() override = default;

  void Run() override;

 private:
  static butil::Status DestroyRegionExecutor(std::shared_ptr<Context> ctx, uint64_t region_id);

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::coordinator::RegionCmd> region_cmd_;
};

class ControlExecutor {
 public:
  explicit ControlExecutor() : is_available_(false), queue_id_({UINT64_MAX}) {}
  virtual ~ControlExecutor() = default;

  bool Init();

  bool Execute(TaskRunnable* task);

  void Stop();

 private:
  // Execution queue is available.
  std::atomic<bool> is_available_;
  bthread::ExecutionQueueId<TaskRunnable*> queue_id_;  // NOLINT
};

class RegionControlExecutor : public ControlExecutor {
 public:
  explicit RegionControlExecutor(uint64_t region_id) : region_id_(region_id) {}
  ~RegionControlExecutor() override = default;

 private:
  uint64_t region_id_;
};

class RegionCommandManager : public TransformKvAble {
 public:
  RegionCommandManager(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : TransformKvAble(Constant::kStoreRegionControlCommandPrefix),
        meta_reader_(meta_reader),
        meta_writer_(meta_writer) {
    bthread_mutex_init(&mutex_, nullptr);
  }
  ~RegionCommandManager() override { bthread_mutex_destroy(&mutex_); }

  RegionCommandManager(const RegionCommandManager&) = delete;
  const RegionCommandManager& operator=(const RegionCommandManager&) = delete;

  bool Init();

  bool IsExist(uint64_t command_id);

  void AddCommand(std::shared_ptr<pb::coordinator::RegionCmd> region_cmd);
  void UpdateCommandStatus(std::shared_ptr<pb::coordinator::RegionCmd> region_cmd,
                           pb::coordinator::RegionCmdStatus status);
  void UpdateCommandStatus(uint64_t command_id, pb::coordinator::RegionCmdStatus status);
  std::shared_ptr<pb::coordinator::RegionCmd> GetCommand(uint64_t command_id);
  std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> GetCommands(pb::coordinator::RegionCmdStatus status);
  std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> GetCommands(uint64_t region_id);
  std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> GetAllCommand();

 private:
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::any obj) override;
  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  bthread_mutex_t mutex_;
  // key: command id, value: command
  std::unordered_map<uint64_t, std::shared_ptr<pb::coordinator::RegionCmd>> region_commands_;
};

// Control all regions execute commands.
// Every region has a region control executor, so that all regions can concurrently execute commands.
class RegionController {
 public:
  RegionController() { bthread_mutex_init(&mutex_, nullptr); }
  ~RegionController() { bthread_mutex_destroy(&mutex_); }

  RegionController(const RegionController&) = delete;
  const RegionController& operator=(const RegionController&) = delete;

  bool Init();
  bool Recover();
  void Destroy();

  std::vector<uint64_t> GetAllRegion();

  bool RegisterExecutor(uint64_t region_id);
  void UnRegisterExecutor(uint64_t region_id);

  butil::Status DispatchRegionControlCommand(std::shared_ptr<Context> ctx,
                                             std::shared_ptr<pb::coordinator::RegionCmd> command);

  // For pre validate
  using ValidateFunc = std::function<butil::Status(const pb::coordinator::RegionCmd&)>;
  using ValidaterMap = std::map<pb::coordinator::RegionCmdType, ValidateFunc>;
  // For pre validate
  static ValidaterMap validaters;

  static ValidateFunc GetValidater(pb::coordinator::RegionCmdType);

 private:
  std::shared_ptr<RegionControlExecutor> GetRegionControlExecutor(uint64_t region_id);
  butil::Status InnerDispatchRegionControlCommand(std::shared_ptr<Context> ctx,
                                                  std::shared_ptr<pb::coordinator::RegionCmd> command);

  bthread_mutex_t mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<RegionControlExecutor>> executors_;

  // When have no regoin executor, used this executorm, like PURGE.
  std::shared_ptr<ControlExecutor> share_executor_;

  // task builder
  using TaskBuildFunc =
      std::function<TaskRunnable*(std::shared_ptr<Context>, std::shared_ptr<pb::coordinator::RegionCmd>)>;
  using TaskBuilderMap = std::map<pb::coordinator::RegionCmdType, TaskBuildFunc>;
  static TaskBuilderMap task_builders;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_REGION_CONTROL_H_