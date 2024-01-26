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

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/runnable.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/store_meta_manager.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"

namespace dingodb {

using RegionCmdPtr = std::shared_ptr<pb::coordinator::RegionCmd>;

class CreateRegionTask : public TaskRunnable {
 public:
  CreateRegionTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~CreateRegionTask() override = default;

  std::string Type() override { return "CREATE_REGION"; }

  void Run() override;

  static butil::Status PreValidateCreateRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateCreateRegion(std::shared_ptr<StoreMetaManager> store_meta_manager, int64_t region_id,
                                            const pb::common::RegionDefinition& region_definiton);
  static butil::Status CreateRegion(const pb::common::RegionDefinition& definition, int64_t parent_region_id);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class DeleteRegionTask : public TaskRunnable {
 public:
  DeleteRegionTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~DeleteRegionTask() override = default;

  std::string Type() override { return "DELETE_REGION"; }

  void Run() override;

  static butil::Status PreValidateDeleteRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateDeleteRegion(std::shared_ptr<StoreMetaManager> /*store_meta_manager*/,
                                            store::RegionPtr region);
  static butil::Status DeleteRegion(std::shared_ptr<Context> ctx, int64_t region_id);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class SplitRegionTask : public TaskRunnable {
 public:
  SplitRegionTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~SplitRegionTask() override = default;

  std::string Type() override { return "SPLIT_REGION"; }

  void Run() override;

  static butil::Status PreValidateSplitRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateSplitRegion(std::shared_ptr<StoreRegionMeta> store_region_meta,
                                           const pb::coordinator::SplitRequest& split_request, int64_t job_id);
  butil::Status SplitRegion();

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class MergeRegionTask : public TaskRunnable {
 public:
  MergeRegionTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~MergeRegionTask() override = default;

  std::string Type() override { return "MERGE_REGION"; }

  void Run() override;

  static butil::Status PreValidateMergeRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateMergeRegion(std::shared_ptr<StoreRegionMeta> store_region_meta,
                                           const pb::coordinator::MergeRequest& merge_request,
                                           int64_t& min_applied_log_id);
  butil::Status MergeRegion();

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class ChangeRegionTask : public TaskRunnable {
 public:
  ChangeRegionTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~ChangeRegionTask() override = default;

  std::string Type() override { return "CHANGE_REGION"; }

  void Run() override;

  static butil::Status PreValidateChangeRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateChangeRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                            const pb::common::RegionDefinition& region_definition);

  static butil::Status ChangeRegion(std::shared_ptr<Context> ctx, RegionCmdPtr command);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class TransferLeaderTask : public TaskRunnable {
 public:
  TransferLeaderTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~TransferLeaderTask() override = default;

  std::string Type() override { return "TRANSFER_LEADER"; }

  void Run() override;

  static butil::Status PreValidateTransferLeader(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateTransferLeader(std::shared_ptr<StoreMetaManager> store_meta_manager, int64_t region_id,
                                              const pb::common::Peer& peer);
  static butil::Status TransferLeader(std::shared_ptr<Context> ctx, int64_t region_id, const pb::common::Peer& peer);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class SnapshotRegionTask : public TaskRunnable {
 public:
  SnapshotRegionTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~SnapshotRegionTask() override = default;

  std::string Type() override { return "SNAPSHOT_REGION"; }

  void Run() override;

 private:
  static butil::Status Snapshot(std::shared_ptr<Context> ctx, int64_t region_id);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

// Clean region zombie, free region meta resource.
class PurgeRegionTask : public TaskRunnable {
 public:
  PurgeRegionTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~PurgeRegionTask() override = default;

  std::string Type() override { return "PURGE_REGION"; }

  void Run() override;

  static butil::Status PreValidatePurgeRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidatePurgeRegion(store::RegionPtr region);
  static butil::Status PurgeRegion(std::shared_ptr<Context> ctx, int64_t region_id);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

// Stop raft peer
class StopRegionTask : public TaskRunnable {
 public:
  StopRegionTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~StopRegionTask() override = default;

  std::string Type() override { return "STOP_REGION"; }

  void Run() override;

  static butil::Status PreValidateStopRegion(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateStopRegion(store::RegionPtr region);
  static butil::Status StopRegion(std::shared_ptr<Context> ctx, int64_t region_id);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

// When regoin delete, destroy region executor
class DestroyRegionExecutorTask : public TaskRunnable {
 public:
  DestroyRegionExecutorTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd)
      : ctx_(ctx), region_cmd_(region_cmd) {}
  ~DestroyRegionExecutorTask() override = default;

  std::string Type() override { return "DESTROY_REGION_EXECUTOR"; }

  void Run() override;

 private:
  static butil::Status DestroyRegionExecutor(std::shared_ptr<Context> ctx, int64_t region_id);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class UpdateDefinitionTask : public TaskRunnable {
 public:
  UpdateDefinitionTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~UpdateDefinitionTask() override = default;

  std::string Type() override { return "UPDATE_DEFINITION"; }

  void Run() override;

  static butil::Status PreValidateUpdateDefinition(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status ValidateUpdateDefinition(store::RegionPtr region);

  static butil::Status UpdateDefinition(std::shared_ptr<Context> ctx, int64_t region_id,
                                        const pb::common::RegionDefinition& new_definition);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class SwitchSplitTask : public TaskRunnable {
 public:
  SwitchSplitTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~SwitchSplitTask() override = default;

  std::string Type() override { return "SWITCH_SPLIT"; }

  void Run() override;

  static butil::Status PreValidateSwitchSplit(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status SwitchSplit(std::shared_ptr<Context> ctx, int64_t region_id, bool disable_split);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class HoldVectorIndexTask : public TaskRunnable {
 public:
  HoldVectorIndexTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~HoldVectorIndexTask() override = default;

  std::string Type() override { return "HOLD_VECTOR_INDEX"; }

  void Run() override;

  static butil::Status PreValidateHoldVectorIndex(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status HoldVectorIndex(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd);
  static butil::Status ValidateHoldVectorIndex(int64_t region_id);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class SnapshotVectorIndexTask : public TaskRunnable {
 public:
  SnapshotVectorIndexTask(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd) : ctx_(ctx), region_cmd_(region_cmd) {}
  ~SnapshotVectorIndexTask() override = default;

  std::string Type() override { return "SNAPSHOT_VECTOR_INDEX"; }

  void Run() override;

  static butil::Status PreValidateSnapshotVectorIndex(const pb::coordinator::RegionCmd& command);

 private:
  static butil::Status SaveSnapshotSync(std::shared_ptr<Context> ctx, int64_t vector_index_id);
  static butil::Status SaveSnapshotAsync(std::shared_ptr<Context> ctx, RegionCmdPtr region_cmd);
  static butil::Status ValidateSaveVectorIndex(int64_t region_id);

  std::shared_ptr<Context> ctx_;
  RegionCmdPtr region_cmd_;
};

class ControlExecutor {
 public:
  explicit ControlExecutor() { worker_ = Worker::New(); }
  virtual ~ControlExecutor() = default;

  bool Init();

  bool Execute(TaskRunnablePtr task);

  void Stop();

 private:
  WorkerPtr worker_;
};

class RegionControlExecutor : public ControlExecutor {
 public:
  explicit RegionControlExecutor(int64_t region_id) : region_id_(region_id) {}
  ~RegionControlExecutor() override = default;

 private:
  int64_t region_id_;
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

  bool IsExist(int64_t command_id);

  void AddCommand(RegionCmdPtr region_cmd);
  void UpdateCommandStatus(RegionCmdPtr region_cmd, pb::coordinator::RegionCmdStatus status);
  void UpdateCommandStatus(int64_t command_id, pb::coordinator::RegionCmdStatus status);
  RegionCmdPtr GetCommand(int64_t command_id);
  std::vector<RegionCmdPtr> GetCommands(pb::coordinator::RegionCmdStatus status);
  std::vector<RegionCmdPtr> GetCommands(int64_t region_id);
  std::vector<RegionCmdPtr> GetAllCommand();

 private:
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::any obj) override;
  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  bthread_mutex_t mutex_;
  // key: command id, value: command
  std::unordered_map<int64_t, RegionCmdPtr> region_commands_;
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

  bool InitVectorAppliedLogId(int64_t region_id);

  std::vector<int64_t> GetAllRegion();

  bool RegisterExecutor(int64_t region_id);
  void UnRegisterExecutor(int64_t region_id);

  butil::Status DispatchRegionControlCommand(std::shared_ptr<Context> ctx, RegionCmdPtr command);

  // For pre validate
  using ValidateFunc = std::function<butil::Status(const pb::coordinator::RegionCmd&)>;
  using ValidaterMap = std::map<pb::coordinator::RegionCmdType, ValidateFunc>;
  // For pre validate
  static ValidaterMap validaters;

  static ValidateFunc GetValidater(pb::coordinator::RegionCmdType);

 private:
  std::shared_ptr<RegionControlExecutor> GetRegionControlExecutor(int64_t region_id);
  butil::Status InnerDispatchRegionControlCommand(std::shared_ptr<Context> ctx, RegionCmdPtr command);

  bthread_mutex_t mutex_;
  std::unordered_map<int64_t, std::shared_ptr<RegionControlExecutor>> executors_;

  // When have no regoin executor, used this executorm, like PURGE.
  std::shared_ptr<ControlExecutor> share_executor_;

  // task builder
  using TaskBuildFunc = std::function<TaskRunnablePtr(std::shared_ptr<Context>, RegionCmdPtr)>;
  using TaskBuilderMap = std::map<pb::coordinator::RegionCmdType, TaskBuildFunc>;
  static TaskBuilderMap task_builders;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_REGION_CONTROL_H_