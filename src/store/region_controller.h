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

#include "bthread/execution_queue.h"
#include "butil/macros.h"
#include "butil/strings/stringprintf.h"
#include "common/constant.h"
#include "common/context.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/store_meta_manager.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/store_internal.pb.h"

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

  static butil::Status ValidateCreateRegion(std::shared_ptr<StoreMetaManager> store_meta_manager, uint64_t region_id);

 private:
  static butil::Status CreateRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::store_internal::Region> region,
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

  static butil::Status ValidateDeleteRegion(std::shared_ptr<StoreMetaManager> /*store_meta_manager*/,
                                            std::shared_ptr<pb::store_internal::Region> region);

 private:
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

  static butil::Status ValidateSplitRegion(std::shared_ptr<StoreRegionMeta> store_region_meta,
                                           const pb::coordinator::SplitRequest& split_request);

 private:
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

  static butil::Status ValidateChangeRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                            const pb::common::RegionDefinition& region_definition);

 private:
  static butil::Status ChangeRegion(std::shared_ptr<Context> ctx,
                                    const pb::common::RegionDefinition& region_definition);

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

class RegionControlExecutor {
 public:
  explicit RegionControlExecutor(uint64_t region_id) : region_id_(region_id), queue_id_({0}) {}
  ~RegionControlExecutor() = default;

  bool Init();

  bool Execute(TaskRunnable* task);

  void Stop();

 private:
  uint64_t region_id_;
  bthread::ExecutionQueueId<TaskRunnable*> queue_id_;
};

class RegionCommandManager : public TransformKvAble {
 public:
  RegionCommandManager(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : TransformKvAble(Constant::kStoreRegionControllerPrefix), meta_reader_(meta_reader), meta_writer_(meta_writer) {
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

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t command_id) override;
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::shared_ptr<google::protobuf::Message> obj) override;

  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

 private:
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
  void Destroy();

  butil::Status DispatchRegionControlCommand(std::shared_ptr<Context> ctx,
                                             std::shared_ptr<pb::coordinator::RegionCmd> command);

  using TaskBuildFunc =
      std::function<TaskRunnable*(std::shared_ptr<Context>, std::shared_ptr<pb::coordinator::RegionCmd>)>;
  static std::map<pb::coordinator::RegionCmdType, TaskBuildFunc> task_builders;

 private:
  bool RegisterExecutor(uint64_t region_id);

  std::shared_ptr<RegionControlExecutor> GetRegionControlExecutor(uint64_t region_id);

  bthread_mutex_t mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<RegionControlExecutor>> executors_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_REGION_CONTROL_H_