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

#ifndef DINGODB_SERVER_HEARTBEAT_H_
#define DINGODB_SERVER_HEARTBEAT_H_

#include <atomic>
#include <memory>

#include "brpc/channel.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "coordinator/coordinator_interaction.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/push.pb.h"
#include "store/region_controller.h"

namespace dingodb {

class HeartbeatTask : public TaskRunnable {
 public:
  HeartbeatTask(std::shared_ptr<CoordinatorInteraction> coordinator_interaction)
      : coordinator_interaction_(coordinator_interaction) {}
  ~HeartbeatTask() override = default;

  void Run() override { SendStoreHeartbeat(coordinator_interaction_); }

  static void SendStoreHeartbeat(std::shared_ptr<CoordinatorInteraction> coordinator_interaction);
  static void HandleStoreHeartbeatResponse(std::shared_ptr<StoreMetaManager> store_meta,
                                           const pb::coordinator::StoreHeartbeatResponse& response);

 private:
  std::shared_ptr<CoordinatorInteraction> coordinator_interaction_;
};

class CoordinatorPushTask : public TaskRunnable {
 public:
  CoordinatorPushTask(std::shared_ptr<CoordinatorControl> coordinator_control)
      : coordinator_control_(coordinator_control) {}
  ~CoordinatorPushTask() override = default;

  void Run() override { SendCoordinatorPushToStore(coordinator_control_); }

 private:
  static void SendCoordinatorPushToStore(std::shared_ptr<CoordinatorControl> coordinator_control);

  std::shared_ptr<CoordinatorControl> coordinator_control_;
};

class CoordinatorUpdateStateTask : public TaskRunnable {
 public:
  CoordinatorUpdateStateTask(std::shared_ptr<CoordinatorControl> coordinator_control)
      : coordinator_control_(coordinator_control) {}
  ~CoordinatorUpdateStateTask() override = default;

  void Run() override { CoordinatorUpdateState(coordinator_control_); }

 private:
  static void CoordinatorUpdateState(std::shared_ptr<CoordinatorControl> coordinator_control);

  std::shared_ptr<CoordinatorControl> coordinator_control_;
};

class CoordinatorTaskListProcessTask : public TaskRunnable {
 public:
  CoordinatorTaskListProcessTask(std::shared_ptr<CoordinatorControl> coordinator_control)
      : coordinator_control_(coordinator_control) {}
  ~CoordinatorTaskListProcessTask() override = default;

  void Run() override { CoordinatorTaskListProcess(coordinator_control_); }

 private:
  static void CoordinatorTaskListProcess(std::shared_ptr<CoordinatorControl> coordinator_control);

  std::shared_ptr<CoordinatorControl> coordinator_control_;
};

class CoordinatorRecycleOrphanTask : public TaskRunnable {
 public:
  CoordinatorRecycleOrphanTask(std::shared_ptr<CoordinatorControl> coordinator_control)
      : coordinator_control_(coordinator_control) {}
  ~CoordinatorRecycleOrphanTask() override = default;

  void Run() override { CoordinatorRecycleOrphan(coordinator_control_); }

 private:
  static void CoordinatorRecycleOrphan(std::shared_ptr<CoordinatorControl> coordinator_control);

  std::shared_ptr<CoordinatorControl> coordinator_control_;
};

class CalculateTableMetricsTask : public TaskRunnable {
 public:
  CalculateTableMetricsTask(std::shared_ptr<CoordinatorControl> coordinator_control)
      : coordinator_control_(coordinator_control) {}
  ~CalculateTableMetricsTask() override = default;

  void Run() override { CalculateTableMetrics(coordinator_control_); }

 private:
  static void CalculateTableMetrics(std::shared_ptr<CoordinatorControl> coordinator_control);

  std::shared_ptr<CoordinatorControl> coordinator_control_;
};

class Heartbeat {
 public:
  Heartbeat() : queue_id_({0}) {}
  ~Heartbeat() = default;

  Heartbeat(const Heartbeat&) = delete;
  const Heartbeat& operator=(const Heartbeat&) = delete;

  bool Init();
  void Destroy();

  static void TriggerStoreHeartbeat(void*);
  static void TriggerCoordinatorPushToStore(void*);
  static void TriggerCoordinatorUpdateState(void*);
  static void TriggerCoordinatorTaskListProcess(void*);
  static void TriggerCoordinatorRecycleOrphan(void*);
  static void TriggerCalculateTableMetrics(void*);

  static butil::Status RpcSendPushStoreOperation(const pb::common::Location& location,
                                                 const pb::push::PushStoreOperationRequest& request,
                                                 pb::push::PushStoreOperationResponse& response);

 private:
  bool Execute(TaskRunnable* task);
  static int ExecuteRoutine(void*, bthread::TaskIterator<TaskRunnable*>& iter);

  bthread::ExecutionQueueId<TaskRunnable*> queue_id_;
};

}  // namespace dingodb

#endif  // DINGODB_SERVER_HEARTBEAT_H_