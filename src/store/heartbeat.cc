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

#include "store/heartbeat.h"

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"

namespace dingodb {

DEFINE_int32(executor_heartbeat_timeout, 30, "executor heartbeat timeout in seconds");
DEFINE_int32(store_heartbeat_timeout, 30, "store heartbeat timeout in seconds");
DEFINE_int32(region_heartbeat_timeout, 30, "region heartbeat timeout in seconds");
DEFINE_int32(region_delete_after_deleted_time, 86400, "delete region after deleted time in seconds");

DECLARE_string(raft_snapshot_policy);

DEFINE_int64(store_heartbeat_report_store_own_metrics_multiple, 10,
             "store heartbeat report store own metrics multiple, "
             "this defines how many times of heartbeat will "
             "report store_metrics once to coordinator");
DEFINE_int64(store_heartbeat_report_region_multiple, 10,
             "store heartbeat report region multiple, this defines how many times of heartbeat will report "
             "region_metrics once to coordinator");

std::atomic<uint64_t> HeartbeatTask::heartbeat_counter = 0;

void HeartbeatTask::SendStoreHeartbeat(std::shared_ptr<CoordinatorInteraction> coordinator_interaction,
                                       std::vector<int64_t> region_ids, bool is_update_epoch_version) {
  auto start_time = Helper::TimestampMs();
  auto first_start_time = start_time;
  auto engine = Server::GetInstance().GetEngine();
  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();

  pb::coordinator::StoreHeartbeatRequest request;

  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  request.set_self_storemap_epoch(store_meta_manager->GetStoreServerMeta()->GetEpoch());

  // store
  // CAUTION: may coredump here, so we cannot delete self store meta.
  *(request.mutable_store()) = (*store_meta_manager->GetStoreServerMeta()->GetStore(Server::GetInstance().Id()));

  uint64_t temp_heartbeat_count = 524287;
  if (region_ids.empty()) {
    temp_heartbeat_count = heartbeat_counter.fetch_add(1, std::memory_order_relaxed);
  }

  auto store_metrics_manager = Server::GetInstance().GetStoreMetricsManager();

  // store_metrics
  bool need_report_store_own_metrics =
      temp_heartbeat_count % FLAGS_store_heartbeat_report_store_own_metrics_multiple == 0 || region_ids.size() > 2;

  // region metrics
  // only partial heartbeat or temp_heartbeat_count % FLAGS_store_heartbeat_report_region_multiple == 0 will report
  // region_metrics, this is for reduce heartbeat size and cpu usage.
  bool need_report_region_metrics =
      !region_ids.empty() || (temp_heartbeat_count % FLAGS_store_heartbeat_report_region_multiple == 0);

  if (need_report_store_own_metrics) {
    *(request.mutable_store_metrics()) = (*store_metrics_manager->GetStoreMetrics()->Metrics());
    // setup id for store_metrics here, coordinator need this id to update store_metrics
    request.mutable_store_metrics()->set_id(Server::GetInstance().Id());
    request.mutable_store_metrics()->set_is_update_epoch_version(is_update_epoch_version);

    DINGO_LOG(INFO) << fmt::format("[heartbeat.store] start_time({}) store_metrics size({}) elapsed time({} ms)",
                                   first_start_time, request.mutable_store_metrics()->ByteSizeLong(),
                                   Helper::TimestampMs() - start_time)
                    << ", metrics: " << request.mutable_store_metrics()->ShortDebugString();
  } else {
    // setup id for store_metrics here, coordinator need this id to update store_metrics
    request.mutable_store_metrics()->set_id(Server::GetInstance().Id());
  }

  if (need_report_region_metrics) {
    DINGO_LOG(INFO) << fmt::format("[heartbeat.store] start_time({}) heartbeat_counter: {}", first_start_time,
                                   temp_heartbeat_count);

    auto* mut_region_metrics_map = request.mutable_store_metrics()->mutable_region_metrics_map();
    auto region_metrics = store_metrics_manager->GetStoreRegionMetrics();
    std::vector<store::RegionPtr> region_metas;
    if (region_ids.empty()) {
      region_metas = store_meta_manager->GetStoreRegionMeta()->GetAllRegion();
    } else {
      request.mutable_store_metrics()->set_is_partial_region_metrics(true);
      for (auto region_id : region_ids) {
        auto region_meta = store_meta_manager->GetStoreRegionMeta()->GetRegion(region_id);
        if (region_meta != nullptr) {
          region_metas.push_back(region_meta);
        }
      }
    }

    for (const auto& region_meta : region_metas) {
      auto inner_region = region_meta->InnerRegion();
      if (inner_region.state() == pb::common::StoreRegionState::SPLITTING ||
          inner_region.state() == pb::common::StoreRegionState::MERGING) {
        DINGO_LOG(WARNING) << fmt::format(
            "[heartbeat.store][region({})] start_time({}) region state({}) not suit heartbeat.", inner_region.id(),
            first_start_time, pb::common::StoreRegionState_Name(inner_region.state()));
        continue;
      }

      pb::common::RegionMetrics tmp_region_metrics;
      auto metrics = region_metrics->GetMetrics(inner_region.id());
      if (metrics != nullptr) {
        tmp_region_metrics = metrics->InnerRegionMetrics();
      }

      tmp_region_metrics.set_id(inner_region.id());
      tmp_region_metrics.set_leader_store_id(inner_region.leader_id());
      tmp_region_metrics.set_store_region_state(inner_region.state());
      *(tmp_region_metrics.mutable_region_definition()) = inner_region.definition();

      if (BAIDU_LIKELY(FLAGS_raft_snapshot_policy == Constant::kRaftSnapshotPolicyDingo)) {
        tmp_region_metrics.set_snapshot_epoch_version(INT64_MAX);
      } else {
        tmp_region_metrics.set_snapshot_epoch_version(inner_region.snapshot_epoch_version());
      }

      if (inner_region.state() == pb::common::StoreRegionState::NORMAL ||
          inner_region.state() == pb::common::StoreRegionState::STANDBY ||
          inner_region.state() == pb::common::StoreRegionState::TOMBSTONE) {
        auto raft_node = raft_store_engine->GetNode(inner_region.id());
        if (raft_node != nullptr) {
          *(tmp_region_metrics.mutable_braft_status()) = (*raft_node->GetStatus());
        }
      }

      auto vector_index_wrapper = region_meta->VectorIndexWrapper();
      if (vector_index_wrapper != nullptr) {
        auto* vector_index_status = tmp_region_metrics.mutable_vector_index_status();
        vector_index_status->set_is_stop(vector_index_wrapper->IsStop());
        vector_index_status->set_is_ready(vector_index_wrapper->IsReady());
        vector_index_status->set_is_own_ready(vector_index_wrapper->IsOwnReady());
        vector_index_status->set_is_build_error(vector_index_wrapper->IsBuildError());
        vector_index_status->set_is_rebuild_error(vector_index_wrapper->IsRebuildError());
        vector_index_status->set_is_switching(vector_index_wrapper->IsSwitchingVectorIndex());
        vector_index_status->set_is_hold_vector_index(vector_index_wrapper->IsOwnReady());
        vector_index_status->set_apply_log_id(vector_index_wrapper->ApplyLogId());
        vector_index_status->set_snapshot_log_id(vector_index_wrapper->SnapshotLogId());
        vector_index_status->set_last_build_epoch_version(vector_index_wrapper->LastBuildEpochVersion());
      }

      mut_region_metrics_map->insert({inner_region.id(), tmp_region_metrics});
    }

    DINGO_LOG(INFO) << fmt::format(
        "[heartbeat.store] start_time({}) request region count({}) size({}) region_ids_count({}), elapsed time({} ms)",
        first_start_time, mut_region_metrics_map->size(), request.ByteSizeLong(), region_ids.size(),
        Helper::TimestampMs() - start_time);
  }

  if (!region_ids.empty()) {
    std::string region_ids_str = "region_ids_count: " + std::to_string(region_ids.size()) + ", region_ids: ";
    for (const auto& region_id : region_ids) {
      region_ids_str += std::to_string(region_id) + ",";
    }

    DINGO_LOG(INFO) << fmt::format("[heartbeat.store] start_time({}) this is a partial heartbeat, {}", first_start_time,
                                   region_ids_str)
                    << " request: " << request.ShortDebugString();
  }

  start_time = Helper::TimestampMs();
  pb::coordinator::StoreHeartbeatResponse response;
  auto status = coordinator_interaction->SendRequest("StoreHeartbeat", request, response);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[heartbeat.store] start_time({}) store heartbeat failed, error: {}",
                                      first_start_time, Helper::PrintStatus(status));
    return;
  }

  DINGO_LOG(INFO) << fmt::format("[heartbeat.store] start_time({}) response size({}) elapsed time({} ms)",
                                 first_start_time, response.ByteSizeLong(), Helper::TimestampMs() - start_time);

  HeartbeatTask::HandleStoreHeartbeatResponse(store_meta_manager, response);
}

static std::vector<std::shared_ptr<pb::common::Store>> GetNewStore(
    std::map<int64_t, std::shared_ptr<pb::common::Store>> local_stores,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Store>& remote_stores) {
  std::vector<std::shared_ptr<pb::common::Store>> new_stores;
  for (const auto& remote_store : remote_stores) {
    if (local_stores.find(remote_store.id()) == local_stores.end()) {
      new_stores.push_back(std::make_shared<pb::common::Store>(remote_store));
    }
  }

  return new_stores;
}

static std::vector<std::shared_ptr<pb::common::Store>> GetChangedStore(
    std::map<int64_t, std::shared_ptr<pb::common::Store>> local_stores,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Store>& remote_stores) {
  std::vector<std::shared_ptr<pb::common::Store>> changed_stores;
  for (const auto& remote_store : remote_stores) {
    if (remote_store.id() == 0) {
      continue;
    }
    auto it = local_stores.find(remote_store.id());
    if (it != local_stores.end()) {
      if (it->second->raft_location().host() != remote_store.raft_location().host() ||
          it->second->raft_location().port() != remote_store.raft_location().port()) {
        changed_stores.push_back(std::make_shared<pb::common::Store>(remote_store));
      }
    }
  }

  return changed_stores;
}

static std::vector<std::shared_ptr<pb::common::Store>> GetDeletedStore(
    std::map<int64_t, std::shared_ptr<pb::common::Store>> local_stores,
    const google::protobuf::RepeatedPtrField<pb::common::Store>& remote_stores) {
  std::set<int64_t> remote_store_ids;
  for (const auto& store : remote_stores) {
    remote_store_ids.insert(store.id());
  }

  std::vector<std::shared_ptr<pb::common::Store>> stores_to_delete;
  for (const auto& store : local_stores) {
    if (remote_store_ids.find(store.first) == remote_store_ids.end()) {
      stores_to_delete.push_back(store.second);
    }
  }

  return stores_to_delete;
}

void HeartbeatTask::HandleStoreHeartbeatResponse(std::shared_ptr<dingodb::StoreMetaManager> store_meta_manager,
                                                 const pb::coordinator::StoreHeartbeatResponse& response) {
  // check error
  if (response.has_error() && response.error().errcode() != pb::error::OK) {
    DINGO_LOG(WARNING) << fmt::format("[heartbeat.store] store heartbeat response error: {} {}",
                                      pb::error::Errno_Name(response.error().errcode()), response.error().errmsg());
    return;
  }

  // Handle store meta data.
  auto store_server_meta = store_meta_manager->GetStoreServerMeta();
  auto local_stores = store_server_meta->GetAllStore();
  auto remote_stores = response.storemap().stores();

  auto new_stores = GetNewStore(local_stores, remote_stores);
  for (const auto& store : new_stores) {
    store_server_meta->AddStore(store);
  }

  auto changed_stores = GetChangedStore(local_stores, remote_stores);
  for (const auto& store : changed_stores) {
    store_server_meta->UpdateStore(store);
  }

  auto deleted_stores = GetDeletedStore(local_stores, remote_stores);
  DINGO_LOG(INFO) << fmt::format("[heartbeat.store] store stats new({}) change({}) delete({}) local({})",
                                 new_stores.size(), changed_stores.size(), deleted_stores.size(), local_stores.size());
  for (const auto& store : deleted_stores) {
    // if deleted store is self, skip, else will coredump in next heartbeat.
    if (store->id() == Server::GetInstance().Id()) {
      DINGO_LOG(ERROR) << fmt::format("[heartbeat.store] deleted store id({}) is self, skip", store->id());
      continue;
    }
    store_server_meta->DeleteStore(store->id());
  }

  // set up read-only
  auto is_read_only = Server::GetInstance().IsReadOnly();
  if (is_read_only != response.cluster_state().cluster_is_read_only()) {
    Server::GetInstance().SetReadOnly(response.cluster_state().cluster_is_read_only());
    DINGO_LOG(WARNING) << fmt::format("[heartbeat.store] cluster set read-only to {}", is_read_only);
  }
}

static std::atomic<bool> g_store_recycle_orphan_running(false);
void CoordinatorRecycleOrphanTask::CoordinatorRecycleOrphan(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (g_store_recycle_orphan_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "CoordinatorRecycleOrphan... g_store_recycle_orphan_running is true, return";
    return;
  }

  AtomicGuard guard(g_store_recycle_orphan_running);

  coordinator_control->RecycleOrphanRegionOnStore();

  coordinator_control->RecycleOrphanRegionOnCoordinator();

  coordinator_control->RecycleDeletedTableAndIndex();

  coordinator_control->RecycleOutdatedStoreMetrics();
}

static std::atomic<bool> g_store_meta_watch_clean_running(false);
void CoordinatorMetaWatchCleanTask::CoordinatorMetaWatchClean(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (g_store_meta_watch_clean_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "CoordinatorMetaWatchClean... g_store_meta_watch_clean_running is true, return";
    return;
  }

  AtomicGuard guard(g_store_meta_watch_clean_running);

  coordinator_control->RecycleOutdatedMetaWatcher();

  coordinator_control->TrimMetaWatchEventList();
}

static std::atomic<bool> g_remove_one_time_watch_running(false);
void KvRemoveOneTimeWatchTask::KvRemoveOneTimeWatch(std::shared_ptr<KvControl> kv_control) {
  if (!kv_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "KvRemoveOneTimeWatch ... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "KvRemoveOneTimeWatch... this is leader";

  if (g_remove_one_time_watch_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "KvRemoveOneTimeWatch... g_remove_one_time_watch_running is true, return";
    return;
  }

  AtomicGuard guard(g_remove_one_time_watch_running);

  kv_control->RemoveOneTimeWatch();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_update_state_running(false);
void CoordinatorUpdateStateTask::CoordinatorUpdateState(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is leader";

  if (g_coordinator_update_state_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "CoordinatorUpdateState... g_coordinator_update_state_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_update_state_running);

  // update executor_state by last_seen_timestamp
  pb::common::ExecutorMap executor_map_temp;
  coordinator_control->GetExecutorMap(executor_map_temp);
  for (const auto& it : executor_map_temp.executors()) {
    if (it.state() == pb::common::ExecutorState::EXECUTOR_NORMAL) {
      if (it.last_seen_timestamp() + (FLAGS_executor_heartbeat_timeout * 1000) < butil::gettimeofday_ms()) {
        DINGO_LOG(INFO) << "CoordinatorUpdateState... update executor " << it.id() << " state to offline";
        coordinator_control->TrySetExecutorToOffline(it.id());
        continue;
      }
    } else {
      continue;
    }
  }

  // RegionHeartbeatState will deprecated in future
  // coordinator_control->UpdateRegionState();

  // now update store state in SendCoordinatorPushToStore

  // update store_state by last_seen_timestamp and send store operation to store
  // here we only update store_state to offline if last_seen_timestamp is too old
  // we will not update store_state to online here
  // in on_apply of store_heartbeat, we will update store_state to online
  // pb::common::StoreMap store_map_temp;
  // coordinator_control->GetStoreMap(store_map_temp);
  // for (const auto& it : store_map_temp.stores()) {
  //   if (it.state() == pb::common::StoreState::STORE_NORMAL) {
  //     if (it.last_seen_timestamp() + (60 * 1000) < butil::gettimeofday_ms()) {
  //       DINGO_LOG(INFO) << "SendCoordinatorPushToStore... update store " << it.id() << " state to offline";
  //       coordinator_control->TrySetStoreToOffline(it.id());
  //       continue;
  //     }
  //   } else {
  //     continue;
  //   }
  // }

  // update cluster is_read_only
  coordinator_control->UpdateClusterReadOnly();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_task_list_process_running(false);
void CoordinatorTaskListProcessTask::CoordinatorTaskListProcess(
    std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is leader";

  if (g_coordinator_task_list_process_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "CoordinatorTaskListProcess... g_coordinator_task_list_process_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_task_list_process_running);

  coordinator_control->ProcessTaskList();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_push_to_store_running(false);
void CoordinatorPushTask::SendCoordinatorPushToStore(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "... this is leader";

  if (g_coordinator_push_to_store_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "... g_coordinator_push_to_store_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_push_to_store_running);

  coordinator_control->TryToSendStoreOperations();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_calc_metrics_running(false);
void CalculateTableMetricsTask::CalculateTableMetrics(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    // DINGO_LOG(INFO) << "SendCoordinatorPushToStore... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CalculateTableMetrics... this is leader";

  if (g_coordinator_calc_metrics_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "CalculateTableMetrics... g_coordinator_calc_metrics_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_calc_metrics_running);

  coordinator_control->CalculateTableMetrics();
  coordinator_control->CalculateIndexMetrics();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_lease_running(false);
void LeaseTask::ExecLeaseTask(std::shared_ptr<KvControl> kv_control) {
  if (!kv_control->IsLeader()) {
    // DINGO_LOG(INFO) << "ExecLeaseTask... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "ExecLeaseTask... this is leader";

  if (g_coordinator_lease_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "ExecLeaseTask... g_coordinator_lease_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_lease_running);

  kv_control->LeaseTask();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_compaction_running(false);
void CompactionTask::ExecCompactionTask(std::shared_ptr<KvControl> kv_control) {
  if (!kv_control->IsLeader()) {
    // DINGO_LOG(INFO) << "ExecCompactionTask... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "ExecCompactionTask... this is leader";

  if (g_coordinator_compaction_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "ExecCompactionTask... g_coordinator_compaction_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_compaction_running);

  kv_control->CompactionTask();
}

// this is for index
void VectorIndexScrubTask::ScrubVectorIndex() {
  auto status = VectorIndexManager::ScrubVectorIndex();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("Scrub vector index failed, error: {}", status.error_str());
  }
}

bool Heartbeat::Init() {
  auto worker = Worker::New();
  if (!worker->Init()) {
    return false;
  }
  worker_ = worker;

  return true;
}

void Heartbeat::Destroy() {
  if (worker_) {
    worker_->Destroy();
  }
}

bool Heartbeat::Execute(TaskRunnablePtr task) {
  if (worker_ == nullptr) {
    DINGO_LOG(ERROR) << "Heartbeat worker is nullptr.";
    return false;
  }
  return worker_->Execute(task);
}

void Heartbeat::TriggerStoreHeartbeat(std::vector<int64_t> region_ids, bool is_update_epoch_version) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<HeartbeatTask>(Server::GetInstance().GetCoordinatorInteraction(), region_ids,
                                              is_update_epoch_version);
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerCoordinatorPushToStore(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<CoordinatorPushTask>(Server::GetInstance().GetCoordinatorControl());
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerCoordinatorUpdateState(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<CoordinatorUpdateStateTask>(Server::GetInstance().GetCoordinatorControl());
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerCoordinatorTaskListProcess(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<CoordinatorTaskListProcessTask>(Server::GetInstance().GetCoordinatorControl());
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerCoordinatorRecycleOrphan(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<CoordinatorRecycleOrphanTask>(Server::GetInstance().GetCoordinatorControl());
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerCoordinatorMetaWatchClean(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<CoordinatorMetaWatchCleanTask>(Server::GetInstance().GetCoordinatorControl());
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerKvRemoveOneTimeWatch(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<KvRemoveOneTimeWatchTask>(Server::GetInstance().GetKvControl());
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerCalculateTableMetrics(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<CalculateTableMetricsTask>(Server::GetInstance().GetCoordinatorControl());
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerLeaseTask(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<LeaseTask>(Server::GetInstance().GetKvControl());
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerCompactionTask(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<CompactionTask>(Server::GetInstance().GetKvControl());
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

void Heartbeat::TriggerScrubVectorIndex(void*) {
  // Free at ExecuteRoutine()
  auto task = std::make_shared<VectorIndexScrubTask>();
  Server::GetInstance().GetHeartbeat()->Execute(task);
}

}  // namespace dingodb
