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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGODB_COORDINATOR_CLIENT_FUNCTION_H_
#define DINGODB_COORDINATOR_CLIENT_FUNCTION_H_

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "coordinator/coordinator_interaction.h"

// node service functions
void SendGetNodeInfo();
void SendGetLogLevel();
void SendChangeLogLevel();

// raft_control functions
void SendRaftAddPeer();
void SendRaftRemovePeer();
void SendRaftTransferLeader();
void SendRaftSnapshot();
void SendRaftResetPeer();

// coordinator service functions
void SendHello(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetStoreMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetExecutorMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetCoordinatorMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetDeletedRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendAddDeletedRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCleanDeletedRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetRegionCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateStore(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteStore(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendUpdateStore(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendStoreHearbeat(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, int64_t store_id);
void SendGetStoreMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteStoreMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetRegionMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteRegionMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateExecutor(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteExecutor(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateExecutorUser(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendUpdateExecutorUser(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteExecutorUser(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetExecutorUserMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendExecutorHeartbeat(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// region
void SendCreateRegionId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendQueryRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateRegionForSplit(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDropRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDropRegionPermanently(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendSplitRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendMergeRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendAddPeerRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendRemovePeerRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetOrphanRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendTransferLeaderRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendScanRegions(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// store operation
void SendGetStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCleanStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendAddStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendRemoveStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetRegionCmd(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// task list
void SendGetTaskList(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCleanTaskList(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// meta service functions
void SendGetSchemas(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetSchemaByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTablesCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTablesBySchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTableByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTableRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateTableId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, bool with_increment);
void SendDropTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDropSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTableMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

void SendGetIndexesCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndexes(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndexByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndexRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateIndexId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendUpdateIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDropIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndexMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendSwitchAutoSplit(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

void SendGetDeletedTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetDeletedIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCleanDeletedTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCleanDeletedIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// table index
void SendGenerateTableIds(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateTables(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTables(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDropTables(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// auto increment functions
void SendGetAutoIncrements(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendUpdateAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGenerateAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// lease
void SendLeaseGrant(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendLeaseRevoke(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendLeaseRenew(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendLeaseQuery(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendListLeases(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// kv
void SendGetRawKvIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetRawKvRev(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCoorKvRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCoorKvPut(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCoorKvDeleteRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCoorKvCompaction(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// watch
void SendOneTimeWatch(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendLock(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// tso
void SendGenTso(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// gc
void SendGetGCSafePoint(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendUpdateGCSafePoint(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// debug
void SendDebug();
std::string EncodeUint64(int64_t value);
int64_t DecodeUint64(const std::string& str);
bool GetBrpcChannel(const std::string& location, brpc::Channel& channel);

class Bthread {
 public:
  template <class Fn, class... Args>
  Bthread(const bthread_attr_t* attr, Fn&& fn, Args&&... args) {  // NOLINT
    auto p_wrap_fn = new auto([=] { fn(args...); });
    auto call_back = [](void* ar) -> void* {
      auto f = reinterpret_cast<decltype(p_wrap_fn)>(ar);
      (*f)();
      delete f;
      return nullptr;
    };

    bthread_start_background(&th_, attr, call_back, (void*)p_wrap_fn);
    joinable_ = true;
  }

  void Join() {
    if (joinable_) {
      bthread_join(th_, nullptr);
      joinable_ = false;
    }
  }

  bool Joinable() const noexcept { return joinable_; }

  bthread_t GetId() const { return th_; }

 private:
  bthread_t th_;
  bool joinable_ = false;
};

#endif  // DINGODB_COORDINATOR_CLIENT_FUNCTION_H_
