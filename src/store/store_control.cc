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

#include "store/store_control.h"

#include <memory>

#include "common/helper.h"
#include "server/server.h"

namespace dingodb {

StoreControl::StoreControl() { bthread_mutex_init(&control_mutex_, nullptr); };
StoreControl::~StoreControl() { bthread_mutex_destroy(&control_mutex_); };

butil::Status ValidateAddRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                std::shared_ptr<pb::common::Region> region) {
  if (store_meta_manager->IsExistRegion(region->id())) {
    return butil::Status(pb::error::EREGION_ALREADY_EXIST, "Region already exist");
  }

  if (region->state() != pb::common::REGION_NEW) {
    return butil::Status(pb::error::EREGION_STATE, "Region state not new");
  }

  return butil::Status();
}

butil::Status StoreControl::AddRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region) {
  BAIDU_SCOPED_LOCK(control_mutex_);
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  LOG(INFO) << "Add region info: " << region->DebugString();

  // Valiate region
  auto status = ValidateAddRegion(store_meta_manager, region);
  if (!status.ok()) {
    return status;
  }

  // Add raft node
  auto engine = std::dynamic_pointer_cast<RaftKvEngine>(Server::GetInstance()->GetEngine(pb::common::ENG_RAFT_STORE));
  if (engine == nullptr) {
    return butil::Status(pb::error::ESTORE_NOTEXIST_RAFTENGINE, "Not exist raft engine");
  }
  status = engine->AddRegion(ctx, region);
  if (!status.ok()) {
    return status;
  }

  // Add region to store region meta manager
  store_meta_manager->AddRegion(region);

  // Update region state
  region->set_state(pb::common::REGION_NORMAL);
  store_meta_manager->UpdateRegion(region);

  return butil::Status();
}

butil::Status ValidateChangeRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                   std::shared_ptr<pb::common::Region> region) {
  if (!store_meta_manager->IsExistRegion(region->id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Region not exist, cant't change.");
  }

  if (region->state() != pb::common::REGION_NORMAL && region->state() != pb::common::REGION_EXPAND &&
      region->state() != pb::common::REGION_SHRINK && region->state() != pb::common::REGION_DEGRADED &&
      region->state() != pb::common::REGION_DANGER) {
    return butil::Status(pb::error::EREGION_STATE, "Region state not allow change.");
  }

  return butil::Status();
}

butil::Status StoreControl::ChangeRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region) {
  BAIDU_SCOPED_LOCK(control_mutex_);
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  // Valiate region
  auto status = ValidateChangeRegion(store_meta_manager, region);
  if (!status.ok()) {
    return status;
  }

  auto filter_peers_by_role = [region](pb::common::PeerRole role) -> std::vector<pb::common::Peer> {
    std::vector<pb::common::Peer> peers;
    for (const auto& peer : region->peers()) {
      if (peer.role() == role) {
        peers.push_back(peer);
      }
    }
    return peers;
  };

  auto engine = std::dynamic_pointer_cast<RaftKvEngine>(Server::GetInstance()->GetEngine(pb::common::ENG_RAFT_STORE));
  if (engine == nullptr) {
    return butil::Status(pb::error::ESTORE_NOTEXIST_RAFTENGINE, "Not exist raft engine");
  }
  return engine->ChangeRegion(ctx, region->id(), filter_peers_by_role(pb::common::VOTER));
}

butil::Status ValidateDeleteRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                   std::shared_ptr<pb::common::Region> region) {
  if (region->state() == pb::common::REGION_DELETE || region->state() == pb::common::REGION_DELETING ||
      region->state() == pb::common::REGION_DELETED) {
    return butil::Status(pb::error::EREGION_STATE, "Region state not allow delete.");
  }

  return butil::Status();
}

butil::Status StoreControl::DeleteRegion(std::shared_ptr<Context> ctx, uint64_t region_id) {
  BAIDU_SCOPED_LOCK(control_mutex_);
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  auto region = store_meta_manager->GetRegion(region_id);

  // Valiate region
  auto status = ValidateDeleteRegion(store_meta_manager, region);
  if (!status.ok()) {
    return status;
  }

  // Update state
  region->set_state(pb::common::REGION_DELETING);
  store_meta_manager->UpdateRegion(region);

  // Shutdown raft node
  auto engine = std::dynamic_pointer_cast<RaftKvEngine>(Server::GetInstance()->GetEngine(pb::common::ENG_RAFT_STORE));
  if (engine == nullptr) {
    return butil::Status(pb::error::ESTORE_NOTEXIST_RAFTENGINE, "Not exist raft engine");
  }

  // Delete data
  auto writer = engine->GetRawEngine()->NewWriter(Constant::kStoreDataCF);
  writer->KvDeleteRange(region->range());

  // Delete raft
  engine->DestroyRegion(ctx, region_id);

  // Delete meta data
  region->set_state(pb::common::REGION_DELETED);
  // store_meta_manager->DeleteRegion(region_id);

  // Free other resources

  return butil::Status();
}

}  // namespace dingodb
