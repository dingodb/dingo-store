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

pb::error::Errno ValidateAddRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                   std::shared_ptr<pb::common::Region> region) {
  if (store_meta_manager->IsExistRegion(region->id())) {
    return pb::error::EREGION_ALREADY_EXIST;
  }

  return pb::error::OK;
}

pb::error::Errno StoreControl::AddRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region) {
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();

  // Valiate region
  auto errcode = ValidateAddRegion(store_meta_manager, region);
  if (errcode != pb::error::OK) {
    return errcode;
  }

  // Add raft node
  auto engine = std::dynamic_pointer_cast<RaftKvEngine>(Server::GetInstance()->GetEngine(pb::common::ENG_RAFT_STORE));
  if (engine == nullptr) {
    return pb::error::ESTORE_NOTEXIST_RAFTENGINE;
  }
  errcode = engine->AddRegion(ctx, region);
  if (errcode != pb::error::OK) {
    return errcode;
  }

  // Add region to store region meta manager
  store_meta_manager->AddRegion(region);
  return pb::error::OK;
}

void StoreControl::AddRegions(std::shared_ptr<Context> ctx, std::vector<std::shared_ptr<pb::common::Region> > regions) {
  for (auto region : regions) {
    AddRegion(ctx, region);
  }
}

pb::error::Errno StoreControl::ChangeRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region) {
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
    return pb::error::ESTORE_NOTEXIST_RAFTENGINE;
  }
  return engine->ChangeRegion(ctx, region->id(), filter_peers_by_role(pb::common::VOTER));
}

pb::error::Errno StoreControl::DeleteRegion(std::shared_ptr<Context> ctx, uint64_t region_id) {
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  auto region = store_meta_manager->GetRegion(region_id);

  // Check region status

  // Shutdown raft node
  auto engine = std::dynamic_pointer_cast<RaftKvEngine>(Server::GetInstance()->GetEngine(pb::common::ENG_RAFT_STORE));
  if (engine == nullptr) {
    return pb::error::ESTORE_NOTEXIST_RAFTENGINE;
  }
  engine->DestroyRegion(ctx, region_id);

  // Delete data
  ctx->set_directly_delete(true);
  engine->KvDeleteRange(ctx, region->range());

  // Delete meta data
  store_meta_manager->DeleteRegion(region_id);

  // Free other resources

  return pb::error::OK;
}

}  // namespace dingodb
