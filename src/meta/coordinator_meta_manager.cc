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

#include "meta/coordinator_meta_manager.h"

#include "glog/logging.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

CoordinatorServerMeta::CoordinatorServerMeta() : store_(std::make_shared<pb::common::Coordinator>()) {}

bool CoordinatorServerMeta::Init() {
  auto* server = Server::GetInstance();
  store_->set_id(server->Id());
  LOG(INFO) << "store server meta: " << store_->ShortDebugString();

  return true;
}

uint64_t CoordinatorServerMeta::GetEpoch() const { return epoch_; }

CoordinatorServerMeta& CoordinatorServerMeta::SetEpoch(uint64_t epoch) {
  epoch_ = epoch;
  return *this;
}

CoordinatorServerMeta& CoordinatorServerMeta::SetId(uint64_t id) {
  store_->set_id(id);
  return *this;
}

CoordinatorServerMeta& CoordinatorServerMeta::SetState(pb::common::CoordinatorState state) {
  store_->set_state(state);
  return *this;
}

CoordinatorServerMeta& CoordinatorServerMeta::SetServerLocation(const butil::EndPoint&& endpoint) {
  //   auto* location = store_->mutable_server_location();
  //   location->set_host(butil::ip2str(endpoint.ip).c_str());
  //   location->set_port(endpoint.port);
  return *this;
}

CoordinatorServerMeta& CoordinatorServerMeta::SetRaftLocation(const butil::EndPoint&& endpoint) {
  //   auto* location = store_->mutable_raft_location();
  //   location->set_host(butil::ip2str(endpoint.ip).c_str());
  //   location->set_port(endpoint.port);
  return *this;
}

std::shared_ptr<pb::common::Coordinator> CoordinatorServerMeta::GetCoordinator() { return store_; }

CoordinatorMetaManager::CoordinatorMetaManager(std::shared_ptr<MetaReader> meta_reader,
                                               std::shared_ptr<MetaWriter> meta_writer)
    : meta_reader_(meta_reader),
      meta_writer_(meta_writer),
      server_meta_(std::make_unique<CoordinatorServerMeta>()),
      region_meta_(std::make_unique<CoordinatorMap<int>>()) {}

bool CoordinatorMetaManager::Init() {
  if (!server_meta_->Init()) {
    LOG(ERROR) << "Init store server meta failed!";
    return false;
  }

  if (!region_meta_->Init()) {
    LOG(ERROR) << "Init store region meta failed!";
    return false;
  }

  return true;
}

bool CoordinatorMetaManager::Recover() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(region_meta_->Prefix(), kvs)) {
    return false;
  }

  if (!region_meta_->Recover(kvs)) {
    return false;
  }

  return true;
}

std::shared_ptr<pb::common::Coordinator> CoordinatorMetaManager::GetCoordinatorServerMeta() {
  return server_meta_->GetCoordinator();
}

bool CoordinatorMetaManager::IsExistRegion(uint64_t region_id) { return region_meta_->IsExist(region_id); }

std::shared_ptr<pb::common::Region> CoordinatorMetaManager::GetRegion(uint64_t region_id) {
  //   return region_meta_->GetRegion(region_id);
}

std::map<uint64_t, std::shared_ptr<pb::common::Region>> CoordinatorMetaManager::GetAllRegion() {
  //   return region_meta_->GetAllRegion();
}

void CoordinatorMetaManager::AddRegion(const std::shared_ptr<pb::common::Region> region) {
  LOG(INFO) << "CoordinatorMeta add region, region_id " << region->id();
  //   region_meta_->AddRegion(region);
  //   meta_writer_->Put(region_meta_->TransformToKv(region));
}

void CoordinatorMetaManager::DeleteRegion(uint64_t region_id) {
  //   region_meta_->DeleteRegion(region_id);
  //   meta_writer_->Delete(region_meta_->GenKey(region_id));
}

}  // namespace dingodb