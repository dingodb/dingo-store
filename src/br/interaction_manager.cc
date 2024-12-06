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

#include "br/interaction_manager.h"

#include <memory>
#include <string>

namespace br {

InteractionManager::InteractionManager() { bthread_mutex_init(&mutex_, nullptr); }

InteractionManager::~InteractionManager() { bthread_mutex_destroy(&mutex_); }

InteractionManager& InteractionManager::GetInstance() {
  static InteractionManager instance;
  return instance;
}

void InteractionManager::SetCoordinatorInteraction(ServerInteractionPtr interaction) {
  coordinator_interaction_ = interaction;
}

void InteractionManager::SetStoreInteraction(ServerInteractionPtr interaction) { store_interaction_ = interaction; }

void InteractionManager::SetIndexInteraction(ServerInteractionPtr interaction) { index_interaction_ = interaction; }
void InteractionManager::SetDocumentInteraction(ServerInteractionPtr interaction) {
  document_interaction_ = interaction;
}

ServerInteractionPtr InteractionManager::GetCoordinatorInteraction() const { return coordinator_interaction_; }
ServerInteractionPtr InteractionManager::GetStoreInteraction() const { return store_interaction_; }
ServerInteractionPtr InteractionManager::GetIndexInteraction() const { return index_interaction_; }
ServerInteractionPtr InteractionManager::GetDocumentInteraction() const { return document_interaction_; }

bool InteractionManager::CreateStoreInteraction(std::vector<std::string> addrs) {
  auto interaction = std::make_shared<ServerInteraction>();
  if (!interaction->Init(addrs)) {
    DINGO_LOG(ERROR) << "Fail to init store_interaction";
    return false;
  }

  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (store_interaction_ == nullptr) {
      store_interaction_ = interaction;
    }
  }

  return true;
}

butil::Status InteractionManager::CreateStoreInteraction(int64_t region_id) {
  auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(region_id);
  if (region_entry == nullptr) {
    std::string s = fmt::format("not found store region entry {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, s);
  }

  if (dingodb::pb::common::RegionType::STORE_REGION != region_entry->Region().region_type()) {
    std::string s = fmt::format("region : {} is not a store region. region_type : {}", region_id,
                                dingodb::pb::common::RegionType_Name(region_entry->Region().region_type()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  if (!CreateStoreInteraction(region_entry->GetAddrs())) {
    std::string s = fmt::format("init store interaction failed, region {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  return butil::Status();
}

bool InteractionManager::CreateIndexInteraction(std::vector<std::string> addrs) {
  auto interaction = std::make_shared<ServerInteraction>();
  if (!interaction->Init(addrs)) {
    DINGO_LOG(ERROR) << "Fail to init index_interaction";
    return false;
  }

  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (index_interaction_ == nullptr) {
      index_interaction_ = interaction;
    }
  }

  return true;
}
butil::Status InteractionManager::CreateIndexInteraction(int64_t region_id) {
  auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(region_id);
  if (region_entry == nullptr) {
    std::string s = fmt::format("not found index region entry {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, s);
  }

  if (dingodb::pb::common::RegionType::INDEX_REGION != region_entry->Region().region_type()) {
    std::string s = fmt::format("region : {} is not a index region. region_type : {}", region_id,
                                dingodb::pb::common::RegionType_Name(region_entry->Region().region_type()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  if (!CreateIndexInteraction(region_entry->GetAddrs())) {
    std::string s = fmt::format("init index interaction failed, region {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  return butil::Status();
}

bool InteractionManager::CreateDocumentInteraction(std::vector<std::string> addrs) {
  auto interaction = std::make_shared<ServerInteraction>();
  if (!interaction->Init(addrs)) {
    DINGO_LOG(ERROR) << "Fail to init document_interaction_" << std::endl;
    return false;
  }

  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (document_interaction_ == nullptr) {
      document_interaction_ = interaction;
    }
  }

  return true;
}

butil::Status InteractionManager::CreateDocumentInteraction(int64_t region_id) {
  auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(region_id);
  if (region_entry == nullptr) {
    std::string s = fmt::format("not found document region entry {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, s);
  }

  if (dingodb::pb::common::RegionType::DOCUMENT_REGION != region_entry->Region().region_type()) {
    std::string s = fmt::format("region : {} is not a document region. region_type : {}", region_id,
                                dingodb::pb::common::RegionType_Name(region_entry->Region().region_type()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  if (!CreateDocumentInteraction(region_entry->GetAddrs())) {
    std::string s = fmt::format("init document interaction failed, region {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  return butil::Status();
}

int64_t InteractionManager::GetCoordinatorInteractionLatency() const {
  if (coordinator_interaction_ == nullptr) {
    return 0;
  }

  return coordinator_interaction_->GetLatency();
}

int64_t InteractionManager::GetStoreInteractionLatency() const {
  if (store_interaction_ == nullptr) {
    return 0;
  }

  return store_interaction_->GetLatency();
}

int64_t InteractionManager::GetIndexInteractionLatency() const {
  if (index_interaction_ == nullptr) {
    return 0;
  }

  return index_interaction_->GetLatency();
}

int64_t InteractionManager::GetDocumentInteractionLatency() const {
  if (document_interaction_ == nullptr) {
    return 0;
  }

  return document_interaction_->GetLatency();
}

}  // namespace br