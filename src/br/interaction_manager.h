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

#ifndef DINGODB_BR_INTERATION_MANAGER_H_
#define DINGODB_BR_INTERATION_MANAGER_H_

#include "br/interation.h"
#include "br/router.h"

namespace br {
class InteractionManager {
 public:
  static InteractionManager& GetInstance();

  void SetCoordinatorInteraction(ServerInteractionPtr interaction);
  void SetStoreInteraction(ServerInteractionPtr interaction);
  void SetIndexInteraction(ServerInteractionPtr interaction);
  void SetDocumentInteraction(ServerInteractionPtr interaction);

  ServerInteractionPtr GetCoordinatorInteraction() const;
  ServerInteractionPtr GetStoreInteraction() const;
  ServerInteractionPtr GetIndexInteraction() const;
  ServerInteractionPtr GetDocumentInteraction() const;

  bool CreateStoreInteraction(std::vector<std::string> addrs);
  butil::Status CreateStoreInteraction(int64_t region_id);

  bool CreateIndexInteraction(std::vector<std::string> addrs);
  butil::Status CreateIndexInteraction(int64_t region_id);

  bool CreateDocumentInteraction(std::vector<std::string> addrs);
  butil::Status CreateDocumentInteraction(int64_t region_id);

  void ResetCoordinatorInteraction() { coordinator_interaction_.reset(); }
  void ResetStoreInteraction() { store_interaction_.reset(); }
  void ResetIndexInteraction() { index_interaction_.reset(); }
  void ResetDocumentInteraction() { document_interaction_.reset(); }

  int64_t GetCoordinatorInteractionLatency() const;
  int64_t GetStoreInteractionLatency() const;
  int64_t GetIndexInteractionLatency() const;
  int64_t GetDocumentInteractionLatency() const;

  template <typename Request, typename Response>
  butil::Status SendRequestWithoutContext(const std::string& service_name, const std::string& api_name,
                                          const Request& request, Response& response);

  template <typename Request, typename Response>
  butil::Status SendRequestWithContext(const std::string& service_name, const std::string& api_name, Request& request,
                                       Response& response);

  template <typename Request, typename Response>
  butil::Status AllSendRequestWithoutContext(const std::string& service_name, const std::string& api_name,
                                             const Request& request, Response& response);

  template <typename Request, typename Response>
  butil::Status AllSendRequestWithContext(const std::string& service_name, const std::string& api_name,
                                          const Request& request, Response& response);

 private:
  InteractionManager();
  ~InteractionManager();

  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  ServerInteractionPtr index_interaction_;
  ServerInteractionPtr document_interaction_;

  bthread_mutex_t mutex_;
};

template <typename Request, typename Response>
butil::Status InteractionManager::SendRequestWithoutContext(const std::string& service_name,
                                                            const std::string& api_name, const Request& request,
                                                            Response& response) {
  if (service_name == "UtilService" || service_name == "DebugService") {
    if (store_interaction_ == nullptr) {
      DINGO_LOG(ERROR) << "Store interaction is nullptr.";
      return butil::Status(dingodb::pb::error::EINTERNAL, "Store interaction is nullptr.");
    }
    return store_interaction_->SendRequest(service_name, api_name, request, response);
  }
  return coordinator_interaction_->SendRequest(service_name, api_name, request, response);

  



}

template <typename Request, typename Response>
butil::Status InteractionManager::SendRequestWithContext(const std::string& service_name, const std::string& api_name,
                                                         Request& request, Response& response) {
  if (store_interaction_ == nullptr) {
    auto status = CreateStoreInteraction(request.context().region_id());
    if (!status.ok()) {
      return status;
    }
  }

  for (;;) {
    auto status = store_interaction_->SendRequest(service_name, api_name, request, response);
    if (status.ok()) {
      return status;
    }

    if (response.error().errcode() == dingodb::pb::error::EREGION_VERSION) {
      RegionRouter::GetInstance().UpdateRegionEntry(response.error().store_region_info());
      DINGO_LOG(INFO) << "QueryRegionEntry region_id: " << request.context().region_id();
      auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(request.context().region_id());
      if (region_entry == nullptr) {
        return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, "Not found region %lu",
                             request.context().region_id());
      }
      *request.mutable_context() = region_entry->GenConext();
    } else {
      return status;
    }
    bthread_usleep(1000 * 500);
  }
}

template <typename Request, typename Response>
butil::Status InteractionManager::AllSendRequestWithoutContext(const std::string& service_name,
                                                               const std::string& api_name, const Request& request,
                                                               Response& response) {
  if (store_interaction_ == nullptr) {
    return butil::Status(dingodb::pb::error::EINTERNAL, "Store interaction is nullptr.");
  }

  return store_interaction_->AllSendRequest(service_name, api_name, request, response);
}

template <typename Request, typename Response>
butil::Status InteractionManager::AllSendRequestWithContext(const std::string& service_name,
                                                            const std::string& api_name, const Request& request,
                                                            Response& response) {
  if (store_interaction_ == nullptr) {
    auto status = CreateStoreInteraction(request.context().region_id());
    if (!status.ok()) {
      return status;
    }
  }

  for (;;) {
    auto status = store_interaction_->AllSendRequest(service_name, api_name, request, response);
    if (status.ok()) {
      return status;
    }

    if (response.error().errcode() == dingodb::pb::error::EREGION_VERSION) {
      RegionRouter::GetInstance().UpdateRegionEntry(response.error().store_region_info());
      auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(request.context().region_id());
      if (region_entry == nullptr) {
        return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, "Not found region %lu",
                             request.context().region_id());
      }
      *request.mutable_context() = region_entry->GenConext();
    } else {
      return status;
    }
    bthread_usleep(1000 * 500);
  }
}
}  // namespace br

#endif  // DINGODB_BR_INTERATION_MANAGER_H_