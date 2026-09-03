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

#include "br/tool_client.h"

#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/parameter.h"
#include "br/tool_utils.h"
#include "br/utils.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/uuid.h"
#include "common/version.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

namespace br {

namespace {

// Send a single ControlConfig request to every addr of ONE service (the per-service
// inner loop behind CoreControlConfig). Errors do NOT abort the loop: each reachable
// node is still attempted (best-effort), and any failure is recorded into failed_addrs
// and reflected in return_status. The coordinator and the store/index/document services
// use different request/response proto types, so this is a template; both type arguments
// are spelled explicitly at the call site (request type first, then response type).
template <typename Request, typename Response>
void SendControlConfigToService(const std::vector<std::string>& addrs, const std::string& service_name,
                                const std::string& action, const Request& request,
                                std::vector<std::string>& failed_addrs, butil::Status& return_status) {
  int i = 0;
  for (const auto& addr : addrs) {
    Response response;
    std::shared_ptr<ServerInteraction> interaction;
    butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return_status = status;
      failed_addrs.push_back(addr);
      ++i;
      continue;
    }

    std::string name = fmt::format("[{}] {} {} {}", i++, service_name, action, addr);
    ToolUtils::PrintRequest(name, request);

    status = interaction->SendRequest(service_name, "ControlConfig", request, response);
    ToolUtils::PrintResponse(name, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return_status = status;
      failed_addrs.push_back(addr);
      continue;
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
      return_status = butil::Status(response.error().errcode(), response.error().errmsg());
      failed_addrs.push_back(addr);
      continue;
    }

    bool has_variable_error = false;
    for (const auto& variable : response.control_config_variable()) {
      if (!variable.is_error_occurred()) {
        continue;
      }

      std::string message =
          fmt::format("{} rejected {} on {} (current value: {}).", action, variable.name(), addr, variable.value());
      DINGO_LOG(ERROR) << message;
      return_status = butil::Status(dingodb::pb::error::EILLEGAL_PARAMTETERS, message);
      failed_addrs.push_back(addr);
      has_variable_error = true;
      break;
    }
    if (has_variable_error) {
      continue;
    }
  }
}

}  // namespace

ToolClient::ToolClient(ToolClientParams params) : tool_client_params_(params) {}

ToolClient::~ToolClient() = default;

std::shared_ptr<ToolClient> ToolClient::GetSelf() { return shared_from_this(); }

butil::Status ToolClient::Init() {
  butil::Status status;

  return butil::Status::OK();
}

butil::Status ToolClient::Run() {
  butil::Status status;

  if ("GcStart" == tool_client_params_.br_client_method) {
    status = GcStart();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("GcStop" == tool_client_params_.br_client_method) {
    status = GcStop();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("GetGCSafePoint" == tool_client_params_.br_client_method) {
    status = GetGCSafePoint();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("EnableBalance" == tool_client_params_.br_client_method) {
    status = EnableBalance();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("DisableBalance" == tool_client_params_.br_client_method) {
    status = DisableBalance();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("QueryBalance" == tool_client_params_.br_client_method) {
    status = QueryBalance();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("EnableSplitAndMerge" == tool_client_params_.br_client_method) {
    status = EnableSplitAndMerge();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("DisableSplitAndMerge" == tool_client_params_.br_client_method) {
    status = DisableSplitAndMerge();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("QuerySplitAndMerge" == tool_client_params_.br_client_method) {
    status = QuerySplitAndMerge();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("RemoteVersion" == tool_client_params_.br_client_method) {
    status = RemoteVersion();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("LocalVersion" == tool_client_params_.br_client_method) {
    status = LocalVersion();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("RegisterBackup" == tool_client_params_.br_client_method) {
    status = RegisterBackup();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("UnregisterBackup" == tool_client_params_.br_client_method) {
    status = UnregisterBackup(tool_client_params_.br_client_method_param1);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("RegisterBackupStatus" == tool_client_params_.br_client_method) {
    status = RegisterBackupStatus();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("RegisterRestore" == tool_client_params_.br_client_method) {
    status = RegisterRestore();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("UnregisterRestore" == tool_client_params_.br_client_method) {
    status = UnregisterRestore(tool_client_params_.br_client_method_param1);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("RegisterRestoreStatus" == tool_client_params_.br_client_method) {
    status = RegisterRestoreStatus();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("DisableRaftSync" == tool_client_params_.br_client_method) {
    status = DisableRaftSync();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("EnableRaftSync" == tool_client_params_.br_client_method) {
    status = EnableRaftSync();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("QueryRaftSync" == tool_client_params_.br_client_method) {
    status = QueryRaftSync();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("DisableRaftMetaForceNoSync" == tool_client_params_.br_client_method) {
    status = DisableRaftMetaForceNoSync();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("EnableRaftMetaForceNoSync" == tool_client_params_.br_client_method) {
    status = EnableRaftMetaForceNoSync();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else if ("QueryRaftMetaForceNoSync" == tool_client_params_.br_client_method) {
    status = QueryRaftMetaForceNoSync();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    std::string s = fmt::format("tool client method not support. {}", tool_client_params_.br_client_method);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  return butil::Status::OK();
}

butil::Status ToolClient::Finish() {
  butil::Status status;

  return butil::Status::OK();
}

butil::Status ToolClient::GcStart() {
  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_START);

  ToolUtils::PrintRequest("GcStart", request);

  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  ToolUtils::PrintResponse("GcStart", response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status ToolClient::GcStop() {
  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_STOP);

  ToolUtils::PrintRequest("GcStop", request);

  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  ToolUtils::PrintResponse("GcStop", response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status ToolClient::GetGCSafePoint() {
  dingodb::pb::coordinator::GetGCSafePointRequest request;
  dingodb::pb::coordinator::GetGCSafePointResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_get_all_tenant(true);

  ToolUtils::PrintRequest("GetGCSafePoint", request);

  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetGCSafePoint", request, response);
  ToolUtils::PrintResponse("GetGCSafePoint", response);
  if (!response.gc_stop()) {
    std::cout << "gc_stop: " << (response.gc_stop() ? "true" : "false") << std::endl;
    DINGO_LOG(INFO) << "gc_stop: " << (response.gc_stop() ? "true" : "false");
  }
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get GC safe point, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to get GC safe point, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  DINGO_LOG(INFO) << "";
  DINGO_LOG(INFO) << "tenant id : " << dingodb::Constant::kDefaultTenantId << " safe point : " << response.safe_point()
                  << "(" << Utils::ConvertTsoToDateTime(response.safe_point()) << ")";
  std::cout << "tenant id : " << dingodb::Constant::kDefaultTenantId << " safe point : " << response.safe_point() << "("
            << Utils::ConvertTsoToDateTime(response.safe_point()) << ")" << std::endl;

  for (const auto& [id, safe_point] : response.tenant_safe_points()) {
    DINGO_LOG(INFO) << "tenant id : " << id << " safe point : " << safe_point << "("
                    << Utils::ConvertTsoToDateTime(safe_point) << ")";
    std::cout << "tenant id : " << id << " safe point : " << safe_point << "("
              << Utils::ConvertTsoToDateTime(safe_point) << ")" << std::endl;
  }

  DINGO_LOG(INFO) << "";

  DINGO_LOG(INFO) << "tenant id : " << dingodb::Constant::kDefaultTenantId
                  << " resolve lock safe point : " << response.resolve_lock_safe_point() << "("
                  << Utils::ConvertTsoToDateTime(response.resolve_lock_safe_point()) << ")";
  std::cout << "tenant id : " << dingodb::Constant::kDefaultTenantId
            << " resolve lock safe point : " << response.resolve_lock_safe_point() << "("
            << Utils::ConvertTsoToDateTime(response.resolve_lock_safe_point()) << ")" << std::endl;

  for (const auto& [id, safe_point] : response.tenant_resolve_lock_safe_points()) {
    DINGO_LOG(INFO) << "tenant id : " << id << " resolve lock safe point : " << safe_point << "("
                    << Utils::ConvertTsoToDateTime(safe_point) << ")";
    std::cout << "tenant id : " << id << " resolve lock safe point : " << safe_point << "("
              << Utils::ConvertTsoToDateTime(safe_point) << ")" << std::endl;
  }
  DINGO_LOG(INFO) << "";

  return butil::Status::OK();
}

butil::Status ToolClient::DisableBalance() { return CoreBalance("false", "DisableBalance"); }

butil::Status ToolClient::EnableBalance() { return CoreBalance("true", "EnableBalance"); }

butil::Status ToolClient::QueryBalance() { return CoreBalance("query", "QueryBalance"); }

butil::Status ToolClient::CoreBalance(const std::string& balance_type, const std::string& action) {
  butil::Status return_status;
  auto coordinator_interaction = br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  dingodb::pb::coordinator::ControlConfigRequest request;
  dingodb::pb::coordinator::ControlConfigResponse response;

  dingodb::pb::common::ControlConfigVariable config_balance_leader;
  config_balance_leader.set_name("FLAGS_enable_balance_leader");
  config_balance_leader.set_value(balance_type);
  request.mutable_control_config_variable()->Add(std::move(config_balance_leader));

  dingodb::pb::common::ControlConfigVariable config_balance_region;
  config_balance_region.set_name("FLAGS_enable_balance_region");
  config_balance_region.set_value(balance_type);
  request.mutable_control_config_variable()->Add(std::move(config_balance_region));

  std::vector<std::string> addrs = coordinator_interaction->GetAddrs();
  int i = 0;

  for (const auto& addr : addrs) {
    response.Clear();

    std::shared_ptr<ServerInteraction> interaction;
    butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return_status = status;
      continue;
    }

    std::string name = fmt::format("[{}] CoordinatorService {} {}", i++, action, addr);
    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

    ToolUtils::PrintRequest(name, request);

    status = interaction->SendRequest("CoordinatorService", "ControlConfig", request, response);
    ToolUtils::PrintResponse(name, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return_status = status;
      continue;
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
      return_status = butil::Status(response.error().errcode(), response.error().errmsg());
      continue;
    }
  }

  return return_status;
}

butil::Status ToolClient::DisableSplitAndMerge() { return CoreSplitAndMerge("false", "DisableSplitAndMerge"); }

butil::Status ToolClient::EnableSplitAndMerge() { return CoreSplitAndMerge("true", "EnableSplitAndMerge"); }

butil::Status ToolClient::QuerySplitAndMerge() { return CoreSplitAndMerge("query", "QuerySplitAndMerge"); }

butil::Status ToolClient::CoreSplitAndMerge(const std::string& type, const std::string& action) {
  butil::Status return_status;
  auto store_interaction = br::InteractionManager::GetInstance().GetStoreInteraction();
  auto index_interaction = br::InteractionManager::GetInstance().GetIndexInteraction();

  bool is_exist_store = (store_interaction != nullptr ? !store_interaction->IsEmpty() : false);
  bool is_exist_index = (index_interaction != nullptr ? !index_interaction->IsEmpty() : false);

  if (!is_exist_store && !is_exist_index) {
    DINGO_LOG(INFO) << "Store and Index not exist, skip DisableSplitAndMerge";
    std::cout << "Store and Index not exist, skip DisableSplitAndMerge" << std::endl;
    return butil::Status::OK();
  }

  dingodb::pb::store::ControlConfigRequest request;
  dingodb::pb::store::ControlConfigResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  dingodb::pb::common::ControlConfigVariable config_auto_split;
  config_auto_split.set_name("FLAGS_region_enable_auto_split");
  config_auto_split.set_value(type);
  request.mutable_control_config_variable()->Add(std::move(config_auto_split));

  dingodb::pb::common::ControlConfigVariable config_auto_merge;
  config_auto_merge.set_name("FLAGS_region_enable_auto_merge");
  config_auto_merge.set_value(type);
  request.mutable_control_config_variable()->Add(std::move(config_auto_merge));

  // store exist
  if (is_exist_store) {
    std::vector<std::string> addrs = store_interaction->GetAddrs();
    int i = 0;
    for (const auto& addr : addrs) {
      response.Clear();

      std::shared_ptr<ServerInteraction> interaction;
      butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return_status = status;
        continue;
      }

      std::string name = fmt::format("[{}] StoreService {} {}", i++, action, addr);

      ToolUtils::PrintRequest(name, request);

      status = interaction->SendRequest("StoreService", "ControlConfig", request, response);
      ToolUtils::PrintResponse(name, response);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return_status = status;
        continue;
      }

      if (response.error().errcode() != dingodb::pb::error::OK) {
        DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
        return_status = butil::Status(response.error().errcode(), response.error().errmsg());
        continue;
      }
    }
  }

  // index exist
  if (is_exist_index) {
    response.Clear();
    std::vector<std::string> addrs = index_interaction->GetAddrs();
    int i = 0;
    for (const auto& addr : addrs) {
      response.Clear();
      std::shared_ptr<ServerInteraction> interaction;

      butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return_status = status;
        continue;
      }

      std::string name = fmt::format("[{}] IndexService {} {}", i++, action, addr);

      ToolUtils::PrintRequest(name, request);

      status = interaction->SendRequest("IndexService", "ControlConfig", request, response);
      ToolUtils::PrintResponse(name, response);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return_status = status;
        continue;
      }

      if (response.error().errcode() != dingodb::pb::error::OK) {
        DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
        return_status = butil::Status(response.error().errcode(), response.error().errmsg());
        continue;
      }
    }
  }

  return return_status;
}

butil::Status ToolClient::RemoteVersion() {
  auto coordinator_interaction = br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_is_just_version_info(true);

  ToolUtils::PrintRequest("RemoteVersion", request);

  butil::Status status = coordinator_interaction->SendRequest("CoordinatorService", "Hello", request, response);
  ToolUtils::PrintResponse("RemoteVersion", response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  return butil::Status::OK();
}

butil::Status ToolClient::LocalVersion() {
  dingodb::pb::common::VersionInfo version_info_local = dingodb::GetVersionInfo();
  ToolUtils::PrintResponse("LocalVersion", version_info_local);
  return butil::Status::OK();
}

butil::Status ToolClient::RegisterBackup() {
  auto coordinator_interaction = br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  bool is_first = true;
  dingodb::pb::coordinator::RegisterBackupRequest request;
  dingodb::pb::coordinator::RegisterBackupResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  auto backup_task_id = dingodb::UUIDGenerator::GenerateUUID();

  std::string storage_internal = "dingodb_br tool client register backup";

  request.set_backup_name(backup_task_id);
  request.set_backup_path(storage_internal);
  int64_t current_now_s = dingodb::Helper::Timestamp();
  if (is_first) {
    request.set_backup_start_timestamp(current_now_s);
  }
  request.set_backup_current_timestamp(current_now_s);
  request.set_backup_timeout_s(FLAGS_backup_task_timeout_s);

  ToolUtils::PrintRequest("RegisterBackup", request);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "RegisterBackup", request, response);
  ToolUtils::PrintResponse("RegisterBackup", response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set RegisterBackup, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set RegisterBackup, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status ToolClient::UnregisterBackup(const std::string& backup_task_id) {
  auto coordinator_interaction = br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  dingodb::pb::coordinator::UnRegisterBackupRequest request;
  dingodb::pb::coordinator::UnRegisterBackupResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_backup_name(backup_task_id);

  ToolUtils::PrintRequest("UnRegisterBackup", request);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "UnRegisterBackup", request, response);
  ToolUtils::PrintResponse("UnRegisterBackup", response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set UnRegisterBackup, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set UnRegisterBackup, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status ToolClient::RegisterBackupStatus() {
  auto coordinator_interaction = br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  dingodb::pb::coordinator::RegisterBackupStatusRequest request;
  dingodb::pb::coordinator::RegisterBackupStatusResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  ToolUtils::PrintRequest("RegisterBackupStatus", request);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "RegisterBackupStatus", request, response);
  ToolUtils::PrintResponse("RegisterBackupStatus", response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set RegisterBackupStatus, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set RegisterBackupStatus, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status ToolClient::RegisterRestore() {
  auto coordinator_interaction = br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  bool is_first = true;
  dingodb::pb::coordinator::RegisterRestoreRequest request;
  dingodb::pb::coordinator::RegisterRestoreResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  auto restore_task_id = dingodb::UUIDGenerator::GenerateUUID();

  std::string storage_internal = "dingodb_br tool client restore backup";

  request.set_restore_name(restore_task_id);
  request.set_restore_path(storage_internal);
  int64_t current_now_s = dingodb::Helper::Timestamp();
  if (is_first) {
    request.set_restore_start_timestamp(current_now_s);
  }
  request.set_restore_current_timestamp(current_now_s);
  request.set_restore_timeout_s(FLAGS_restore_task_timeout_s);

  ToolUtils::PrintRequest("RegisterRestore", request);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "RegisterRestore", request, response);
  ToolUtils::PrintResponse("RegisterRestore", response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set RegisterRestore, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set RegisterRestore, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status ToolClient::UnregisterRestore(const std::string& restore_task_id) {
  auto coordinator_interaction = br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  dingodb::pb::coordinator::UnRegisterRestoreRequest request;
  dingodb::pb::coordinator::UnRegisterRestoreResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_restore_name(restore_task_id);

  ToolUtils::PrintRequest("UnRegisterRestore", request);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "UnRegisterRestore", request, response);
  ToolUtils::PrintResponse("UnRegisterRestore", response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set UnRegisterRestore, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set UnRegisterRestore, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status ToolClient::RegisterRestoreStatus() {
  auto coordinator_interaction = br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  dingodb::pb::coordinator::RegisterRestoreStatusRequest request;
  dingodb::pb::coordinator::RegisterRestoreStatusResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  ToolUtils::PrintRequest("RegisterRestoreStatus", request);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "RegisterRestoreStatus", request, response);
  ToolUtils::PrintResponse("RegisterRestoreStatus", response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set RegisterRestoreStatus, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set RegisterRestoreStatus, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status ToolClient::DisableRaftSync() { return CoreRaftSync("false", "DisableRaftSync"); }

butil::Status ToolClient::EnableRaftSync() { return CoreRaftSync("true", "EnableRaftSync"); }

butil::Status ToolClient::QueryRaftSync() { return CoreRaftSync("query", "QueryRaftSync"); }

butil::Status ToolClient::CoreRaftSync(const std::string& type, const std::string& action) {
  return CoreControlConfig("FLAGS_raft_sync", type, action);
}

butil::Status ToolClient::CoreControlConfig(const std::string& flag_name, const std::string& type,
                                            const std::string& action) {
  butil::Status return_status;
  auto coordinator_interaction = br::InteractionManager::GetInstance().GetCoordinatorInteraction();
  auto store_interaction = br::InteractionManager::GetInstance().GetStoreInteraction();
  auto index_interaction = br::InteractionManager::GetInstance().GetIndexInteraction();
  auto document_interaction = br::InteractionManager::GetInstance().GetDocumentInteraction();

  bool is_exist_coordinator = (coordinator_interaction != nullptr ? !coordinator_interaction->IsEmpty() : false);
  bool is_exist_store = (store_interaction != nullptr ? !store_interaction->IsEmpty() : false);
  bool is_exist_index = (index_interaction != nullptr ? !index_interaction->IsEmpty() : false);
  bool is_exist_document = (document_interaction != nullptr ? !document_interaction->IsEmpty() : false);

  if (!is_exist_coordinator && !is_exist_store && !is_exist_index && !is_exist_document) {
    std::string msg = fmt::format("Coordinator, Store, Index and Document not exist, skip {}", action);
    DINGO_LOG(INFO) << msg;
    std::cout << msg << std::endl;
    return butil::Status::OK();
  }

  dingodb::pb::store::ControlConfigRequest request;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  dingodb::pb::common::ControlConfigVariable config_variable;
  config_variable.set_name(flag_name);
  config_variable.set_value(type);
  request.mutable_control_config_variable()->Add(std::move(config_variable));

  std::vector<std::string> failed_addrs;

  // coordinator exist
  if (is_exist_coordinator) {
    dingodb::pb::coordinator::ControlConfigRequest coordinator_request;
    coordinator_request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    coordinator_request.mutable_control_config_variable()->Add()->CopyFrom(request.control_config_variable(0));
    SendControlConfigToService<dingodb::pb::coordinator::ControlConfigRequest,
                               dingodb::pb::coordinator::ControlConfigResponse>(
        coordinator_interaction->GetAddrs(), "CoordinatorService", action, coordinator_request, failed_addrs,
        return_status);
  }

  // store exist
  if (is_exist_store) {
    SendControlConfigToService<dingodb::pb::store::ControlConfigRequest, dingodb::pb::store::ControlConfigResponse>(
        store_interaction->GetAddrs(), "StoreService", action, request, failed_addrs, return_status);
  }

  // index exist
  if (is_exist_index) {
    SendControlConfigToService<dingodb::pb::store::ControlConfigRequest, dingodb::pb::store::ControlConfigResponse>(
        index_interaction->GetAddrs(), "IndexService", action, request, failed_addrs, return_status);
  }

  // document exist
  if (is_exist_document) {
    SendControlConfigToService<dingodb::pb::store::ControlConfigRequest, dingodb::pb::store::ControlConfigResponse>(
        document_interaction->GetAddrs(), "DocumentService", action, request, failed_addrs, return_status);
  }

  // A control-config change applied to only some nodes leaves the cluster in a
  // mixed state. Surface exactly which nodes failed (instead of silently keeping
  // only the last error) so the operator can re-apply there.
  if (!failed_addrs.empty()) {
    std::string joined;
    for (const auto& failed_addr : failed_addrs) {
      if (!joined.empty()) {
        joined += ", ";
      }
      joined += failed_addr;
    }
    std::string msg = fmt::format(
        "{} failed on {} node(s): {}. The cluster may be in an inconsistent state; re-run on the failed node(s).",
        action, failed_addrs.size(), joined);
    DINGO_LOG(ERROR) << msg;
    std::cout << msg << std::endl;
  } else {
    DINGO_LOG(INFO) << fmt::format("{} succeeded on all nodes.", action);
  }

  return return_status;
}

butil::Status ToolClient::DisableRaftMetaForceNoSync() {
  // Disabling raft_meta_force_no_sync restores normal (safe) fsync behaviour, so no confirmation needed.
  return CoreRaftMetaForceNoSync("false", "DisableRaftMetaForceNoSync");
}

butil::Status ToolClient::EnableRaftMetaForceNoSync() {
  // Enabling raft_meta_force_no_sync tells braft to STOP fsync-ing raft meta (vote records): writes
  // get faster, but a machine power failure can then lose unsynced votes. This is the DANGEROUS
  // direction, so require explicit operator acknowledgement before broadcasting it to the cluster.
  butil::Status status = ConfirmDangerous(
      "EnableRaftMetaForceNoSync",
      "This DISABLES fsync of raft meta (vote records) on every coordinator/store/index/document node.\n"
      "Writes become faster, but a machine power failure may then lose unsynced vote records.");
  if (!status.ok()) {
    return status;
  }
  return CoreRaftMetaForceNoSync("true", "EnableRaftMetaForceNoSync");
}

butil::Status ToolClient::QueryRaftMetaForceNoSync() {
  return CoreRaftMetaForceNoSync("query", "QueryRaftMetaForceNoSync");
}

butil::Status ToolClient::CoreRaftMetaForceNoSync(const std::string& type, const std::string& action) {
  return CoreControlConfig("FLAGS_raft_meta_force_no_sync", type, action);
}

butil::Status ToolClient::ConfirmDangerous(const std::string& action, const std::string& detail) {
  // Two ways to proceed with a dangerous operation:
  //   1. --confirm_dangerous on the command line (scripts / CI acknowledge up front), or
  //   2. an interactive TTY where the operator types an explicit confirmation.
  // Otherwise (piped stdin / CI without the flag) refuse with a non-OK status so automation cannot
  // silently weaken durability. A silent success here would be the worst outcome.
  if (FLAGS_confirm_dangerous) {
    DINGO_LOG(WARNING) << action << ": proceeding because --confirm_dangerous was set. " << detail;
    return butil::Status::OK();
  }

  if (!isatty(fileno(stdin))) {
    std::string msg = fmt::format(
        "{} is a DANGEROUS operation; it requires either an interactive TTY or --confirm_dangerous to "
        "acknowledge the risk. Refusing to proceed in non-interactive mode. {}",
        action, detail);
    DINGO_LOG(ERROR) << msg;
    std::cerr << "ERROR: " << msg << std::endl;
    return butil::Status(dingodb::pb::error::EILLEGAL_PARAMTETERS, msg);
  }

  std::cout << "============================================================\n"
            << "  WARNING: " << action << " (DANGEROUS)\n"
            << "============================================================\n"
            << detail << "\n\n"
            << "Type 'YES' or 'Yes' or 'yes' or 'Y' or 'y' to confirm, anything else to abort:\n"
            << "> " << std::flush;
  std::string confirmation_raw;
  std::getline(std::cin, confirmation_raw);
  std::string confirmation = dingodb::Helper::Trim(confirmation_raw, " \t\r\n");
  if (confirmation != "YES" && confirmation != "Yes" && confirmation != "yes" && confirmation != "Y" &&
      confirmation != "y") {
    std::string msg = fmt::format("{} aborted by user (entered: '{}')", action, confirmation_raw);
    DINGO_LOG(WARNING) << msg;
    std::cerr << msg << std::endl;
    return butil::Status(dingodb::pb::error::EILLEGAL_PARAMTETERS, msg);
  }
  return butil::Status::OK();
}

}  // namespace br
