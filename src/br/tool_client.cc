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

#include <cstdint>
#include <memory>
#include <string>

#include "br/helper.h"
#include "br/interaction_manager.h"
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

}  // namespace br