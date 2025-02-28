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

#include "br/restore_region_data_manager.h"

#include <butil/strings/string_split.h>

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "br/restore_region_data.h"
#include "common/constant.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace br {

#ifndef ENABLE_RESTORE_REGION_DATA_PTHREAD
#define ENABLE_RESTORE_REGION_DATA_PTHREAD
#endif

// #undef ENABLE_RESTORE_REGION_DATA_PTHREAD

RestoreRegionDataManager::RestoreRegionDataManager(
    ServerInteractionPtr coordinator_interaction, ServerInteractionPtr interaction, uint32_t concurrency,
    int64_t replica_num, const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
    const std::string& storage_internal,
    std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>
        id_and_sst_meta_group_kvs,
    const std::string& backup_meta_region_cf_name, const std::string& group_belongs_to_whom,
    int64_t restore_region_timeout_s,
    std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> id_and_region_kvs)
    : coordinator_interaction_(coordinator_interaction),
      interaction_(interaction),
      concurrency_(concurrency),
      replica_num_(replica_num),
      restorets_(restorets),
      restoretso_internal_(restoretso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      backup_meta_region_cf_name_(backup_meta_region_cf_name),
      group_belongs_to_whom_(group_belongs_to_whom),
      restore_region_timeout_s_(restore_region_timeout_s),
      id_and_region_kvs_(id_and_region_kvs),
      id_and_sst_meta_group_kvs_(id_and_sst_meta_group_kvs),
      is_need_exit_(false),
      already_restore_region_datas_(0) {
  bthread_mutex_init(&mutex_, nullptr);
}

RestoreRegionDataManager::~RestoreRegionDataManager() { bthread_mutex_destroy(&mutex_); }

std::shared_ptr<RestoreRegionDataManager> RestoreRegionDataManager::GetSelf() { return shared_from_this(); }

butil::Status RestoreRegionDataManager::Init() {
  butil::Status status;

  if (id_and_sst_meta_group_kvs_) {
    auto iter = id_and_sst_meta_group_kvs_->begin();
    while (iter != id_and_sst_meta_group_kvs_->end()) {
      sst_meta_groups_.push_back(iter->second);
      iter++;
    }

    std::string s = fmt::format("backup_meta_region_cf_name : {} regions size : {}", backup_meta_region_cf_name_,
                                sst_meta_groups_.size());

    s += " regions : \n";

    for (size_t i = 0; i < sst_meta_groups_.size(); i++) {
      s += "[" + std::to_string(i) + "] ";

      if (sst_meta_groups_[i]->backup_data_file_value_sst_metas_size() > 0) {
        auto region_id = sst_meta_groups_[i]->backup_data_file_value_sst_metas(0).region_id();
        s += std::to_string(region_id);
        for (size_t j = 0; j < sst_meta_groups_[i]->backup_data_file_value_sst_metas_size(); j++) {
          s += "-";
          s += sst_meta_groups_[i]->backup_data_file_value_sst_metas(j).cf();
        }
      } else {
        s += "empty";
      }
      s += "\n";
    }

    DINGO_LOG(INFO) << s;

    if (sst_meta_groups_.size() > 1) {
      std::reverse(sst_meta_groups_.begin(), sst_meta_groups_.end());
    }
  }  // if (id_and_sst_meta_group_kvs_)

  // double check
  if (id_and_sst_meta_group_kvs_ && !id_and_region_kvs_) {
    std::string s = "id_and_sst_meta_group_kvs_ is not null, but id_and_region_kvs_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  return butil::Status::OK();
}

butil::Status RestoreRegionDataManager::Run() {
  butil::Status status;

  uint32_t concurrency = std::min(concurrency_, static_cast<uint32_t>(sst_meta_groups_.size()));
  int64_t sst_meta_groups_size = sst_meta_groups_.size();

  // init thread_exit_flags_ set already exit
  thread_exit_flags_.resize(concurrency, 1);

  for (uint32_t i = 0; i < concurrency; i++) {
    // set thread running
    {
      BAIDU_SCOPED_LOCK(mutex_);
      thread_exit_flags_[i] = 0;
    }

    status = DoAsyncRestoreRegionData(i);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
        // set thread exit
        thread_exit_flags_[i] = 1;
      }
      continue;
    }
  }

  std::string s;
  std::atomic<int64_t> last_already_handle_regions = 0;
  std::vector<std::string> backup_meta_region_cf_names;
  FormatBackupMetaRegionCfName(backup_meta_region_cf_names);
  PaddingBackupMetaRegionCfName(backup_meta_region_cf_names);

  std::cerr << "Full Restore " << backup_meta_region_cf_names[0] << " " << backup_meta_region_cf_names[1] << " "
            << backup_meta_region_cf_names[4] << " " << backup_meta_region_cf_names[5] << " " << "<";
  DINGO_LOG(INFO) << "Full Restore " << backup_meta_region_cf_names[0] << " " << backup_meta_region_cf_names[1] << " "
                  << backup_meta_region_cf_names[4] << " " << backup_meta_region_cf_names[5] << " " << "<";

  while (true) {
    if (is_need_exit_) {
      break;
    }

    int64_t diff = already_restore_region_datas_ - last_already_handle_regions;
    for (int i = 0; i < diff; i++) {
      std::cerr << "-";
      s += "-";
    }

    if (already_restore_region_datas_ >= sst_meta_groups_size) {
      break;
    }

    {
      BAIDU_SCOPED_LOCK(mutex_);
      // check thread create failed
      if (last_error_.error_code() != dingodb::pb::error::OK) {
        break;
      }
    }

    last_already_handle_regions.store(already_restore_region_datas_);

    sleep(1);
  }

  // check thread exit
  int64_t start_time_s = dingodb::Helper::Timestamp();

  while (true) {
    int64_t end_time_s = dingodb::Helper::Timestamp();
    if ((end_time_s - start_time_s) > restore_region_timeout_s_) {
      DINGO_LOG(ERROR) << fmt::format("restore region data timeout : {}s", (end_time_s - start_time_s));
      break;
    }

    bool is_all_thread_exit = true;
    {
      BAIDU_SCOPED_LOCK(mutex_);
      for (uint32_t i = 0; i < concurrency; i++) {
        // thread is running
        if (thread_exit_flags_[i] == 0) {
          is_all_thread_exit = false;
          break;
        }
      }
    }

    if (is_all_thread_exit) {
      break;
    }

    sleep(1);
  }

  std::cerr << ">" << " 100.00%" << " [" << backup_meta_region_cf_names[0][0] << ":" << already_restore_region_datas_
            << "]";
  DINGO_LOG(INFO) << s;
  DINGO_LOG(INFO) << ">" << " 100.00%" << " [" << backup_meta_region_cf_names[0][0] << ":"
                  << already_restore_region_datas_ << "]";
  std::cout << std::endl;

  return last_error_;
}

butil::Status RestoreRegionDataManager::Finish() { return butil::Status::OK(); }

butil::Status RestoreRegionDataManager::DoAsyncRestoreRegionData(uint32_t thread_no) {
  std::shared_ptr<RestoreRegionDataManager> self = GetSelf();

  ServerInteractionPtr internal_coordinator_interaction;

  butil::Status status =
      ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), internal_coordinator_interaction);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  ServerInteractionPtr internal_interaction;

  status = ServerInteraction::CreateInteraction(interaction_->GetAddrs(), internal_interaction);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  auto lambda_call = [self, internal_coordinator_interaction, internal_interaction, thread_no]() {
    self->DoRestoreRegionDataInternal(internal_coordinator_interaction, internal_interaction, thread_no);
  };

#if defined(ENABLE_RESTORE_REGION_META_PTHREAD)
  std::thread th(lambda_call);
  th.detach();
#else

  std::function<void()>* call = new std::function<void()>;
  *call = lambda_call;
  bthread_t th;

  int ret = bthread_start_background(
      &th, nullptr,
      [](void* arg) -> void* {
        auto* call = static_cast<std::function<void()>*>(arg);
        (*call)();
        delete call;
        return nullptr;
      },
      call);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("bthread_start_background fail");
    return butil::Status(dingodb::pb::error::EINTERNAL, "bthread_start_background fail");
  }
#endif  // #if defined(ENABLE_RESTORE_REGION_META_PTHREAD)

  return butil::Status::OK();
}

butil::Status RestoreRegionDataManager::DoRestoreRegionDataInternal(ServerInteractionPtr coordinator_interaction,
                                                                    ServerInteractionPtr interaction,
                                                                    uint32_t thread_no) {
  butil::Status status;

  while (true) {
    if (is_need_exit_) {
      break;
    }

    std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup> sst_meta_group;
    {
      BAIDU_SCOPED_LOCK(mutex_);
      if (!sst_meta_groups_.empty()) {
        // sst_meta_group = sst_meta_groups_.front();
        // sst_meta_groups_.erase(sst_meta_groups_.begin());
        sst_meta_group = sst_meta_groups_.back();
        sst_meta_groups_.pop_back();
      } else {
        // empty regions. thread exit
        break;
      }
    }

    // check sst_meta_group empty. ignore
    if (0 == sst_meta_group->backup_data_file_value_sst_metas_size()) {
      already_restore_region_datas_++;
      continue;
    }

    int64_t region_id = sst_meta_group->backup_data_file_value_sst_metas(0).region_id();

    std::shared_ptr<dingodb::pb::common::Region> region;
    {
      auto iter = id_and_region_kvs_->find(region_id);
      if (iter == id_and_region_kvs_->end()) {
        std::string s = fmt::format("region id : {} not found", region_id);
        DINGO_LOG(ERROR) << s;
        is_need_exit_ = true;
        DINGO_LOG(ERROR) << status.error_cstr();
        {
          BAIDU_SCOPED_LOCK(mutex_);
          last_error_ = status;
        }
      }

      region = iter->second;
    }

    std::shared_ptr<RestoreRegionData> restore_region_data = std::make_shared<RestoreRegionData>(
        coordinator_interaction, interaction, region, restorets_, restoretso_internal_, storage_, storage_internal_,
        sst_meta_group, backup_meta_region_cf_name_, group_belongs_to_whom_, restore_region_timeout_s_);

    status = restore_region_data->Init();
    if (!status.ok()) {
      is_need_exit_ = true;
      DINGO_LOG(ERROR) << status.error_cstr();
      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
      }
      break;
    }

    status = restore_region_data->Run();
    if (!status.ok()) {
      is_need_exit_ = true;
      DINGO_LOG(ERROR) << status.error_cstr();
      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
      }
      break;
    }

    status = restore_region_data->Finish();
    if (!status.ok()) {
      is_need_exit_ = true;
      DINGO_LOG(ERROR) << status.error_cstr();
      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
      }
      break;
    }

    already_restore_region_datas_++;
  }

  // thread exit
  {
    BAIDU_SCOPED_LOCK(mutex_);
    thread_exit_flags_[thread_no] = 1;
  }

  return butil::Status::OK();
}

butil::Status RestoreRegionDataManager::FormatBackupMetaRegionCfName(
    std::vector<std::string>& backup_meta_region_cf_names) {
  if (backup_meta_region_cf_name_ != dingodb::Constant::kStoreCfSstMetaSqlMetaSstName &&
      backup_meta_region_cf_name_ != dingodb::Constant::kStoreCfSstMetaSdkDataSstName &&
      backup_meta_region_cf_name_ != dingodb::Constant::kIndexCfSstMetaSdkDataSstName &&
      backup_meta_region_cf_name_ != dingodb::Constant::kDocumentCfSstMetaSdkDataSstName &&
      backup_meta_region_cf_name_ != dingodb::Constant::kStoreCfSstMetaSqlDataSstName &&
      backup_meta_region_cf_name_ != dingodb::Constant::kIndexCfSstMetaSqlDataSstName &&
      backup_meta_region_cf_name_ != dingodb::Constant::kDocumentCfSstMetaSqlDataSstName) {
    DINGO_LOG(WARNING) << "backup_meta_region_cf_name_ is invalid : " << backup_meta_region_cf_name_;
    return butil::Status::OK();
  }

  std::vector<std::string> parts1;
  butil::SplitString(backup_meta_region_cf_name_, '.', &parts1);

  if (parts1.size() != 2) {
    DINGO_LOG(WARNING) << "backup_meta_region_cf_name_ is invalid : " << backup_meta_region_cf_name_;
    return butil::Status::OK();
  }

  std::vector<std::string> parts2;
  butil::SplitString(parts1[0], '_', &parts2);

  if (parts2.size() != 6) {
    DINGO_LOG(WARNING) << "backup_meta_region_cf_name_ is invalid : " << backup_meta_region_cf_name_;
    return butil::Status::OK();
  }

  // double check
  if (parts2[0] != "store" && parts2[0] != "index" && parts2[0] != "document") {
    DINGO_LOG(WARNING) << "backup_meta_region_cf_name_ is invalid : " << backup_meta_region_cf_name_;
    return butil::Status::OK();
  }

  if (parts2[1] != "cf") {
    DINGO_LOG(WARNING) << "backup_meta_region_cf_name_ is invalid : " << backup_meta_region_cf_name_;
    return butil::Status::OK();
  }

  if (parts2[2] != "sst") {
    DINGO_LOG(WARNING) << "backup_meta_region_cf_name_ is invalid : " << backup_meta_region_cf_name_;
    return butil::Status::OK();
  }

  if (parts2[3] != "meta") {
    DINGO_LOG(WARNING) << "backup_meta_region_cf_name_ is invalid : " << backup_meta_region_cf_name_;
    return butil::Status::OK();
  }

  if (parts2[4] != "sql" && parts2[4] != "sdk") {
    DINGO_LOG(WARNING) << "backup_meta_region_cf_name_ is invalid : " << backup_meta_region_cf_name_;
    return butil::Status::OK();
  }

  if (parts2[5] != "data" && parts2[5] != "meta") {
    DINGO_LOG(WARNING) << "backup_meta_region_cf_name_ is invalid : " << backup_meta_region_cf_name_;
    return butil::Status::OK();
  }

  backup_meta_region_cf_names = parts2;

  return butil::Status::OK();
}

butil::Status RestoreRegionDataManager::PaddingBackupMetaRegionCfName(
    std::vector<std::string>& backup_meta_region_cf_names) {
  if (backup_meta_region_cf_names.empty()) {
    backup_meta_region_cf_names.resize(6, "Unknow");
  } else {
    if (backup_meta_region_cf_names[0] == std::string("store")) {
      backup_meta_region_cf_names[0] = std::string("Store");
    } else if (backup_meta_region_cf_names[0] == std::string("index")) {
      backup_meta_region_cf_names[0] = std::string("Index");
    } else if (backup_meta_region_cf_names[0] == std::string("document")) {
      backup_meta_region_cf_names[0] = std::string("Document");
    } else {
      backup_meta_region_cf_names[0] = std::string("Unknow");
    }

    if (backup_meta_region_cf_names[1] == std::string("cf")) {
      backup_meta_region_cf_names[1] = std::string("Cf");
    } else {
      backup_meta_region_cf_names[1] = std::string("Unknow");
    }

    if (backup_meta_region_cf_names[2] == std::string("sst")) {
      backup_meta_region_cf_names[2] = std::string("Sst");
    } else {
      backup_meta_region_cf_names[2] = std::string("Unknow");
    }

    if (backup_meta_region_cf_names[3] == std::string("meta")) {
      backup_meta_region_cf_names[3] = std::string("Meta");
    } else {
      backup_meta_region_cf_names[3] = std::string("Unknow");
    }

    if (backup_meta_region_cf_names[4] == std::string("sql")) {
      backup_meta_region_cf_names[4] = std::string("Sql");
    } else if (backup_meta_region_cf_names[4] == std::string("sdk")) {
      backup_meta_region_cf_names[4] = std::string("Sdk");
    } else {
      backup_meta_region_cf_names[4] = std::string("Unknow");
    }

    if (backup_meta_region_cf_names[5] == std::string("meta")) {
      backup_meta_region_cf_names[5] = std::string("Meta");
    } else if (backup_meta_region_cf_names[5] == std::string("data")) {
      backup_meta_region_cf_names[5] = std::string("Data");
    } else {
      backup_meta_region_cf_names[5] = std::string("Unknow");
    }
  }

  return butil::Status::OK();
}

int64_t RestoreRegionDataManager::GetRegions() { return already_restore_region_datas_; }

}  // namespace br