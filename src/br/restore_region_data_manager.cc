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

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "br/restore_region_data.h"
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
      id_and_sst_meta_group_kvs_(id_and_sst_meta_group_kvs),
      backup_meta_region_cf_name_(backup_meta_region_cf_name),
      group_belongs_to_whom_(group_belongs_to_whom),
      restore_region_timeout_s_(restore_region_timeout_s),
      id_and_region_kvs_(id_and_region_kvs) {
  bthread_mutex_init(&mutex_, nullptr);
}

RestoreRegionDataManager::~RestoreRegionDataManager() { bthread_mutex_destroy(&mutex_); }

std::shared_ptr<RestoreRegionDataManager> RestoreRegionDataManager::GetSelf() { return shared_from_this(); }

butil::Status RestoreRegionDataManager::Init() {
  butil::Status status;

  auto iter = id_and_sst_meta_group_kvs_->begin();
  while (iter != id_and_sst_meta_group_kvs_->end()) {
    sst_meta_groups_.push_back(iter->second);
    iter++;
  }

  return butil::Status::OK();
}

butil::Status RestoreRegionDataManager::Run() {
  butil::Status status;

  uint32_t concurrency = std::min(concurrency_, static_cast<uint32_t>(sst_meta_groups_.size()));

  // init thread_exit_flags_ set already exit
  thread_exit_flags_.resize(concurrency, 1);

  for (uint32_t i = 0; i < concurrency; i++) {
    // set thread running
    thread_exit_flags_[i] = 0;

    status = DoAsyncRestoreRegionData(i);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
      }
      // set thread exit
      thread_exit_flags_[i] = 1;
      continue;
    }
  }

  while (true) {
    if (is_need_exit_) {
      break;
    }

    if (already_restore_region_datas_ >= sst_meta_groups_.size()) {
      break;
    }

    // check thread create failed
    if (last_error_.error_code() != dingodb::pb::error::OK) {
      break;
    }

    sleep(1);
  }

  // check thread exit
  int64_t start_time_s = dingodb::Helper::Timestamp();
  int64_t end_time_s = start_time_s;
  while (true) {
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

  return last_error_;
}

butil::Status RestoreRegionDataManager::Finish() { return butil::Status::OK(); }

butil::Status RestoreRegionDataManager::DoAsyncRestoreRegionData(uint32_t thread_no) {
  std::shared_ptr<RestoreRegionDataManager> self = GetSelf();

  ServerInteractionPtr internal_coordinator_interaction;

  butil::Status status =
      ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), coordinator_interaction_);
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
        sst_meta_group = sst_meta_groups_.front();
        sst_meta_groups_.erase(sst_meta_groups_.begin());
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

}  // namespace br