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

#include "br/restore_region_meta_manager.h"

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "br/restore_region_meta.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace br {

#ifndef ENABLE_RESTORE_REGION_META_PTHREAD
#define ENABLE_RESTORE_REGION_META_PTHREAD
#endif

// #undef ENABLE_RESTORE_REGION_META_PTHREAD

RestoreRegionMetaManager::RestoreRegionMetaManager(
    ServerInteractionPtr coordinator_interaction, uint32_t concurrency, int64_t replica_num,
    const std::string& storage_internal,
    std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> id_and_region_kvs,
    const std::string& backup_meta_region_name, int64_t create_region_timeout_s)
    : coordinator_interaction_(coordinator_interaction),
      concurrency_(concurrency),
      replica_num_(replica_num),
      storage_internal_(storage_internal),
      id_and_region_kvs_(id_and_region_kvs),
      backup_meta_region_name_(backup_meta_region_name),
      create_region_timeout_s_(create_region_timeout_s),
      is_need_exit_(false),
      already_restore_region_metas_(0) {
  bthread_mutex_init(&mutex_, nullptr);
}

RestoreRegionMetaManager::~RestoreRegionMetaManager() { bthread_mutex_destroy(&mutex_); }

std::shared_ptr<RestoreRegionMetaManager> RestoreRegionMetaManager::GetSelf() { return shared_from_this(); }

butil::Status RestoreRegionMetaManager::Init() {
  butil::Status status;

  auto iter = id_and_region_kvs_->begin();
  while (iter != id_and_region_kvs_->end()) {
    regions_.push_back(iter->second);
    iter++;
  }

  return butil::Status::OK();
}

butil::Status RestoreRegionMetaManager::Run() {
  butil::Status status;

  uint32_t concurrency = std::min(concurrency_, static_cast<uint32_t>(regions_.size()));

  // init thread_exit_flags_ set already exit
  thread_exit_flags_.resize(concurrency, 1);

  for (uint32_t i = 0; i < concurrency; i++) {
    // set thread running
    thread_exit_flags_[i] = 0;

    status = DoAsyncRestoreRegionMeta(i);
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

    if (already_restore_region_metas_ >= regions_.size()) {
      break;
    }

    // check thread create failed
    if (last_error_.error_code() != dingodb::pb::error::OK) {
      break;
    }

    sleep(1);
  }

  // check thread exit
  int64_t wait_all_thread_exit_timeout_s = create_region_timeout_s_ + 5;
  int64_t start_time_s = dingodb::Helper::Timestamp();
  int64_t end_time_s = start_time_s;
  while (true) {
    if ((end_time_s - start_time_s) > wait_all_thread_exit_timeout_s) {
      DINGO_LOG(ERROR) << fmt::format("restore region meta timeout : {}s. force exit !!!", (end_time_s - start_time_s));
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

butil::Status RestoreRegionMetaManager::Finish() { return butil::Status::OK(); }

butil::Status RestoreRegionMetaManager::DoAsyncRestoreRegionMeta(uint32_t thread_no) {
  std::shared_ptr<RestoreRegionMetaManager> self = GetSelf();

  ServerInteractionPtr internal_coordinator_interaction;

  butil::Status status =
      ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), coordinator_interaction_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  auto lambda_call = [self, internal_coordinator_interaction, thread_no]() {
    self->DoBackupRegionInternal(internal_coordinator_interaction, thread_no);
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

butil::Status RestoreRegionMetaManager::DoBackupRegionInternal(ServerInteractionPtr coordinator_interaction,
                                                               uint32_t thread_no) {
  butil::Status status;

  while (true) {
    if (is_need_exit_) {
      break;
    }

    std::shared_ptr<dingodb::pb::common::Region> region;
    {
      BAIDU_SCOPED_LOCK(mutex_);
      if (!regions_.empty()) {
        region = regions_.front();
        regions_.erase(regions_.begin());
      } else {
        // empty regions. thread exit
        break;
      }
    }

    std::shared_ptr<RestoreRegionMeta> restore_region_meta = std::make_shared<RestoreRegionMeta>(
        coordinator_interaction, region, replica_num_, backup_meta_region_name_, create_region_timeout_s_);

    status = restore_region_meta->Init();
    if (!status.ok()) {
      is_need_exit_ = true;
      DINGO_LOG(ERROR) << status.error_cstr();
      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
      }
      break;
    }

    status = restore_region_meta->Run();
    if (!status.ok()) {
      is_need_exit_ = true;
      DINGO_LOG(ERROR) << status.error_cstr();
      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
      }
      break;
    }

    status = restore_region_meta->Finish();
    if (!status.ok()) {
      is_need_exit_ = true;
      DINGO_LOG(ERROR) << status.error_cstr();
      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
      }
      break;
    }

    already_restore_region_metas_++;
  }

  // thread exit
  {
    BAIDU_SCOPED_LOCK(mutex_);
    thread_exit_flags_[thread_no] = 1;
  }

  return butil::Status::OK();
}

}  // namespace br