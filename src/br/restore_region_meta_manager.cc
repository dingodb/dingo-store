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

#include <butil/strings/string_split.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "br/restore_region_meta.h"
#include "common/constant.h"
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

  if (id_and_region_kvs_) {
    auto iter = id_and_region_kvs_->begin();
    while (iter != id_and_region_kvs_->end()) {
      regions_.push_back(iter->second);
      iter++;
    }

    std::string s =
        fmt::format("backup_meta_region_name : {} regions size : {}", backup_meta_region_name_, regions_.size());

    s += " regions : [\n";

    for (size_t i = 0; i < regions_.size(); i++) {
      s += " " + std::to_string((regions_[i])->id());
      if ((i + 1) % 10 == 0) {
        s += "\n";
      }
    }
    s += "]";

    DINGO_LOG(INFO) << s;

    if (regions_.size() > 1) {
      std::reverse(regions_.begin(), regions_.end());
    }

    // std::mt19937 rng(std::random_device{}());
    // std::shuffle(regions_.begin(), regions_.end(), rng);

  }  // if (id_and_region_kvs_) {

  return butil::Status::OK();
}

butil::Status RestoreRegionMetaManager::Run() {
  butil::Status status;

  uint32_t concurrency = std::min(concurrency_, static_cast<uint32_t>(regions_.size()));
  int64_t regions_size = regions_.size();

  // init thread_exit_flags_ set already exit
  thread_exit_flags_.resize(concurrency, 1);

  for (uint32_t i = 0; i < concurrency; i++) {
    // set thread running
    {
      BAIDU_SCOPED_LOCK(mutex_);
      thread_exit_flags_[i] = 0;
    }

    status = DoAsyncRestoreRegionMeta(i);
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

  std::vector<std::string> backup_meta_region_names;
  FormatBackupMetaRegionName(backup_meta_region_names);
  PaddingBackupMetaRegionName(backup_meta_region_names);

  std::cerr << "Full Restore " << backup_meta_region_names[0] << " " << backup_meta_region_names[1] << " "
            << backup_meta_region_names[2] << " " << backup_meta_region_names[3] << " " << "<";
  DINGO_LOG(INFO) << "Full Restore " << backup_meta_region_names[0] << " " << backup_meta_region_names[1] << " "
                  << backup_meta_region_names[2] << " " << backup_meta_region_names[3] << " " << "<";

  while (true) {
    std::cerr << "-";
    DINGO_LOG(INFO) << "-";
    if (is_need_exit_) {
      break;
    }

    if (already_restore_region_metas_ >= regions_size) {
      break;
    }

    {
      BAIDU_SCOPED_LOCK(mutex_);
      // check thread create failed
      if (last_error_.error_code() != dingodb::pb::error::OK) {
        break;
      }
    }

    sleep(1);
  }

  // check thread exit
  int64_t wait_all_thread_exit_timeout_s = create_region_timeout_s_ + 5;
  int64_t start_time_s = dingodb::Helper::Timestamp();
  while (true) {
    int64_t end_time_s = dingodb::Helper::Timestamp();
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

  std::cerr << ">" << " 100.00%" << " [" << backup_meta_region_names[0][0] << ":" << already_restore_region_metas_
            << "]";
  DINGO_LOG(INFO) << ">" << " 100.00%" << " [" << backup_meta_region_names[0][0] << ":" << already_restore_region_metas_
                  << "]";

  std::cout << std::endl;

  return last_error_;
}

butil::Status RestoreRegionMetaManager::Finish() { return butil::Status::OK(); }

butil::Status RestoreRegionMetaManager::DoAsyncRestoreRegionMeta(uint32_t thread_no) {
  std::shared_ptr<RestoreRegionMetaManager> self = GetSelf();

  ServerInteractionPtr internal_coordinator_interaction;

  butil::Status status =
      ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), internal_coordinator_interaction);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  auto lambda_call = [self, internal_coordinator_interaction, thread_no]() {
    self->DoRestoreRegionInternal(internal_coordinator_interaction, thread_no);
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

butil::Status RestoreRegionMetaManager::DoRestoreRegionInternal(ServerInteractionPtr coordinator_interaction,
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
        // region = regions_.front();
        // regions_.erase(regions_.begin());
        region = regions_.back();
        regions_.pop_back();
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

butil::Status RestoreRegionMetaManager::FormatBackupMetaRegionName(std::vector<std::string>& backup_meta_region_names) {
  butil::Status status;

  if (backup_meta_region_name_ != dingodb::Constant::kStoreRegionSqlMetaSstName &&
      backup_meta_region_name_ != dingodb::Constant::kStoreRegionSdkDataSstName &&
      backup_meta_region_name_ != dingodb::Constant::kIndexRegionSdkDataSstName &&
      backup_meta_region_name_ != dingodb::Constant::kDocumentRegionSdkDataSstName &&
      backup_meta_region_name_ != dingodb::Constant::kStoreRegionSqlDataSstName &&
      backup_meta_region_name_ != dingodb::Constant::kIndexRegionSqlDataSstName &&
      backup_meta_region_name_ != dingodb::Constant::kDocumentRegionSqlDataSstName) {
    DINGO_LOG(WARNING) << "backup_meta_region_name_ is invalid : " << backup_meta_region_name_;
    return butil::Status::OK();
  }

  std::vector<std::string> parts1;
  butil::SplitString(backup_meta_region_name_, '.', &parts1);

  if (parts1.size() != 2) {
    DINGO_LOG(WARNING) << "backup_meta_region_name_ is invalid : " << backup_meta_region_name_;
    return butil::Status::OK();
  }

  std::vector<std::string> parts2;
  butil::SplitString(parts1[0], '_', &parts2);

  if (parts2.size() != 4) {
    DINGO_LOG(WARNING) << "backup_meta_region_name_ is invalid : " << backup_meta_region_name_;
    return butil::Status::OK();
  }

  // double check
  if (parts2[0] != "store" && parts2[0] != "index" && parts2[0] != "document") {
    DINGO_LOG(WARNING) << "backup_meta_region_name_ is invalid : " << backup_meta_region_name_;
    return butil::Status::OK();
  }

  if (parts2[1] != "region") {
    DINGO_LOG(WARNING) << "backup_meta_region_name_ is invalid : " << backup_meta_region_name_;
    return butil::Status::OK();
  }

  if (parts2[2] != "sql" && parts2[2] != "sdk") {
    DINGO_LOG(WARNING) << "backup_meta_region_name_ is invalid : " << backup_meta_region_name_;
    return butil::Status::OK();
  }

  if (parts2[3] != "data" && parts2[3] != "meta") {
    DINGO_LOG(WARNING) << "backup_meta_region_name_ is invalid : " << backup_meta_region_name_;
    return butil::Status::OK();
  }

  backup_meta_region_names = parts2;

  return butil::Status::OK();
}

butil::Status RestoreRegionMetaManager::PaddingBackupMetaRegionName(
    std::vector<std::string>& backup_meta_region_names) {
  if (backup_meta_region_names.empty()) {
    backup_meta_region_names.resize(4, "Unknow");
  } else {
    if (backup_meta_region_names[0] == std::string("store")) {
      backup_meta_region_names[0] = std::string("Store");
    } else if (backup_meta_region_names[0] == std::string("index")) {
      backup_meta_region_names[0] = std::string("Index");
    } else if (backup_meta_region_names[0] == std::string("document")) {
      backup_meta_region_names[0] = std::string("Document");
    } else {
      backup_meta_region_names[0] = std::string("Unknow");
    }

    if (backup_meta_region_names[1] == std::string("region")) {
      backup_meta_region_names[1] = std::string("Region");
    } else {
      backup_meta_region_names[1] = std::string("Unknow");
    }

    if (backup_meta_region_names[2] == std::string("sql")) {
      backup_meta_region_names[2] = std::string("Sql");
    } else if (backup_meta_region_names[2] == std::string("sdk")) {
      backup_meta_region_names[2] = std::string("Sdk");
    } else {
      backup_meta_region_names[2] = std::string("Unknow");
    }

    if (backup_meta_region_names[3] == std::string("meta")) {
      backup_meta_region_names[3] = std::string("Meta");
    } else if (backup_meta_region_names[3] == std::string("data")) {
      backup_meta_region_names[3] = std::string("Data");
    } else {
      backup_meta_region_names[3] = std::string("Unknow");
    }
  }

  return butil::Status::OK();
}

int64_t RestoreRegionMetaManager::GetRegions() { return already_restore_region_metas_; }

}  // namespace br