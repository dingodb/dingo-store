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

#include "br/backup_sdk_data.h"

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <thread>

#include "br/utils.h"
#include "common/constant.h"
#include "common/helper.h"
#include "fmt/core.h"

namespace br {

#ifndef ENABLE_BACKUP_SDK_DATA_PTHREAD
#define ENABLE_BACKUP_SDK_DATA_PTHREAD
#endif

// #undef ENABLE_BACKUP_SDK_DATA_PTHREAD

BackupSdkData::BackupSdkData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                             ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                             const std::string& backupts, int64_t backuptso_internal, const std::string& storage,
                             const std::string& storage_internal)
    : BackupDataBase(coordinator_interaction, store_interaction, index_interaction, document_interaction, backupts,
                     backuptso_internal, storage, storage_internal, dingodb::Constant::kSdkData) {}

BackupSdkData::~BackupSdkData() = default;

std::shared_ptr<BackupSdkData> BackupSdkData::GetSelf() { return shared_from_this(); }

butil::Status BackupSdkData::Filter() {
  if (!wait_for_handle_store_regions_) {
    wait_for_handle_store_regions_ = std::make_shared<std::vector<dingodb::pb::common::Region>>();
  }

  if (!wait_for_handle_index_regions_) {
    wait_for_handle_index_regions_ = std::make_shared<std::vector<dingodb::pb::common::Region>>();
  }

  if (!wait_for_handle_document_regions_) {
    wait_for_handle_document_regions_ = std::make_shared<std::vector<dingodb::pb::common::Region>>();
  }

  for (const auto& region : region_map_->regions()) {
    // only handle sdk non txn region
    if (dingodb::Helper::IsClientRaw(region.definition().range().start_key())) {
      if (dingodb::pb::common::RegionType::STORE_REGION == region.region_type()) {
        wait_for_handle_store_regions_->push_back(region);
      } else if (dingodb::pb::common::RegionType::INDEX_REGION == region.region_type()) {
        wait_for_handle_index_regions_->push_back(region);
      } else if (dingodb::pb::common::RegionType::DOCUMENT_REGION == region.region_type()) {
        wait_for_handle_document_regions_->push_back(region);
      }
    }

    if (FLAGS_br_backup_enable_sdk_txn_region_backup) {
      // only handle sdk txn region for dingo-fs
      if (dingodb::Helper::IsClientTxn(region.definition().range().start_key())) {
        if (dingodb::pb::common::RegionType::STORE_REGION == region.region_type()) {
          wait_for_handle_store_regions_->push_back(region);
        } else if (dingodb::pb::common::RegionType::INDEX_REGION == region.region_type()) {
          wait_for_handle_index_regions_->push_back(region);
        } else if (dingodb::pb::common::RegionType::DOCUMENT_REGION == region.region_type()) {
          wait_for_handle_document_regions_->push_back(region);
        }
      }
    }
  }

  if (FLAGS_br_log_switch_backup_detail) {
    DINGO_LOG(INFO) << "sdk data : wait_for_handle_store_regions size = " << wait_for_handle_store_regions_->size();
    int i = 0;
    std::string s;
    for (const auto& region : *wait_for_handle_store_regions_) {
      if (0 != i++) {
        s += ", ";
      }
      s += std::to_string(region.id());
      if (i == 10) {
        DINGO_LOG(INFO) << "sdk data : wait_for_handle_store_regions region id=[" << s << "]";
        s.clear();
        i = 0;
      }
    }

    if (!s.empty()) {
      DINGO_LOG(INFO) << "sdk data : wait_for_handle_store_regions region id=[" << s << "]";
    }

    s.clear();
    i = 0;

    DINGO_LOG(INFO) << "sdk data : wait_for_handle_index_regions size = " << wait_for_handle_index_regions_->size();
    for (const auto& region : *wait_for_handle_index_regions_) {
      if (0 != i++) {
        s += ", ";
      }
      s += std::to_string(region.id());
      if (i == 10) {
        DINGO_LOG(INFO) << "sdk data : wait_for_handle_index_regions region id=[" << s << "]";
        s.clear();
        i = 0;
      }
    }

    if (!s.empty()) {
      DINGO_LOG(INFO) << "sdk data : wait_for_handle_index_regions region id=[" << s << "]";
    }

    s.clear();
    i = 0;

    DINGO_LOG(INFO) << "sdk data : wait_for_handle_document_regions size = "
                    << wait_for_handle_document_regions_->size();
    for (const auto& region : *wait_for_handle_document_regions_) {
      if (0 != i++) {
        s += ", ";
      }
      s += std::to_string(region.id());
      if (i == 10) {
        DINGO_LOG(INFO) << "sdk data : wait_for_handle_document_regions region id=[" << s << "]";
        s.clear();
        i = 0;
      }
    }
    if (!s.empty()) {
      DINGO_LOG(INFO) << "sdk data : wait_for_handle_document_regions region id=[" << s << "]";
    }
  }

  return butil::Status::OK();
}

butil::Status BackupSdkData::Run() {
  butil::Status status;
  if (!save_store_region_map_) {
    save_store_region_map_ =
        std::make_shared<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>>();
  }

  if (!save_index_region_map_) {
    save_index_region_map_ =
        std::make_shared<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>>();
  }

  if (!save_document_region_map_) {
    save_document_region_map_ =
        std::make_shared<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>>();
  }

  int64_t total_regions_count = wait_for_handle_store_regions_->size() + wait_for_handle_index_regions_->size() +
                                wait_for_handle_document_regions_->size();

  std::atomic<bool> store_is_thread_exit = false;
  std::atomic<bool> index_is_thread_exit = false;
  std::atomic<bool> document_is_thread_exit = false;

  // store
  if (store_interaction_ && !store_interaction_->IsEmpty()) {
    status = DoAsyncBackupRegion(store_interaction_, "StoreService", wait_for_handle_store_regions_,
                                 already_handle_store_regions_, save_store_region_map_, store_is_thread_exit);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    store_is_thread_exit = true;
  }

  // index
  if (index_interaction_ && !index_interaction_->IsEmpty()) {
    status = DoAsyncBackupRegion(index_interaction_, "IndexService", wait_for_handle_index_regions_,
                                 already_handle_index_regions_, save_index_region_map_, index_is_thread_exit);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    index_is_thread_exit = true;
  }

  // document
  if (document_interaction_ && !document_interaction_->IsEmpty()) {
    status = DoAsyncBackupRegion(document_interaction_, "DocumentService", wait_for_handle_document_regions_,
                                 already_handle_document_regions_, save_document_region_map_, document_is_thread_exit);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    document_is_thread_exit = true;
  }

  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::atomic<int64_t> last_already_handle_regions = 0;

  int64_t calc_start_time_ms = dingodb::Helper::TimestampMs();
  int64_t calc_end_time_ms = calc_start_time_ms;

  const std::string& progress_head = "Full Backup Sdk Data";

  auto lambda_output_progress_function = [total_regions_count, this, calc_start_time_ms, &calc_end_time_ms,
                                          progress_head]() {
    if (total_regions_count > 0) {
      calc_end_time_ms = dingodb::Helper::TimestampMs();
      int64_t elapsed_time_ms = calc_end_time_ms - calc_start_time_ms;
      std::string elapsed_time_str = Utils::FormatDurationFromMs(elapsed_time_ms);
      int64_t elapsed_time_per_region_ms =
          (already_handle_regions_.load() != 0) ? elapsed_time_ms / already_handle_regions_.load() : 0;

      std::string elapsed_time_per_region_str = Utils::FormatDurationFromMs(elapsed_time_per_region_ms);

      std::cout << "\r" << progress_head << " <" << already_handle_regions_ << "/" << total_regions_count << " "
                << elapsed_time_str << " " << elapsed_time_per_region_str << "/r" << "> " << std::fixed
                << std::setprecision(2)
                << static_cast<double>(already_handle_regions_.load()) / total_regions_count * 100 << "%" << " ["
                << "S:" << already_handle_store_regions_ << ",I:" << already_handle_index_regions_
                << ",D:" << already_handle_document_regions_ << "]" << std::flush;
    } else {
      std::cout << "\r" << progress_head << " <" << "0/0 0.00s 0.00ms/r" << "> " << " 100.00%" << " ["
                << "S:" << already_handle_store_regions_ << ",I:" << already_handle_index_regions_
                << ",D:" << already_handle_document_regions_ << "]" << std::flush;
    }
  };

  DINGO_LOG(INFO) << progress_head << " <";
  lambda_output_progress_function();

  std::string s;
  while (!is_need_exit_) {
    already_handle_regions_ =
        already_handle_store_regions_ + already_handle_index_regions_ + already_handle_document_regions_;

    int64_t diff = already_handle_regions_ - last_already_handle_regions;
    for (int i = 0; i < diff; i++) {
      // s += "-";
    }

    lambda_output_progress_function();

    if (already_handle_regions_ >= total_regions_count) {
      break;
    }

    last_already_handle_regions.store(already_handle_regions_);

    sleep(1);
  }

  // check thread exit
  int64_t start_time_s = dingodb::Helper::Timestamp();
  int64_t end_time_s = start_time_s;
  while (true) {
    if ((end_time_s - start_time_s) > (FLAGS_br_backup_region_timeout_ms / 1000 + 5)) {
      DINGO_LOG(ERROR) << fmt::format("backup sdk region data timeout : {}s", (end_time_s - start_time_s));
      break;
    }

    bool is_all_thread_exit = false;

    if (store_is_thread_exit && index_is_thread_exit && document_is_thread_exit) {
      is_all_thread_exit = true;
    }

    if (is_all_thread_exit) {
      break;
    }

    sleep(1);
  }

  if (is_need_exit_) {
    return last_error_;
  }

  lambda_output_progress_function();
  std::cout << std::endl;

  int64_t elapsed_time_ms = calc_end_time_ms - calc_start_time_ms;
  std::string elapsed_time_str = Utils::FormatDurationFromMs(elapsed_time_ms);
  int64_t elapsed_time_per_region_ms =
      (already_handle_regions_.load() != 0) ? elapsed_time_ms / already_handle_regions_.load() : 0;

  std::string elapsed_time_per_region_str = Utils::FormatDurationFromMs(elapsed_time_per_region_ms);

  DINGO_LOG(INFO) << already_handle_regions_ << "/" << total_regions_count << " " << elapsed_time_str << " "
                  << elapsed_time_per_region_str << "/r" << "> " << " 100.00%" << " ["
                  << "S:" << already_handle_store_regions_ << ",I:" << already_handle_index_regions_
                  << ",D:" << wait_for_handle_document_regions_->size() << "]";

  DINGO_LOG(INFO) << "backup sdk data  " << "total_regions : " << already_handle_regions_
                  << ", store_regions : " << already_handle_store_regions_
                  << ", index_regions : " << already_handle_index_regions_
                  << ", document_regions : " << already_handle_document_regions_;

  return butil::Status::OK();
}

butil::Status BackupSdkData::DoAsyncBackupRegion(
    ServerInteractionPtr interaction, const std::string& service_name,
    std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_regions,
    std::atomic<int64_t>& already_handle_regions,
    std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_region_map,
    std::atomic<bool>& is_thread_exit) {
  std::shared_ptr<BackupSdkData> self = GetSelf();
  auto lambda_call = [self, interaction, service_name, wait_for_handle_regions, &already_handle_regions,
                      save_region_map, &is_thread_exit]() {
    self->DoBackupRegionInternal(interaction, service_name, wait_for_handle_regions, already_handle_regions,
                                 save_region_map, is_thread_exit);
  };

#if defined(ENABLE_BACKUP_SDK_DATA_PTHREAD)
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
#endif  // #if defined(ENABLE_BACKUP_SDK_DATA_PTHREAD)

  return butil::Status::OK();
}

}  // namespace br