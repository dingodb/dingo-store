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

#ifndef DINGODB_DISKANN_DISKANN_UTILS_H_  // NOLINT
#define DINGODB_DISKANN_DISKANN_UTILS_H_

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <string>

#include "butil/status.h"
#include "proto/common.pb.h"

namespace dingodb {

enum class DiskANNCoreState : char {
  kUnknown = 0,
  kImporting = 1,
  kImported = 2,
  kUninitialized = 3,
  kInitialized = 4,
  kBuilding = 5,
  kBuilded = 6,
  kUpdatingPath = 7,
  kUpdatedPath = 8,
  kLoading = 9,
  kLoaded = 10,
  kSearching = 11,
  kSearched = 12,
  kReseting = 13,
  kReset = 14,
  kDestroying = 15,
  kDestroyed = 16,
  kIdle = 17,
  kFailed = 18,
  kFakeBuilded = 19,
  kNoData = 20,
};

class DiskANNUtils {
 public:
  DiskANNUtils() = delete;
  ~DiskANNUtils() = delete;
  DiskANNUtils(const DiskANNUtils& rhs) = delete;
  DiskANNUtils& operator=(const DiskANNUtils& rhs) = delete;
  DiskANNUtils(DiskANNUtils&& rhs) = delete;
  DiskANNUtils& operator=(DiskANNUtils&& rhs) = delete;

  static butil::Status FileExistsAndRegular(const std::string& file_path);
  static butil::Status DirExists(const std::string& dir_path);
  static butil::Status ClearDir(const std::string& dir_path);
  static butil::Status DiskANNIndexPathPrefixExists(const std::string& dir_path, bool& build_with_mem_index,
                                                    pb::common::MetricType metric_type);
  static butil::Status RemoveFile(const std::string& file_path);
  static butil::Status RemoveDir(const std::string& dir_path);
  static butil::Status Rename(const std::string& old_path, const std::string& new_path);
  static std::string DiskANNCoreStateToString(DiskANNCoreState state);
  static std::string DiskANNCoreStateToString(const std::atomic<DiskANNCoreState>& state);
  static butil::Status CreateDir(const std::string& dir_path);
  static pb::common::DiskANNCoreState DiskANNCoreStateToPb(DiskANNCoreState state);
  static DiskANNCoreState DiskANNCoreStateFromPb(pb::common::DiskANNCoreState state);
  static int64_t GetAioRelated(const std::string& path);
  static int64_t GetAioNr();
  static int64_t GetAioMaxNr();
  static void OutputAioRelatedInformation(uint32_t num_threads, uint32_t max_event);

 protected:
 private:
  static butil::Status RemoveAllDir(const std::string& dir_path, bool remove_self);
  static butil::Status RenameDir(const std::string& old_dir_path, const std::string& new_dir_path);
  static butil::Status RenameFile(const std::string& old_file_path, const std::string& new_file_path);
};

}  // namespace dingodb

#endif  // DINGODB_DISKANN_DISKANN_UTILS_H_  // NOLINT
