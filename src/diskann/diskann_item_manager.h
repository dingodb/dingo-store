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

#ifndef DINGODB_DISKANN_DISKANN_ITEM_MANAGER_H_  // NOLINT
#define DINGODB_DISKANN_DISKANN_ITEM_MANAGER_H_

#include <sys/types.h>
#include <xmmintrin.h>

#include <cstdint>
#include <string>

#include "butil/status.h"
#include "common/synchronization.h"
#include "config/config.h"
#include "diskann/diskann_item.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

template <typename T>
class DiskANNItemManagerSingleton {
 public:
  static T& GetInstance() noexcept(std::is_nothrow_constructible_v<T>) {
    static T instance{};
    return instance;
  }
  virtual ~DiskANNItemManagerSingleton() = default;
  DiskANNItemManagerSingleton(const DiskANNItemManagerSingleton&) = delete;
  DiskANNItemManagerSingleton& operator=(const DiskANNItemManagerSingleton&) = delete;
  DiskANNItemManagerSingleton(DiskANNItemManagerSingleton&& rhs) = delete;
  DiskANNItemManagerSingleton& operator=(DiskANNItemManagerSingleton&& rhs) = delete;

 protected:
  DiskANNItemManagerSingleton() noexcept = default;
};

class DiskANNItemManager : public DiskANNItemManagerSingleton<DiskANNItemManager> {
 public:
  explicit DiskANNItemManager();

  ~DiskANNItemManager() override;

  bool Init(std::shared_ptr<Config> config);

  std::shared_ptr<DiskANNItem> Create(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                      const pb::common::VectorIndexParameter& vector_index_parameter);

  std::shared_ptr<DiskANNItem> Find(int64_t vector_index_id);

  void Delete(int64_t vector_index_id);

  std::vector<std::shared_ptr<DiskANNItem>> FindAll();

  static void SetSimdHookForDiskANN();

 protected:
 private:
  std::string path_;
  uint32_t num_threads_;
  float search_dram_budget_gb_;
  float build_dram_budget_gb_;
  int64_t import_timeout_s_;
  std::map<int64_t, std::shared_ptr<DiskANNItem>> items_;
  RWLock rw_lock_;
};

}  // namespace dingodb

#endif  // DINGODB_DISKANN_DISKANN_ITEM_MANAGER_H_  // NOLINT
