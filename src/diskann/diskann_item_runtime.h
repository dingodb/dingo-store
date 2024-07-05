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

#ifndef DINGODB_DISKANN_DISKANN_ITEM_RUNTIME_H_  // NOLINT
#define DINGODB_DISKANN_DISKANN_ITEM_RUNTIME_H_

#include <memory>

#include "common/runnable.h"
#include "config/config.h"

namespace dingodb {

class DiskANNItemRuntime {
 public:
  explicit DiskANNItemRuntime() = delete;

  ~DiskANNItemRuntime() = delete;

  static bool Init(std::shared_ptr<Config> config);
  static SimpleWorkerSetPtr &GetImportWorkerSet();
  static SimpleWorkerSetPtr &GetBuildWorkerSet();
  static SimpleWorkerSetPtr &GetLoadWorkerSet();
  static SimpleWorkerSetPtr &GetSearchWorkerSet();
  static SimpleWorkerSetPtr &GetMiscWorkerSet();
  static uint32_t GetNumBthreads();

 protected:
 private:
  static inline SimpleWorkerSetPtr import_worker_set;
  static inline SimpleWorkerSetPtr build_worker_set;
  static inline SimpleWorkerSetPtr load_worker_set;
  static inline SimpleWorkerSetPtr search_worker_set;
  static inline SimpleWorkerSetPtr misc_worker_set;
  static inline uint32_t num_bthreads = 0;
};

}  // namespace dingodb

#endif  // DINGODB_DISKANN_DISKANN_ITEM_RUNTIME_H_  // NOLINT
