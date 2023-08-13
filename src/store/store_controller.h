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

#ifndef DINGODB_STORE_STORE_CONTROL_H_
#define DINGODB_STORE_STORE_CONTROL_H_

#include <atomic>
#include <cstdint>
#include <memory>

#include "butil/macros.h"
#include "common/context.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "store/region_controller.h"

namespace dingodb {

class StoreController {
 public:
  StoreController() = default;
  ~StoreController() = default;

  StoreController(const StoreController &) = delete;
  const StoreController &operator=(const StoreController &) = delete;

  static bool Init() { return true; }
  void Destroy() {}
};

}  // namespace dingodb

#endif  // DINGODB_STORE_STORE_CONTROL_H_