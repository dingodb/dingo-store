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

#include <memory>

#include "butil/macros.h"
#include "common/context.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class StoreControl {
 public:
  StoreControl(){};
  ~StoreControl(){};

  butil::Status AddRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region);
  void AddRegions(std::shared_ptr<Context> ctx, std::vector<std::shared_ptr<pb::common::Region> > regions);

  butil::Status ChangeRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region);

  butil::Status DeleteRegion(std::shared_ptr<Context> ctx, uint64_t region_id);

 private:
  DISALLOW_COPY_AND_ASSIGN(StoreControl);
};

}  // namespace dingodb

#endif  // DINGODB_STORE_STORE_CONTROL_H_