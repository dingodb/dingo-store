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

#include "store/store_control.h"

#include "server/server.h"

namespace dingodb {

static bool ValidateAddRegion(std::shared_ptr<pb::common::Region> region) {
  return true;
}

void StoreControl::AddRegion(std::shared_ptr<pb::common::Region> region) {
  // valiate region
  if (!ValidateAddRegion(region)) {
    return;
  }

  // Add raft node
  auto engine = Server::GetInstance()->get_engine(pb::common::ENG_RAFTSTORE);
  if (engine == nullptr) {
    return;
  }
  engine->AddRegion(region);

  // Add region to store region meta manager
  Server::GetInstance()->get_store_meta_manager()->AddRegion(region);
}

void StoreControl::AddRegions(
    std::vector<std::shared_ptr<pb::common::Region> > regions) {
  for (auto region : regions) {
    AddRegion(region);
  }
}

}  // namespace dingodb