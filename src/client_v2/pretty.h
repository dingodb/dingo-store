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

#ifndef DINGODB_CLIENT_PRETTY_H_
#define DINGODB_CLIENT_PRETTY_H_

#include <string>
#include <vector>

#include "proto/coordinator.pb.h"
#include "proto/debug.pb.h"

namespace client_v2 {

class Pretty {
 public:
  static void Show(dingodb::pb::coordinator::GetCoordinatorMapResponse &response);
  static void Show(dingodb::pb::coordinator::GetStoreMapResponse &response);

  static void Show(dingodb::pb::debug::DumpRegionResponse &response);
};

}  // namespace client_v2

#endif  // DINGODB_CLIENT_PRETTY_H_