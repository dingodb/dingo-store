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

#ifndef DINGODB_SDK_REGION_CREATOR_DATA_H_
#define DINGODB_SDK_REGION_CREATOR_DATA_H_

#include "sdk/client.h"
#include "sdk/client_stub.h"

namespace dingodb {
namespace sdk {
class RegionCreator::Data {
 public:
  Data(const Data&) = delete;
  const Data& operator=(const Data&) = delete;

  explicit Data(const ClientStub& stub) : stub(stub) {}

  ~Data() = default;

  const ClientStub& stub;

  std::string region_name;

  std::string lower_bound;
  std::string upper_bound;

  EngineType engine_type{kLSM};

  int64_t replica_num{1};

  bool wait{true};
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_REGION_CREATOR_DATA_H_