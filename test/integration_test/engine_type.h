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

#ifndef DINGODB_INTEGRATION_TEST_ENGINE_TYPE_
#define DINGODB_INTEGRATION_TEST_ENGINE_TYPE_

#include "sdk/client.h"

namespace dingodb {
namespace integration_test {

class EngineType {
 public:
  virtual ~EngineType() = default;
};

class LsmEngine : EngineType {
 public:
  LsmEngine() = default;
  ~LsmEngine() override = default;
};

class BtreeEngine : EngineType {
 public:
  BtreeEngine() = default;
  ~BtreeEngine() override = default;
};

template <class T>
sdk::EngineType GetEngineType();

}  // namespace integration_test
}  // namespace dingodb
#endif  // DINGODB_INTEGRATION_TEST_ENGINE_TYPE_