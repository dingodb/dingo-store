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

#ifndef DINGODB_SDK_ACTUATOR_H_
#define DINGODB_SDK_ACTUATOR_H_

#include <functional>
#include <string>

namespace dingodb {
namespace sdk {

class Actuator {
 public:
  virtual ~Actuator() = default;

  virtual bool Start(int thread_num) = 0;

  virtual bool Stop() = 0;

  virtual bool Execute(std::function<void()> func) = 0;

  virtual bool Schedule(std::function<void()> func, int delay_ms) = 0;

  virtual int ThreadNum() const = 0;

  virtual std::string Name() const = 0;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_ACTUATOR_H_