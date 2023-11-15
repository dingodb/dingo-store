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

#ifndef DINGODB_SDK_CLIENT_IMPL_H_
#define DINGODB_SDK_CLIENT_IMPL_H_

#include <cstdint>
#include <memory>

#include "coordinator/coordinator_interaction.h"
#include "sdk/client.h"
#include "sdk/client_stub.h"
#include "sdk/meta_cache.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

class Client::ClientImpl {
 public:
  ClientImpl(const ClientImpl&) = delete;
  const ClientImpl& operator=(const ClientImpl&) = delete;

  ClientImpl();

  ~ClientImpl();

  Status Init(std::string naming_service_url);

  const ClientStub& GetStub() const { return *stub_; }

 private:
  bool init_;
  std::unique_ptr<ClientStub> stub_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_CLIENT_IMPL_H_