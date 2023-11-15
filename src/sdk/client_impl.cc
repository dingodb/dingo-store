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

#include "sdk/client_impl.h"

#include <cstdint>
#include <memory>
#include <string>

#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "glog/logging.h"
#include "proto/meta.pb.h"
#include "sdk/client.h"
#include "sdk/client_stub.h"

namespace dingodb {
namespace sdk {

Client::ClientImpl::ClientImpl() : init_(false), stub_(new ClientStub()){};

Client::ClientImpl::~ClientImpl() = default;

Status Client::ClientImpl::Init(std::string naming_service_url) {
  CHECK(!init_) << "forbidden multiple init";
  Status open = stub_->Open(naming_service_url);
  if (open.IsOK()) {
    init_ = true;
  }
  return open;
}

}  // namespace sdk
}  // namespace dingodb