//
// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "sdk/store_rpc.h"

#include "fmt/core.h"

namespace dingodb {
namespace sdk {

KvGetRpc::KvGetRpc() : KvGetRpc("") {}

KvGetRpc::KvGetRpc(const std::string& cmd) : ClientRpc(cmd) {}

KvGetRpc::~KvGetRpc() = default;

void KvGetRpc::Send(StoreService_Stub& stub, google::protobuf::Closure* done) {
  stub.KvGet(MutableController(), request, response, done);
}

std::string KvGetRpc::ConstMethod() { return fmt::format("{}.KvGetRpc", StoreService::descriptor()->name()); }

KvPutRpc::KvPutRpc() : KvPutRpc("") {}

KvPutRpc::KvPutRpc(const std::string& cmd) : ClientRpc(cmd) {}

KvPutRpc::~KvPutRpc() = default;

void KvPutRpc::Send(StoreService_Stub& stub, google::protobuf::Closure* done) {
  stub.KvPut(MutableController(), request, response, done);
}

std::string KvPutRpc::ConstMethod() { return fmt::format("{}.KvPutRpc", StoreService::descriptor()->name()); }

}  // namespace sdk
}  // namespace dingodb