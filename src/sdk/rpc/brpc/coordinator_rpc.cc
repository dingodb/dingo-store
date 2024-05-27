
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

#include "sdk/rpc/brpc/coordinator_rpc.h"
#include "sdk/rpc/brpc/unary_rpc.h"

namespace dingodb {
namespace sdk {

#define DEFINE_COORDINATOR_RPC(METHOD) DEFINE_UNAEY_RPC(pb::coordinator, CoordinatorService, METHOD)

#define DEFINE_META_RPC(METHOD) DEFINE_UNAEY_RPC(pb::meta, MetaService, METHOD)

namespace coordinator {
DEFINE_COORDINATOR_RPC(Hello);
}
DEFINE_COORDINATOR_RPC(QueryRegion);
DEFINE_COORDINATOR_RPC(CreateRegion);
DEFINE_COORDINATOR_RPC(DropRegion);
DEFINE_COORDINATOR_RPC(ScanRegions);

DEFINE_META_RPC(GenerateAutoIncrement);
DEFINE_META_RPC(CreateIndex);
DEFINE_META_RPC(GetIndexByName);
DEFINE_META_RPC(DropIndex);
DEFINE_META_RPC(CreateTableIds);
DEFINE_META_RPC(GetIndex);

DEFINE_META_RPC(TsoService);

}  // namespace sdk
}  // namespace dingodb