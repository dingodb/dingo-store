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

#include "sdk/rpc/brpc/index_service_rpc.h"

namespace dingodb {
namespace sdk {

#define DEFINE_INDEX_SERVICE_RPC(METHOD) DEFINE_UNAEY_RPC(pb::index, IndexService, METHOD)

namespace index {
DEFINE_INDEX_SERVICE_RPC(Hello);
}

DEFINE_INDEX_SERVICE_RPC(VectorAdd);
DEFINE_INDEX_SERVICE_RPC(VectorSearch);
DEFINE_INDEX_SERVICE_RPC(VectorDelete);
DEFINE_INDEX_SERVICE_RPC(VectorBatchQuery);
DEFINE_INDEX_SERVICE_RPC(VectorGetBorderId);
DEFINE_INDEX_SERVICE_RPC(VectorScanQuery);
DEFINE_INDEX_SERVICE_RPC(VectorGetRegionMetrics);
DEFINE_INDEX_SERVICE_RPC(VectorCount);

}  // namespace sdk
}  // namespace dingodb