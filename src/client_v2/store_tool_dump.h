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

#ifndef DINGODB_STORE_TOOL_DUMP_H_  // NOLINT
#define DINGODB_STORE_TOOL_DUMP_H_

#include "client_v2/store_client_function.h"
namespace client_v2 {

void DumpDb(std::shared_ptr<Context> ctx);
void WhichRegion(std::shared_ptr<Context> ctx);

}  // namespace client_v2

#endif  // DINGODB_STORE_TOOL_DUMP_H_