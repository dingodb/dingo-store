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

#ifndef DINGODB_SDK_PARAM_CONFIG_H_
#define DINGODB_SDK_PARAM_CONFIG_H_

#include <cstdint>

// TODO: make params in this file use glfags

// start each rpc call params
const int64_t kRpcCallMaxRetry = 3;

const int64_t kRpcTimeOutMs = 5000;
// end each rpc call params

// use case: wrong leader or request range invalid
const int64_t kRpcMaxRetry = 5;

#endif  // DINGODB_SDK_PARAM_CONFIG_H_