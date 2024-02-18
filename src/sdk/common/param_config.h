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

#include "gflags/gflags.h"

// TODO: make params in this file use glfags

const int64_t kSdkVlogLevel = 60;

// ChannelOptions should set "timeout_ms > connect_timeout_ms" for circuit breaker
DECLARE_int64(rpc_channel_timeout_ms);
DECLARE_int64(rpc_channel_connect_timeout_ms);

// each rpc call params, set for brpc::Controller
DECLARE_int64(rpc_max_retry);
DECLARE_int64(rpc_time_out_ms);

// each store rpc params, used for store rpc controller
DECLARE_int64(store_rpc_max_retry);
DECLARE_int64(store_rpc_retry_delay_ms);

// start: use for region scanner
DECLARE_int64(scan_batch_size);
const int64_t kMinScanBatchSize = 1;
const int64_t kMaxScanBatchSize = 100;
// end: use for region scanner

const int64_t kPrefetchRegionCount = 3;

DECLARE_int64(coordinator_interaction_delay_ms);
DECLARE_int64(coordinator_interaction_max_retry);

DECLARE_int64(actuator_thread_num);

DECLARE_int64(raw_kv_delay_ms);
DECLARE_int64(raw_kv_max_retry);

DECLARE_int64(txn_op_delay_ms);
DECLARE_int64(txn_op_max_retry);

DECLARE_int64(vector_op_delay_ms);
DECLARE_int64(vector_op_max_retry);

#endif  // DINGODB_SDK_PARAM_CONFIG_H_