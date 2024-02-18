
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

#include "sdk/common/param_config.h"

// ChannelOptions should set "timeout_ms > connect_timeout_ms" for circuit breaker
DEFINE_int64(rpc_channel_timeout_ms, 500000, "rpc channel timeout ms");
DEFINE_int64(rpc_channel_connect_timeout_ms, 3000, "rpc channel connect timeout ms");

DEFINE_int64(rpc_max_retry, 3, "rpc call max retry times");
DEFINE_int64(rpc_time_out_ms, 500000, "rpc call timeout ms");

DEFINE_int64(store_rpc_max_retry, 5, "store rpc max retry times, use case: wrong leader or request range invalid");
DEFINE_int64(store_rpc_retry_delay_ms, 1000, "store rpc retry delay ms");

DEFINE_int64(scan_batch_size, 10, "scan batch size, use for region scanner");

DEFINE_int64(coordinator_interaction_delay_ms, 200, "coordinator interaction delay ms");
DEFINE_int64(coordinator_interaction_max_retry, 300, "coordinator interaction max retry");

DEFINE_int64(txn_op_delay_ms, 200, "txn op delay ms");
DEFINE_int64(txn_op_max_retry, 2, "txn op max retry times");

DEFINE_int64(actuator_thread_num, 8, "actuator thread num");

DEFINE_int64(raw_kv_delay_ms, 200, "raw kv backoff delay ms");
DEFINE_int64(raw_kv_max_retry, 5, "raw kv max retry times");

DEFINE_int64(vector_op_delay_ms, 500, "raw kv backoff delay ms");
DEFINE_int64(vector_op_max_retry, 10, "raw kv max retry times");