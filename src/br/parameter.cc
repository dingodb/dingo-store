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

#include "br/parameter.h"

namespace br {

DEFINE_string(br_coor_url, "", "coordinator url");

DEFINE_string(br_type, "backup", "backup restore type. default: backup");

DEFINE_string(br_backup_type, "full", "backup  type. default: full.");

DEFINE_string(backupts, "", "backup ts. like: 2022-09-08 13:30:00 +08:00");
DEFINE_int64(backuptso_internal, 0, "backup tso. like: convert 2022-09-08 13:30:00 +08:00 to tso");

DEFINE_string(storage, "", "storage. like: local:///br_data");
DEFINE_string(storage_internal, "", "storage. like: /br_data. remove local://");

// backup watch interval in seconds. default 5s
DEFINE_uint32(backup_watch_interval_s, 5, "backup watch interval in seconds. default 5s");

// backup task timeout in seconds. default 100s
DEFINE_uint32(backup_task_timeout_s, 100, "backup task timeout in seconds. default 100s");

// backup task max retry times. default 5
DEFINE_uint32(backup_task_max_retry, 5, "backup task max retry times. default 5");

DEFINE_bool(br_server_interaction_print_each_rpc_request, false,
            "br server interaction log switch rpc request. default is false");

DEFINE_int32(br_server_interaction_max_retry, 5, "br server interaction  max retry. default 5");

// DEFINE_int64(br_server_interaction_timeout_ms, 60000, "br server interaction connect timeout . default 60000 ms");
DEFINE_int64(br_server_interaction_timeout_ms, 60000, "br server interaction connect timeout . default 60000 ms");

DEFINE_bool(br_log_switch_backup_detail, true, "backup detail log");

DEFINE_bool(br_log_switch_backup_detail_detail, false, "backup detail detail log");

DEFINE_string(br_log_dir, "./log", "backup log dir. default ./log");

DEFINE_string(br_restore_type, "full", "restore  type. default: full.");

DEFINE_bool(br_log_switch_restore_detail, true, "restore detail log");

DEFINE_bool(br_log_switch_restore_detail_detail, false, "restore detail detail log");

// restore watch interval in seconds. default 5s
DEFINE_uint32(restore_watch_interval_s, 5, "restore watch interval in seconds. default 5s");

// restore task timeout in seconds. default 100s
DEFINE_uint32(restore_task_timeout_s, 100, "restore task timeout in seconds. default 100s");

// restore task max retry times. default 5
DEFINE_uint32(restore_task_max_retry, 5, "restore task max retry times. default 5");

// default replica number
DEFINE_int32(br_default_replica_num, 0, "default replica number. default 0");

// create region concurrency
DEFINE_uint32(create_region_concurrency, 10, "restore create region to coordinator concurrency. default 10");

// restore region concurrency
DEFINE_uint32(restore_region_concurrency, 5, "restore region concurrency. default 5");

// create region timeout s (second)
DEFINE_int64(create_region_timeout_s, 60, "restore create region to coordinator timeout s. default 60s");

// restore region timeout s (second)
DEFINE_int64(restore_region_timeout_s, 600, "restore region timeout s. default 600s");

}  // namespace br