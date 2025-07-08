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

#include "gflags/gflags.h"

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

DEFINE_int32(br_server_interaction_max_retry, 10, "br server interaction  max retry. default 10");

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

// br backup  version comparison dingo-store version comparison
DEFINE_bool(backup_strict_version_comparison, true,
            "br backup version vs dingo-store version must be consistent. default true.");

// br restore  version comparison dingo-store version comparison
DEFINE_bool(restore_strict_version_comparison, true,
            "br restore version vs dingo-store version must be consistent. default true.");

// br restore after create region wait for region normal max retry
DEFINE_int32(restore_wait_for_region_normal_max_retry, 30, "restore wait for region normal max retry. default 30");

// br restore after create region wait for region normal interval s (second)
DEFINE_uint32(restore_wait_for_region_normal_interval_s, 1, "restore wait for region normal interval s. default 1s");

// br tool type. dump or diff
DEFINE_string(br_tool_type, "dump", "br tool type. default dump");

// br dump file
DEFINE_string(br_dump_file, "", "br dump file. default empty");

// br diff file1
DEFINE_string(br_diff_file1, "", "br diff file1. default empty");

// br diff file2
DEFINE_string(br_diff_file2, "", "br diff file2. default empty");

// br client method
DEFINE_string(br_client_method, "", "br client method. default empty");

// br client method param1
DEFINE_string(br_client_method_param1, "", "br client method param1. default empty");

DEFINE_bool(br_backup_enable_get_job_list_check, true, "br backup enable get job list check. default true");

// br backup index must be exist
DEFINE_bool(br_backup_index_must_be_exist, true,
            "br backup index must be exist. default true. if false, index region will not be backup.");

// br backup document must be exist
DEFINE_bool(br_backup_document_must_be_exist, true,
            "br backup document must be exist. default true. if false, document region will not be backup.");

// br restore index must be exist
DEFINE_bool(br_restore_index_must_be_exist, false,
            "br restore index must be exist. default false. if false, index region will not be restore.");

// br restore document must be exist
DEFINE_bool(br_restore_document_must_be_exist, false,
            "br restore document must be exist. default false. if false, document region will not be restore.");

}  // namespace br