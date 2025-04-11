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

#ifndef DINGODB_BR_PARAMETER_H_
#define DINGODB_BR_PARAMETER_H_

#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"

namespace br {

DECLARE_string(br_coor_url);

DECLARE_string(br_type);

DECLARE_string(br_backup_type);

DECLARE_string(backupts);
DECLARE_int64(backuptso_internal);

DECLARE_string(storage);
DECLARE_string(storage_internal);

// backup watch interval in seconds. default 10s
DECLARE_uint32(backup_watch_interval_s);

// backup task timeout in seconds. default 100s
DECLARE_uint32(backup_task_timeout_s);

// backup task max retry times. default 5
DECLARE_uint32(backup_task_max_retry);

struct BackupParams {
  std::string coor_url;
  std::string br_type;
  std::string br_backup_type;
  std::string backupts;
  int64_t backuptso_internal;
  std::string storage;
  std::string storage_internal;
};

inline const std::string kBackupFileLock = "backup.lock";

DECLARE_bool(br_server_interaction_print_each_rpc_request);

DECLARE_int32(br_server_interaction_max_retry);

DECLARE_int64(br_server_interaction_timeout_ms);

DECLARE_bool(br_log_switch_backup_detail);

DECLARE_bool(br_log_switch_backup_detail_detail);

DECLARE_string(br_log_dir);

DECLARE_string(br_restore_type);

struct RestoreParams {
  std::string coor_url;
  std::string br_type;
  std::string br_restore_type;
  std::string storage;
  std::string storage_internal;
};

DECLARE_bool(br_log_switch_restore_detail);

DECLARE_bool(br_log_switch_restore_detail_detail);

// restore watch interval in seconds. default 10s
DECLARE_uint32(restore_watch_interval_s);

// restore task timeout in seconds. default 100s
DECLARE_uint32(restore_task_timeout_s);

// restore task max retry times. default 5
DECLARE_uint32(restore_task_max_retry);

// default replica number. default 0.
DECLARE_int32(br_default_replica_num);

// create region concurrency
DECLARE_uint32(create_region_concurrency);

// restore region concurrency
DECLARE_uint32(restore_region_concurrency);

// create region timeout s (second)
DECLARE_int64(create_region_timeout_s);

// restore region timeout s (second)
DECLARE_int64(restore_region_timeout_s);

// br backup  version comparison dingo-store version comparison
DECLARE_bool(backup_strict_version_comparison);

// br restore  version comparison dingo-store version comparison
DECLARE_bool(restore_strict_version_comparison);

// br restore after create region wait for region normal max retry
DECLARE_int32(restore_wait_for_region_normal_max_retry);

// br restore after create region wait for region normal interval s (second)
DECLARE_uint32(restore_wait_for_region_normal_interval_s);

struct ToolParams {
  std::string br_type;
  std::string br_tool_type;
  std::string br_dump_file;
  std::string br_diff_file1;
  std::string br_diff_file2;
  std::string br_client_method;
  std::string br_client_method_param1;
};

struct ToolDumpParams {
  std::string br_type;
  std::string br_tool_type;
  std::string br_dump_file;
};

struct ToolDiffParams {
  std::string br_type;
  std::string br_tool_type;
  std::string br_diff_file1;
  std::string br_diff_file2;
};

struct ToolClientParams {
  std::string br_type;
  std::string br_tool_type;
  std::string br_client_method;
  std::string br_client_method_param1;
};

// br tool type
DECLARE_string(br_tool_type);

// br dump file
DECLARE_string(br_dump_file);

// br diff file1
DECLARE_string(br_diff_file1);

// br diff file2
DECLARE_string(br_diff_file2);

// br client method
DECLARE_string(br_client_method);

// br client method param1
DECLARE_string(br_client_method_param1);

}  // namespace br

#endif  // DINGODB_BR_PARAMETER_H_