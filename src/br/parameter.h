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

}  // namespace br

#endif  // DINGODB_BR_PARAMETER_H_