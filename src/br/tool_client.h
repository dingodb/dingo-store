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

#ifndef DINGODB_BR_TOOL_CLIENT_H_
#define DINGODB_BR_TOOL_CLIENT_H_

#include <memory>

#include "br/parameter.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class ToolClient : public std::enable_shared_from_this<ToolClient> {
 public:
  ToolClient(ToolClientParams params);
  ~ToolClient();

  ToolClient(const ToolClient&) = delete;
  const ToolClient& operator=(const ToolClient&) = delete;
  ToolClient(ToolClient&&) = delete;
  ToolClient& operator=(ToolClient&&) = delete;

  std::shared_ptr<ToolClient> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  static butil::Status GcStart();
  static butil::Status GcStop();
  static butil::Status GetGCSafePoint();
  static butil::Status DisableBalance();
  static butil::Status EnableBalance();
  static butil::Status QueryBalance();
  static butil::Status CoreBalance(const std::string& balance_type, const std::string& action);
  static butil::Status DisableSplitAndMerge();
  static butil::Status EnableSplitAndMerge();
  static butil::Status QuerySplitAndMerge();
  static butil::Status CoreSplitAndMerge(const std::string& type, const std::string& action);
  static butil::Status RemoteVersion();
  static butil::Status LocalVersion();
  static butil::Status RegisterBackup();
  static butil::Status UnregisterBackup(const std::string& backup_task_id);
  static butil::Status RegisterBackupStatus();
  static butil::Status RegisterRestore();
  static butil::Status UnregisterRestore(const std::string& restore_task_id);
  static butil::Status RegisterRestoreStatus();

  ToolClientParams tool_client_params_;
};

}  // namespace br

#endif  // DINGODB_BR_TOOL_CLIENT