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

#ifndef DINGODB_BR_BACKUP_SDK_META_H_
#define DINGODB_BR_BACKUP_SDK_META_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/meta.pb.h"

namespace br {

class BackupSdkMeta : public std::enable_shared_from_this<BackupSdkMeta> {
 public:
  BackupSdkMeta(ServerInteractionPtr coordinator_interaction, const std::string &storage_internal);
  ~BackupSdkMeta();

  BackupSdkMeta(const BackupSdkMeta&) = delete;
  const BackupSdkMeta& operator=(const BackupSdkMeta&) = delete;
  BackupSdkMeta(BackupSdkMeta&&) = delete;
  BackupSdkMeta& operator=(BackupSdkMeta&&) = delete;

  std::shared_ptr<BackupSdkMeta> GetSelf();

  butil::Status GetSdkMetaFromCoordinator();

  butil::Status Run();

  butil::Status Backup();

  std::shared_ptr<dingodb::pb::common::BackupMeta> GetBackupMeta();

 protected:
 private:
  ServerInteractionPtr coordinator_interaction_;
  std::string storage_internal_;
  std::shared_ptr<dingodb::pb::meta::MetaALL> meta_all_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> backup_sdk_meta_;
};

}  // namespace br

#endif  // DINGODB_BR_BACKUP_SDK_META_H_