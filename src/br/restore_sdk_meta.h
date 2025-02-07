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

#ifndef DINGODB_BR_RESTORE_SDK_META_H_
#define DINGODB_BR_RESTORE_SDK_META_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/meta.pb.h"

namespace br {

class RestoreSdkMeta : public std::enable_shared_from_this<RestoreSdkMeta> {
 public:
  RestoreSdkMeta(ServerInteractionPtr coordinator_interaction, const std::string& restorets,
                 int64_t restoretso_internal, const std::string& storage, const std::string& storage_internal,
                 std::shared_ptr<dingodb::pb::common::BackupMeta> coordinator_sdk_meta_sst);
  ~RestoreSdkMeta();

  RestoreSdkMeta(const RestoreSdkMeta&) = delete;
  const RestoreSdkMeta& operator=(const RestoreSdkMeta&) = delete;
  RestoreSdkMeta(RestoreSdkMeta&&) = delete;
  RestoreSdkMeta& operator=(RestoreSdkMeta&&) = delete;

  std::shared_ptr<RestoreSdkMeta> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status CheckCoordinatorSdkMetaSst();
  butil::Status ExtractFromCoordinatorSdkMetaSst();
  butil::Status ImportSdkMetaToCoordinator() ;
  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  std::string restorets_;
  int64_t restoretso_internal_;
  std::string storage_;
  std::string storage_internal_;

  std::shared_ptr<dingodb::pb::common::BackupMeta> coordinator_sdk_meta_sst_;
  std::shared_ptr<dingodb::pb::meta::MetaALL> meta_all_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_SDK_META_H_