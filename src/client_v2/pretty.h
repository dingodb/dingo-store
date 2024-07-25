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

#ifndef DINGODB_CLIENT_PRETTY_H_
#define DINGODB_CLIENT_PRETTY_H_

#include <string>
#include <vector>

#include "butil/status.h"
#include "proto/coordinator.pb.h"
#include "proto/debug.pb.h"
#include "proto/meta.pb.h"

namespace client_v2 {

class Pretty {
 public:
  struct TenantInfo {
    int64_t id;
    std::string name;
    std::string comment;
    int64_t create_time;
    int64_t update_time;
  };

  static bool ShowError(const butil::Status &status);
  static bool ShowError(const dingodb::pb::error::Error &error);

  static void Show(dingodb::pb::coordinator::GetCoordinatorMapResponse &response);
  static void Show(dingodb::pb::coordinator::GetStoreMapResponse &response);

  static void Show(const dingodb::pb::debug::DumpRegionResponse::Data &data,
                   const dingodb::pb::meta::TableDefinition &table_definition = {},
                   const std::vector<std::string> &exclude_columns = {});
  static void Show(dingodb::pb::debug::DumpRegionResponse &response);

  static void Show(std::vector<TenantInfo> tenants);
  static void Show(dingodb::pb::store::TxnScanResponse &response);
  static void Show(dingodb::pb::store::TxnScanLockResponse &response);
};

}  // namespace client_v2

#endif  // DINGODB_CLIENT_PRETTY_H_