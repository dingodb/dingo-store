
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGODB_CLIENT_RESTORE_H_
#define DINGODB_CLIENT_RESTORE_H_

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

#include "CLI/CLI.hpp"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "client_v2/store.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"

namespace client_v2 {

bool QueryRegionFromSst(int64_t region_id, std::string file_path, dingodb::pb::common::Region &region);

void SetUpRestoreSubCommands(CLI::App &app);

struct RestoreRegionOptions {
  std::string coor_url;
  int64_t region_id;
  std::string file_path;
};
void SetUpRestoreRegion(CLI::App &app);
void RunRestoreRegion(RestoreRegionOptions const &opt);

struct RestoreRegionDataOptions {
  std::string coor_url;
  int64_t region_id;
  std::string file_path;
};
void SetUpRestoreRegionData(CLI::App &app);
void RunRestoreRegionData(RestoreRegionDataOptions const &opt);

struct CheckRestoreRegionDataOptions {
  std::string coor_url;
  int64_t region_id;
  std::string file_path;
  std::string db_path;
  std::string base_dir;
};
void SetUpCheckRestoreRegionData(CLI::App &app);
void RunCheckRestoreRegionData(CheckRestoreRegionDataOptions const &opt);

}  // namespace client_v2

#endif  // DINGODB_CLIENT_RESTORE_H_