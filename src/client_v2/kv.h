
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
// #include "client_v2/store_function.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"

#ifndef DINGODB_KV_H
#define DINGODB_KV_H
namespace client_v2 {
void SetUpKVSubCommands(CLI::App &app);
// coordinator kv
struct KvHelloOptions {
  std::string coor_url;
};
void SetUpKvHello(CLI::App &app);
void RunKvHello(KvHelloOptions const &opt);

struct GetRawKvIndexOptions {
  std::string coor_url;
  std::string key;
};
void SetUpGetRawKvIndex(CLI::App &app);
void RunGetRawKvIndex(GetRawKvIndexOptions const &opt);

struct GetRawKvRevOptions {
  std::string coor_url;
  int64_t revision;
  int64_t sub_revision;
};
void SetUpGetRawKvRev(CLI::App &app);
void RunGetRawKvRev(GetRawKvRevOptions const &opt);

struct CoorKvRangeOptions {
  std::string coor_url;
  std::string key;
  std::string range_end;
  int64_t limit;
  bool keys_only;
  bool count_only;
};
void SetUpCoorKvRange(CLI::App &app);
void RunCoorKvRange(CoorKvRangeOptions const &opt);

// coordinator kv
struct CoorKvPutOptions {
  std::string coor_url;
  std::string key;
  std::string value;
  int64_t lease;
  bool ignore_lease;
  bool ignore_value;
  bool need_prev_kv;
};
void SetUpCoorKvPut(CLI::App &app);
void RunCoorKvPut(CoorKvPutOptions const &opt);

struct CoorKvDeleteRangeOptions {
  std::string coor_url;
  std::string key;
  std::string range_end;
  bool need_prev_kv;
};
void SetUpCoorKvDeleteRange(CLI::App &app);
void RunCoorKvDeleteRange(CoorKvDeleteRangeOptions const &opt);

struct CoorKvCompactionOptions {
  std::string coor_url;
  std::string key;
  int64_t revision;
  std::string range_end;
};
void SetUpCoorKvCompaction(CLI::App &app);
void RunCoorKvCompaction(CoorKvCompactionOptions const &opt);

// coordinator watch
struct OneTimeWatchOptions {
  std::string coor_url;
  std::string key;
  int64_t revision;
  bool need_prev_kv;
  bool wait_on_not_exist_key;
  bool no_put;
  bool no_delete;
  int32_t max_watch_count;
};
void SetUpOneTimeWatch(CLI::App &app);
void RunOneTimeWatch(OneTimeWatchOptions const &opt);

struct LockOptions {
  std::string coor_url;
  std::string lock_name;
  std::string client_uuid;
};
void SetUpLock(CLI::App &app);
void RunLock(LockOptions const &opt);

struct LeaseGrantOptions {
  std::string coor_url;
  int64_t id;
  int64_t ttl;
};
void SetUpLeaseGrant(CLI::App &app);
void RunLeaseGrant(LeaseGrantOptions const &opt);

struct LeaseRevokeOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpLeaseRevoke(CLI::App &app);
void RunLeaseRevoke(LeaseRevokeOptions const &opt);

struct LeaseRenewOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpLeaseRenew(CLI::App &app);
void RunLeaseRenew(LeaseRenewOptions const &opt);

struct LeaseQueryOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpLeaseQuery(CLI::App &app);
void RunLeaseQuery(LeaseQueryOptions const &opt);

struct ListLeasesOptions {
  std::string coor_url;
};
void SetUpListLeases(CLI::App &app);
void RunListLeases(ListLeasesOptions const &opt);

}  // namespace client_v2
#endif  // DINGODB_KV_H
