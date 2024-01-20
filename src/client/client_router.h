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

#ifndef DINGODB_CLIENT_ROUTER_H_
#define DINGODB_CLIENT_ROUTER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/mutex.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"

namespace client {

class RegionEntry {
 public:
  RegionEntry(const dingodb::pb::common::Region& region);
  ~RegionEntry() = default;

  static std::shared_ptr<RegionEntry> New(dingodb::pb::common::Region region);

  bool IsDirty();
  void SetDirty(bool dirty);

  int64_t RegionId();
  int64_t PartitionId();
  int64_t TableId();

  dingodb::pb::common::Region& Region();
  void SetRegion(const dingodb::pb::common::Region& region);

  const dingodb::pb::common::Range& Range();
  void SetRange(const dingodb::pb::common::Range& range);

  const dingodb::pb::common::RegionEpoch& Epoch();
  void SetEpoch(const dingodb::pb::common::RegionEpoch& epoch);

  std::vector<std::string> GetAddrs();
  void SetPeers(const dingodb::pb::error::StoreRegionInfo& region_info);

  dingodb::pb::store::Context GenConext();

 private:
  dingodb::pb::common::Region region_;
  std::atomic<bool> is_dirty_;
};

using RegionEntryPtr = std::shared_ptr<RegionEntry>;

class RegionRouter {
 public:
  RegionRouter();
  ~RegionRouter();

  static RegionRouter& GetInstance();

  void AddRegionEntry(const dingodb::pb::common::Region& region);
  void AddRegionEntry(RegionEntryPtr region);
  RegionEntryPtr AddRegionEntry(int64_t region_id);

  void UpdateRegionEntry(const dingodb::pb::error::StoreRegionInfo& region_info);

  RegionEntryPtr QueryRegionEntry(const std::string& key);
  RegionEntryPtr QueryRegionEntry(int64_t region_id);
  std::vector<RegionEntryPtr> QueryRegionEntryByTable(int64_t table_id);
  std::vector<RegionEntryPtr> QueryRegionEntryByPartition(int64_t partition_id);

  dingodb::pb::store::Context GenConext(int64_t region_id);

 private:
  static bool UpdateRegion(RegionEntryPtr region_entry);

  // key: the start_key of region range
  // value: RegionEntry
  std::map<std::string, RegionEntryPtr> route_map_;
  bthread_mutex_t mutex_;
};

}  // namespace client

#endif  // DINGODB_CLIENT_ROUTER_H_