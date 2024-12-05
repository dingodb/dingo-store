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

#ifndef DINGODB_BALANCE_REGION_H_
#define DINGODB_BALANCE_REGION_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "coordinator/coordinator_control.h"
#include "proto/common.pb.h"

namespace dingodb {

namespace balanceregion {

class StoreEntry;
using StoreEntryPtr = std::shared_ptr<StoreEntry>;

class CandidateStores;
using CandidateStoresPtr = std::shared_ptr<CandidateStores>;

class BalanceRegionScheduler;
using BalanceRegionSchedulerPtr = std::shared_ptr<BalanceRegionScheduler>;

struct ChangeRegionTask;
using ChangeRegionTaskPtr = std::shared_ptr<ChangeRegionTask>;

struct Tracker;
using TrackerPtr = std::shared_ptr<Tracker>;

// tracking balance region process
struct Tracker {
  struct Record {
    int64_t region_id{0};
    int64_t source_store_id{0};
    int64_t target_store_id{0};

    std::string region_score;

    std::vector<std::string> filter_records;
  };
  using RecordPtr = std::shared_ptr<Record>;

  static TrackerPtr New() { return std::make_shared<Tracker>(); }

  RecordPtr AddRecord() {
    records.push_back(std::make_shared<Record>());
    return GetLastRecord();
  }
  RecordPtr GetLastRecord() { return records.back(); }

  void Print();

  pb::common::StoreType store_type;
  std::string region_score;

  std::vector<RecordPtr> records;
  std::vector<std::string> filter_records;
  ChangeRegionTaskPtr task;
};

// filter abstract interface class
class Filter {
 public:
  virtual ~Filter() = default;

  // true reserve, false filter
  virtual bool Check(const dingodb::pb::common::Store&) { return true; }
  virtual bool Check(int64_t) { return true; }
  virtual bool Check(const dingodb::pb::common::Store&, int64_t) { return true; }
};
using FilterPtr = std::shared_ptr<Filter>;

// filter store by state
class StoreStateFilter : public Filter {
 public:
  StoreStateFilter(TrackerPtr tracker) : tracker_(tracker) {}
  ~StoreStateFilter() override = default;

  bool Check(const dingodb::pb::common::Store& store) override;

 private:
  TrackerPtr tracker_;
};

class ResourceFilter : public Filter {
 public:
  ResourceFilter(std::shared_ptr<CoordinatorControl> coordinator_controller, TrackerPtr tracker)
      : coordinator_controller_(coordinator_controller), tracker_(tracker){};
  ~ResourceFilter() override = default;

  bool Check(const dingodb::pb::common::Store&, int64_t) override;

 private:
  std::shared_ptr<CoordinatorControl> coordinator_controller_;
  TrackerPtr tracker_;
};

// filter region by some health state and leader
class RegionStateFilter : public Filter {
 public:
  RegionStateFilter(std::shared_ptr<CoordinatorControl> coordinator_controller, TrackerPtr tracker)
      : coordinator_controller_(coordinator_controller), tracker_(tracker){};
  ~RegionStateFilter() override = default;

  bool Check(const dingodb::pb::common::Store& store, int64_t region_id) override;

 private:
  std::shared_ptr<CoordinatorControl> coordinator_controller_;
  TrackerPtr tracker_;
};

// filter region by placementsafeguard
class RegionPlacementSafeguard : public Filter {
 public:
  RegionPlacementSafeguard(std::shared_ptr<CoordinatorControl> coordinator_controller, TrackerPtr tracker)
      : coordinator_controller_(coordinator_controller), tracker_(tracker){};
  ~RegionPlacementSafeguard() override = default;

  bool Check(const dingodb::pb::common::Store&, int64_t) override;

 private:
  std::shared_ptr<CoordinatorControl> coordinator_controller_;
  TrackerPtr tracker_;
};

// abstract store node, support calulate region score
class StoreEntry {
 public:
  struct Less {
    bool operator()(const StoreEntryPtr& lhs, const StoreEntryPtr& rhs);
  };

  struct Greater {
    bool operator()(const StoreEntryPtr& lhs, const StoreEntryPtr& rhs);
  };

  StoreEntry(const pb::common::Store& store, const pb::common::StoreMetrics store_metric,
             const std::vector<int64_t>& leader_region_ids, const std::vector<int64_t>& follower_region_ids)
      : store_(store),
        store_metric_(store_metric),
        leader_region_ids_(leader_region_ids),
        follower_region_ids_(follower_region_ids){};
  ~StoreEntry() = default;

  static StoreEntryPtr New(const pb::common::Store& store, const pb::common::StoreMetrics& store_metric,
                           const std::vector<int64_t>& leader_region_ids,
                           const std::vector<int64_t>& follower_region_ids) {
    return std::make_shared<StoreEntry>(store, store_metric, leader_region_ids, follower_region_ids);
  }

  int64_t Id();
  pb::common::Store& Store();

  std::vector<int64_t> LeaderRegionIds();
  std::vector<int64_t> FollowerRegionIds();

  bool IsLeader(int64_t region_id);
  bool IsFollower(int64_t region_id);

  float Score();
  float GetCapacityScore() const { return capacity_score_; }
  float GetRegionCountScore() const { return region_count_score_; }

  // for unit test
  void TestAddLeader(int64_t region_id) { leader_region_ids_.push_back(region_id); }
  void TestAddFollower(int64_t region_id) { follower_region_ids_.push_back(region_id); }
  pb::common::StoreMetrics& StoreMetrics() { return store_metric_; }

 private:
  pb::common::Store store_;
  pb::common::StoreMetrics store_metric_;
  std::vector<int64_t> leader_region_ids_;
  std::vector<int64_t> follower_region_ids_;
  float region_count_score_;
  float capacity_score_;
  float total_score_;
};

// contain some sort candidate store for balance region
// source candidate stores: region score descending order
// target candidate stores: region score ascending order
class CandidateStores {
 public:
  CandidateStores(const std::vector<StoreEntryPtr>& stores, bool asc) : stores_(stores), asc_(asc) { Sort(); }
  ~CandidateStores() = default;

  static CandidateStoresPtr New(const std::vector<StoreEntryPtr>& stores, bool asc) {
    return std::make_shared<CandidateStores>(stores, asc);
  }

  bool HasStore();
  StoreEntryPtr Store(int64_t store_id);
  StoreEntryPtr GetStore();
  uint32_t StoreSize();

  void Sort();
  void Next();

  std::string ToString();

 private:
  bool asc_;
  std::vector<StoreEntryPtr> stores_;

  // the current process store offset of stores_
  int index_{0};
};

// transfer region task descriptor
struct ChangeRegionTask {
  int64_t region_id;
  int64_t source_store_id;
  int64_t target_store_id;
  std::vector<int64_t> new_store_ids;
};

class BalanceRegionScheduler {
 public:
  BalanceRegionScheduler(std::shared_ptr<CoordinatorControl> coordinator_controller,
                         std::shared_ptr<Engine> raft_engine, std::vector<FilterPtr>& store_filters,
                         std::vector<FilterPtr>& region_filters, std::vector<FilterPtr>& resource_filters,
                         std::vector<FilterPtr>& placement_safe_filters, TrackerPtr tracker)
      : coordinator_controller_(coordinator_controller),
        raft_engine_(raft_engine),
        store_filters_(store_filters),
        region_filters_(region_filters),
        resource_filters_(resource_filters),
        placement_safe_filters_(placement_safe_filters),
        tracker_(tracker){};
  ~BalanceRegionScheduler() = default;

  static BalanceRegionSchedulerPtr New(std::shared_ptr<CoordinatorControl> coordinator_controller,
                                       std::shared_ptr<Engine> raft_engine, std::vector<FilterPtr>& store_filters,
                                       std::vector<FilterPtr>& region_filters, std::vector<FilterPtr>& resource_filters,
                                       std::vector<FilterPtr>& placement_safe_filters, TrackerPtr tracker) {
    return std::make_shared<BalanceRegionScheduler>(coordinator_controller, raft_engine, store_filters, region_filters,
                                                    resource_filters, placement_safe_filters, tracker);
  }

  // check run timing
  // set run time base on config item(coordinator.balance_leader_inspection_time_period)
  static bool ShouldRun();

  // launch balance region schedule
  // only one schedule is allowed run at a time
  static butil::Status LaunchBalanceRegion(std::shared_ptr<CoordinatorControl> coordinator_controller,
                                           std::shared_ptr<Engine> raft_engine, pb::common::StoreType store_type,
                                           bool dryrun, bool force, TrackerPtr tracker);

  // schedule balance region generate change region task
  ChangeRegionTaskPtr Schedule(const pb::common::RegionMap& region_map, const pb::common::StoreMap& store_map);

  // Just for unit test
  static std::vector<std::pair<int, int>> TestParseTimePeriod(const std::string& time_period) {
    return ParseTimePeriod(time_period);
  }

 private:
  // parse config item(coordinator.balance_leader_inspection_time_period)
  static std::vector<std::pair<int, int>> ParseTimePeriod(const std::string& time_period);
  // commit change region task to raft
  void CommitChangRegionTaskList(const ChangeRegionTaskPtr& task);

  static pb::common::Store GetStore(const pb::common::StoreMap& store_map, int64_t store_id);

  // store_id: leader_region_ids,follower_region_ids
  using StoreRegionMap = std::map<int64_t, std::pair<std::vector<int64_t>, std::vector<int64_t>>>;

  StoreRegionMap GenerateStoreRegionMap(const pb::common::RegionMap& region_map);

  std::vector<StoreEntryPtr> GenerateStoreEntries(const StoreRegionMap& store_region_id_map,
                                                  const pb::common::StoreMap& store_map);

  ChangeRegionTaskPtr GenerateChangeRegionTask(CandidateStoresPtr source_stores, CandidateStoresPtr target_stores);

  static ChangeRegionTaskPtr GenerateChangeRegionTask(pb::common::RegionMetrics region_metrics, int64_t source_store_id,
                                                      StoreEntryPtr target_store_entry);

  // filter true: eliminate false: reserve
  bool FilterStore(const dingodb::pb::common::Store& store);

  // true: eliminate false: reserve
  bool FilterRegion(int64_t region_id);
  std::vector<int64_t> FilterRegion(std::vector<int64_t> region_ids);

  // true: eliminate false: reserve
  bool FilterResource(const dingodb::pb::common::Store& store, int64_t region_id);
  std::vector<StoreEntryPtr> FilterResource(const std::vector<StoreEntryPtr>& store_entries, int64_t region_id);

  // true: eliminate false: reserve
  bool FilterPlacementSafeguard(const dingodb::pb::common::Store& store, int64_t region_id);

  std::shared_ptr<CoordinatorControl> coordinator_controller_;
  // for commit change region task
  std::shared_ptr<Engine> raft_engine_;

  // some filter
  std::vector<FilterPtr> store_filters_;
  std::vector<FilterPtr> region_filters_;
  std::vector<FilterPtr> resource_filters_;
  std::vector<FilterPtr> placement_safe_filters_;

  // for track balance region schedule process
  TrackerPtr tracker_;
};

}  // namespace balanceregion
}  // namespace dingodb

#endif  // DINGODB_BALANCE_LEADER_H_
