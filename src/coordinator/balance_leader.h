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

#ifndef DINGODB_BALANCE_LEADER_H_
#define DINGODB_BALANCE_LEADER_H_

#include <algorithm>
#include <complex>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "coordinator/coordinator_control.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"

namespace dingodb {

namespace balance {

class StoreEntry;
using StoreEntryPtr = std::shared_ptr<StoreEntry>;

class CandidateStores;
using CandidateStoresPtr = std::shared_ptr<CandidateStores>;

class BalanceLeaderScheduler;
using BalanceLeaderSchedulerPtr = std::shared_ptr<BalanceLeaderScheduler>;

struct TransferLeaderTask;
using TransferLeaderTaskPtr = std::shared_ptr<TransferLeaderTask>;

struct Tracker;
using TrackerPtr = std::shared_ptr<Tracker>;

struct Tracker {
  struct Record {
    uint32_t round{0};
    int64_t region_id{0};
    int64_t source_store_id{0};
    int64_t target_store_id{0};

    std::string leader_score;

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

  std::string leader_score;
  std::string expect_leader_score;

  std::vector<RecordPtr> records;
  std::vector<std::string> filter_records;
  std::vector<TransferLeaderTaskPtr> tasks;
};

// filter abstract interface class
class Filter {
 public:
  virtual ~Filter() = default;

  // true reserve, false filter
  virtual bool Check(dingodb::pb::common::Store&) { return true; }
  virtual bool Check(int64_t) { return true; }
};
using FilterPtr = std::shared_ptr<Filter>;

// filter store by state
class StoreStateFilter : public Filter {
 public:
  StoreStateFilter(TrackerPtr tracker) : tracker_(tracker) {}
  ~StoreStateFilter() override = default;

  bool Check(dingodb::pb::common::Store& store) override;

 private:
  TrackerPtr tracker_;
};

// filter region by some health state
class RegionHealthFilter : public Filter {
 public:
  RegionHealthFilter(std::shared_ptr<CoordinatorControl> coordinator_controller, TrackerPtr tracker)
      : coordinator_controller_(coordinator_controller), tracker_(tracker){};
  ~RegionHealthFilter() override = default;

  bool Check(int64_t region_id) override;

 private:
  std::shared_ptr<CoordinatorControl> coordinator_controller_;
  TrackerPtr tracker_;
};

// filter task
class TaskFilter : public Filter {
 public:
  TaskFilter(std::shared_ptr<CoordinatorControl> coordinator_controller, TrackerPtr tracker)
      : coordinator_controller_(coordinator_controller), tracker_(tracker){};
  ~TaskFilter() override = default;

  bool Check(int64_t region_id) override;

 private:
  std::shared_ptr<CoordinatorControl> coordinator_controller_;
  TrackerPtr tracker_;
};

class StoreEntry {
 public:
  struct Less {
    bool operator()(const StoreEntryPtr& lhs, const StoreEntryPtr& rhs);
  };

  struct Greater {
    bool operator()(const StoreEntryPtr& lhs, const StoreEntryPtr& rhs);
  };

  StoreEntry(const pb::common::Store& store, const std::vector<int64_t>& leader_region_ids,
             const std::vector<int64_t>& follower_region_ids)
      : store_(store), leader_region_ids_(leader_region_ids), follower_region_ids_(follower_region_ids){};
  ~StoreEntry() = default;

  static StoreEntryPtr New(const pb::common::Store& store, const std::vector<int64_t>& leader_region_ids,
                           const std::vector<int64_t>& follower_region_ids) {
    return std::make_shared<StoreEntry>(store, leader_region_ids, follower_region_ids);
  }

  int64_t Id();
  pb::common::Store& Store();
  std::vector<int64_t> LeaderRegionIds();
  std::vector<int64_t> FollowerRegionIds();
  bool IsLeader(int64_t region_id);
  bool IsFollower(int64_t region_id);

  int32_t DeltaLeaderNum() const;
  void IncDeltaLeaderNum();
  void DecDeltaLeaderNum();

  float LeaderScore();
  float LeaderScore(int32_t delta);

  void TestAddLeader(int64_t region_id) { leader_region_ids_.push_back(region_id); }

  void TestAddFollower(int64_t region_id) { follower_region_ids_.push_back(region_id); }

 private:
  pb::common::Store store_;
  std::vector<int64_t> leader_region_ids_;
  std::vector<int64_t> follower_region_ids_;
  int32_t delta_leader_num_{0};
};

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

  int index_{0};
};

struct TransferLeaderTask {
  int64_t region_id;
  int64_t source_store_id;
  int64_t target_store_id;
  pb::common::Location target_raft_location;
  pb::common::Location target_server_location;
};

class BalanceLeaderScheduler {
 public:
  BalanceLeaderScheduler(std::shared_ptr<CoordinatorControl> coordinator_controller,
                         std::shared_ptr<Engine> raft_engine, std::vector<FilterPtr>& store_filters,
                         std::vector<FilterPtr>& region_filters, std::vector<FilterPtr>& task_filters,
                         TrackerPtr tracker)
      : coordinator_controller_(coordinator_controller),
        raft_engine_(raft_engine),
        store_filters_(store_filters),
        region_filters_(region_filters),
        task_filters_(task_filters),
        tracker_(tracker){};
  ~BalanceLeaderScheduler() = default;

  static BalanceLeaderSchedulerPtr New(std::shared_ptr<CoordinatorControl> coordinator_controller,
                                       std::shared_ptr<Engine> raft_engine, std::vector<FilterPtr>& store_filters,
                                       std::vector<FilterPtr>& region_filters, std::vector<FilterPtr>& task_filters,
                                       TrackerPtr tracker) {
    return std::make_shared<BalanceLeaderScheduler>(coordinator_controller, raft_engine, store_filters, region_filters,
                                                    task_filters, tracker);
  }

  static butil::Status LaunchBalanceLeader(std::shared_ptr<CoordinatorControl> coordinator_controller,
                                           std::shared_ptr<Engine> raft_engine, pb::common::StoreType store_type,
                                           bool dryrun, TrackerPtr tracker);

  std::vector<TransferLeaderTaskPtr> Schedule(const pb::common::RegionMap& region_map,
                                              const pb::common::StoreMap& store_map);

 private:
  void CommitTransferLeaderTaskList(const std::vector<TransferLeaderTaskPtr>& tasks);

  static pb::common::Store GetStore(const pb::common::StoreMap& store_map, int64_t store_id);

  // store_id: leader_region_ids,follower_region_ids
  using StoreRegionMap = std::map<int64_t, std::pair<std::vector<int64_t>, std::vector<int64_t>>>;

  static StoreRegionMap GenerateStoreRegionMap(const pb::common::RegionMap& region_map);

  static void ReadjustLeaderScore(CandidateStoresPtr source_candidate_stores,
                                  CandidateStoresPtr target_candidate_stores, TransferLeaderTaskPtr task);

  static std::vector<int64_t> FilterUsedRegion(std::vector<int64_t> region_ids, const std::set<int64_t>& used_regions);
  pb::coordinator_internal::RegionInternal PickOneRegion(std::vector<int64_t> region_ids);

  static std::vector<StoreEntryPtr> GetFollowerStores(CandidateStoresPtr candidate_stores,
                                                      pb::coordinator_internal::RegionInternal& region,
                                                      int64_t leader_store_id);
  static StoreEntryPtr GetLeaderStore(CandidateStoresPtr candidate_stores,
                                      pb::coordinator_internal::RegionInternal& region);

  std::vector<StoreEntryPtr> GenerateStoreEntries(const StoreRegionMap& store_region_id_map,
                                                  const pb::common::StoreMap& store_map);

  static TransferLeaderTaskPtr GenerateTransferLeaderTask(int64_t region_id, int64_t leader_store_id,
                                                          StoreEntryPtr follower_store_entry);

  TransferLeaderTaskPtr GenerateTransferOutLeaderTask(CandidateStoresPtr candidate_stores,
                                                      const std::set<int64_t>& used_regions);
  TransferLeaderTaskPtr GenerateTransferInLeaderTask(CandidateStoresPtr candidate_stores,
                                                     const std::set<int64_t>& used_regions);

  // filter
  bool FilterStore(dingodb::pb::common::Store& store);

  bool FilterRegion(int64_t region_id);
  std::vector<int64_t> FilterRegion(std::vector<int64_t> region_ids);

  bool FilterTask(int64_t region_id);
  std::vector<TransferLeaderTaskPtr> FilterTask(std::vector<TransferLeaderTaskPtr>& transfer_leader_tasks);

  std::shared_ptr<CoordinatorControl> coordinator_controller_;
  std::shared_ptr<Engine> raft_engine_;

  std::vector<FilterPtr> store_filters_;
  std::vector<FilterPtr> region_filters_;
  std::vector<FilterPtr> task_filters_;

  TrackerPtr tracker_;
};

}  // namespace balance
}  // namespace dingodb

#endif  // DINGODB_BALANCE_LEADER_H_
