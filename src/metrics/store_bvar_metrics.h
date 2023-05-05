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

#ifndef DINGODB_STORE_BVAR_METRICS_H_
#define DINGODB_STORE_BVAR_METRICS_H_

#include <string>

#include "bvar/bvar.h"
#include "bvar/multi_dimension.h"
#include "bvar/reducer.h"
#include "bvar/status.h"
#include "bvar/variable.h"
#include "common/helper.h"

namespace dingodb {

class StoreBvarMetrics {
 public:
  StoreBvarMetrics()
      : leader_switch_time_("dingo_metrics_store_raft_leader_switch_time", {"region"}),
        leader_switch_count_("dingo_metrics_store_raft_leader_switch_count", {"region"}),
        commit_count_per_second_("dingo_metrics_store_raft_commit_count_per_second", {"region"}),
        apply_count_per_second_("dingo_metrics_store_raft_apply_count_per_second", {"region"}) {
    leader_switch_time_.expose("dingo_metrics_store_raft_leader_switch_time");
    leader_switch_count_.expose("dingo_metrics_store_raft_leader_switch_count");
    commit_count_per_second_.expose("dingo_metrics_store_raft_commit_count_per_second");
    apply_count_per_second_.expose("dingo_metrics_store_raft_apply_count_per_second");
  }
  ~StoreBvarMetrics() = default;

  StoreBvarMetrics(const StoreBvarMetrics&) = delete;
  void operator=(const StoreBvarMetrics&) = delete;

  static StoreBvarMetrics& GetInstance();

  void UpdateLeaderSwitchTime(std::string region_id) {
    auto* region_stat = leader_switch_time_.get_stats({region_id});
    region_stat->set_value(Helper::TimestampMs());
  }

  void UpdateLeaderSwitchCount(std::string region_id, uint64_t value) {
    auto* region_stat = leader_switch_count_.get_stats({region_id});
    region_stat->set_value(value);
  }

  void IncCommitCountPerSecond(std::string region_id) {
    auto* region_stat = commit_count_per_second_.get_stats({region_id});
    *region_stat << 1;
  }

  void IncApplyCountPerSecond(std::string region_id) {
    auto* region_stat = apply_count_per_second_.get_stats({region_id});
    *region_stat << 1;
  }

 private:
  bvar::MultiDimension<bvar::Status<uint64_t>> leader_switch_time_;
  bvar::MultiDimension<bvar::Status<uint64_t>> leader_switch_count_;
  bvar::MultiDimension<bvar::PerSecondEx<bvar::Adder<uint64_t>>> commit_count_per_second_;
  bvar::MultiDimension<bvar::PerSecondEx<bvar::Adder<uint64_t>>> apply_count_per_second_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_BVAR_METRICS_H_