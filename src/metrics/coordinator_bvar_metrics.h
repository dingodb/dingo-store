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

#ifndef DINGODB_COORDINATOR_BVAR_METRICS_H_
#define DINGODB_COORDINATOR_BVAR_METRICS_H_

#include <cstdint>
#include <memory>
#include <string>

#include "bvar/bvar.h"
#include "bvar/multi_dimension.h"
#include "bvar/reducer.h"
#include "bvar/status.h"
#include "bvar/variable.h"
#include "common/helper.h"
#include "metrics/dingo_bvar.h"

namespace dingodb {

class CoordinatorBvarMetricsStore {
 public:
  CoordinatorBvarMetricsStore() : store_metrics_("dingo_metrics_coordinator_store", {"id", "type"}) {
    store_metrics_.expose("dingo_metrics_coordinator_store");
  }
  ~CoordinatorBvarMetricsStore() = default;

  CoordinatorBvarMetricsStore(const CoordinatorBvarMetricsStore &) = delete;
  void operator=(const CoordinatorBvarMetricsStore &) = delete;

  void UpdateStoreBvar(uint64_t store_id, uint64_t total_capacity, uint64_t free_capacity) {
    auto *stats = store_metrics_.get_stats({std::to_string(store_id), "total_capacity"});
    if (stats) {
      stats->set_value(total_capacity);
    }
    auto *stats2 = store_metrics_.get_stats({std::to_string(store_id), "free_capacity"});
    if (stats2) {
      stats2->set_value(free_capacity);
    }
  }

  void DeleteStoreBvar(uint64_t store_id) {
    store_metrics_.delete_stats({std::to_string(store_id), "total_capacity"});
    store_metrics_.delete_stats({std::to_string(store_id), "free_capacity"});
  }

  void Clear() { store_metrics_.delete_stats(); }

 private:
  DingoMultiDimension<bvar::Status<uint64_t>> store_metrics_;
};

class CoordinatorBvarMetricsRegion {
 public:
  CoordinatorBvarMetricsRegion() : region_metrics_("dingo_metrics_coordinator_region", {"id", "type"}) {
    region_metrics_.expose("dingo_metrics_coordinator_region");
  }
  ~CoordinatorBvarMetricsRegion() = default;

  CoordinatorBvarMetricsRegion(const CoordinatorBvarMetricsRegion &) = delete;
  void operator=(const CoordinatorBvarMetricsRegion &) = delete;

  void UpdateRegionBvar(uint64_t region_id, uint64_t region_row_count, uint64_t region_size) {
    auto *stats = region_metrics_.get_stats({std::to_string(region_id), "row_count"});
    if (stats) {
      stats->set_value(region_row_count);
    }
    auto *stats2 = region_metrics_.get_stats({std::to_string(region_id), "size"});
    if (stats2) {
      stats2->set_value(region_size);
    }
  }

  void DeleteRegionBvar(uint64_t region_id) {
    region_metrics_.delete_stats({std::to_string(region_id), "row_count"});
    region_metrics_.delete_stats({std::to_string(region_id), "size"});
  }

  void Clear() { region_metrics_.delete_stats(); }

 private:
  DingoMultiDimension<bvar::Status<uint64_t>> region_metrics_;
};

class CoordinatorBvarMetricsTable {
 public:
  CoordinatorBvarMetricsTable() : table_metrics_("dingo_metrics_coordinator_table", {"id", "type"}) {
    table_metrics_.expose("dingo_metrics_coordinator_table");
  }
  ~CoordinatorBvarMetricsTable() = default;

  CoordinatorBvarMetricsTable(const CoordinatorBvarMetricsTable &) = delete;
  void operator=(const CoordinatorBvarMetricsTable &) = delete;

  void UpdateTableBvar(uint64_t table_id, uint64_t table_row_count, uint64_t table_part_count) {
    auto *stats = table_metrics_.get_stats({std::to_string(table_id), "row_count"});
    if (stats) {
      stats->set_value(table_row_count);
    }
    auto *stats2 = table_metrics_.get_stats({std::to_string(table_id), "part_count"});
    if (stats2) {
      stats2->set_value(table_part_count);
    }
  }

  void DeleteTableBvar(uint64_t table_id) {
    table_metrics_.delete_stats({std::to_string(table_id), "row_count"});
    table_metrics_.delete_stats({std::to_string(table_id), "part_count"});
  }

  void Clear() { table_metrics_.delete_stats(); }

 private:
  DingoMultiDimension<bvar::Status<uint64_t>> table_metrics_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_BVAR_METRICS_H_