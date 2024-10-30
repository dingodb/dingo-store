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

#ifndef DINGODB_COMMON_TRACKER_H_
#define DINGODB_COMMON_TRACKER_H_

#include <cstdint>
#include <memory>

#include "bvar/latency_recorder.h"
#include "common/helper.h"
#include "proto/common.pb.h"

namespace dingodb {

class Tracker {
 public:
  Tracker(const pb::common::RequestInfo& request_info) : request_info_(request_info) {
    start_time_ = Helper::TimestampNs();
    last_time_ = start_time_;
  }
  ~Tracker() = default;

  static std::shared_ptr<Tracker> New(const pb::common::RequestInfo& request_info) {
    return std::make_shared<Tracker>(request_info);
  }

  struct Metrics {
    uint64_t total_rpc_time_ns{0};
    uint64_t service_queue_wait_time_ns{0};
    uint64_t prepair_commit_time_ns{0};
    uint64_t raft_commit_time_ns{0};
    uint64_t raft_queue_wait_time_ns{0};
    uint64_t raft_apply_time_ns{0};
    uint64_t store_write_time_ns{0};
    uint64_t vector_index_write_time_ns{0};
    uint64_t document_index_write_time_ns{0};

    uint64_t read_store_time_ns{0};
  };

  void SetTotalRpcTime() { metrics_.total_rpc_time_ns = Helper::TimestampNs() - start_time_; }
  uint64_t TotalRpcTime() const { return metrics_.total_rpc_time_ns; }

  void SetServiceQueueWaitTime() {
    uint64_t now_time = Helper::TimestampNs();
    metrics_.service_queue_wait_time_ns = now_time - last_time_;
    last_time_ = now_time;

    service_queue_latency << metrics_.service_queue_wait_time_ns / 1000;
  }
  uint64_t ServiceQueueWaitTime() const { return metrics_.service_queue_wait_time_ns; }

  void SetPrepairCommitTime() {
    uint64_t now_time = Helper::TimestampNs();
    metrics_.prepair_commit_time_ns = now_time - last_time_;
    last_time_ = now_time;

    prepair_commit_latency << metrics_.prepair_commit_time_ns / 1000;
  }
  uint64_t PrepairCommitTime() const { return metrics_.prepair_commit_time_ns; }

  void SetRaftCommitTime() {
    uint64_t now_time = Helper::TimestampNs();
    metrics_.raft_commit_time_ns = now_time - last_time_;
    last_time_ = now_time;

    raft_commit_latency << metrics_.raft_commit_time_ns / 1000;
  }
  uint64_t RaftCommitTime() const { return metrics_.raft_commit_time_ns; }

  void SetRaftQueueWaitTime() {
    uint64_t now_time = Helper::TimestampNs();
    metrics_.raft_queue_wait_time_ns = now_time - last_time_;
    last_time_ = now_time;

    raft_queue_wait_latency << metrics_.raft_queue_wait_time_ns / 1000;
  }
  uint64_t RaftQueueWaitTime() const { return metrics_.raft_queue_wait_time_ns; }

  void SetRaftApplyTime() {
    uint64_t now_time = Helper::TimestampNs();
    metrics_.raft_apply_time_ns = now_time - last_time_;
    last_time_ = now_time;

    raft_apply_latency << metrics_.raft_apply_time_ns / 1000;
  }
  uint64_t RaftApplyTime() const { return metrics_.raft_apply_time_ns; }

  void SetStoreWriteTime(uint64_t elapsed_time) {
    metrics_.store_write_time_ns = elapsed_time;
    store_write_latency << metrics_.store_write_time_ns / 1000;
  }
  uint64_t StoreWriteTime() const { return metrics_.store_write_time_ns; }

  void SetVectorIndexWriteTime(uint64_t elapsed_time) {
    metrics_.vector_index_write_time_ns = elapsed_time;
    vector_index_write_latency << metrics_.vector_index_write_time_ns / 1000;
  }
  uint64_t VectorIndexwriteTime() const { return metrics_.vector_index_write_time_ns; }

  void SetDocumentIndexWriteTime(uint64_t elapsed_time) {
    metrics_.document_index_write_time_ns = elapsed_time;
    document_index_write_latency << metrics_.document_index_write_time_ns / 1000;
  }
  uint64_t DocumentIndexwriteTime() const { return metrics_.document_index_write_time_ns; }

  void SetReadStoreTime() {
    uint64_t now_time = Helper::TimestampNs();
    metrics_.read_store_time_ns = now_time - last_time_;
    last_time_ = now_time;

    read_store_latency << metrics_.read_store_time_ns / 1000;
  }
  inline uint64_t ReadStoreTime() const { return metrics_.read_store_time_ns; }

  // latency statistics
  static bvar::LatencyRecorder service_queue_latency;
  static bvar::LatencyRecorder prepair_commit_latency;
  static bvar::LatencyRecorder raft_commit_latency;
  static bvar::LatencyRecorder raft_queue_wait_latency;
  static bvar::LatencyRecorder raft_apply_latency;
  static bvar::LatencyRecorder read_store_latency;

  static bvar::LatencyRecorder store_write_latency;
  static bvar::LatencyRecorder vector_index_write_latency;
  static bvar::LatencyRecorder document_index_write_latency;

 private:
  uint64_t start_time_;
  uint64_t last_time_;

  pb::common::RequestInfo request_info_;
  Metrics metrics_;
};
using TrackerPtr = std::shared_ptr<Tracker>;

}  // namespace dingodb

#endif  // DINGODB_COMMON_TRACKER_H_