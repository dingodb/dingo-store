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

#ifndef DINGODB_HANDLER_RAFT_HANDLER_H_
#define DINGODB_HANDLER_RAFT_HANDLER_H_

#include <cstdint>
#include <memory>

#include "common/context.h"
#include "engine/raw_engine.h"
#include "handler/handler.h"
#include "proto/raft.pb.h"

namespace dingodb {

// PutRequest
class PutHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kPut; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
             int64_t log_id) override;
};

// DeleteRangeRequest
class DeleteRangeHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kDeleteRange; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
             int64_t log_id) override;
};

// DeleteBatchRequest
class DeleteBatchHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kDeleteBatch; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metricss, int64_t term_id,
             int64_t log_id) override;
};

// SplitHandler
class SplitHandler : public BaseHandler {
 public:
  class SplitClosure : public braft::Closure {
   public:
    SplitClosure(int64_t region_id) : region_id_(region_id) {
      DINGO_LOG(DEBUG) << fmt::format("[new.SplitClosure][id({})]", region_id_);
    }
    ~SplitClosure() override { DINGO_LOG(DEBUG) << fmt::format("[delete.SplitClosure][id({})]", region_id_); }

    void Run() override;

   private:
    int64_t region_id_;
  };

  HandlerType GetType() override { return HandlerType::kSplit; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
             int64_t log_id) override;
};

// Handle raft command PrepareMergeRequest
class PrepareMergeHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kPrepareMerge; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr source_region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
             int64_t log_id) override;
};

// Handle raft command CommitMergeRequest
class CommitMergeHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kCommitMerge; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr target_region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
             int64_t log_id) override;
};

// Handle raft command RollbackMergeRequest
class RollbackMergeHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kRollbackMerge; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
             int64_t log_id) override;
};

// SaveRaftSnapshotHandler
class SaveRaftSnapshotHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kSaveSnapshotInApply; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
             int64_t log_id) override;
};

// VectorAddRequest
class VectorAddHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kVectorAdd; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
             int64_t log_id) override;
};

// VectorDeleteRequest
class VectorDeleteHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kVectorDelete; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metricss, int64_t term_id,
             int64_t log_id) override;
};

// Rebuild vector index handler
class RebuildVectorIndexHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kRebuildVectorIndex; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metricss, int64_t term_id,
             int64_t log_id) override;
};

class TxnHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kTxn; }
  int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
             const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
             int64_t log_id) override;

  static void HandleMultiCfPutAndDeleteRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                               std::shared_ptr<RawEngine> engine,
                                               const pb::raft::MultiCfPutAndDeleteRequest &request,
                                               store::RegionMetricsPtr region_metrics, int64_t term_id, int64_t log_id);

  static void HandleTxnDeleteRangeRequest(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                          std::shared_ptr<RawEngine> engine,
                                          const pb::raft::TxnDeleteRangeRequest &request,
                                          store::RegionMetricsPtr region_metrics, int64_t term_id, int64_t log_id);
};

class RaftApplyHandlerFactory : public HandlerFactory {
 public:
  std::shared_ptr<HandlerCollection> Build() override;
};

}  // namespace dingodb

#endif  // DINGODB_HANDLER_RAFT_HANDLER_H_