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

#include "common/context.h"
#include "engine/raw_engine.h"
#include "handler/handler.h"
#include "proto/raft.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

// PutRequest
class PutHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kPut; }
  void Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
              const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t term_id,
              uint64_t log_id) override;
};

// PutIfAbsentRequest
class PutIfAbsentHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kPutIfabsent; }
  void Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
              const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t term_id,
              uint64_t log_id) override;
};

// CompareAndSetRequest
class CompareAndSetHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kCompareAndSet; }
  void Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
              const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t term_id,
              uint64_t log_id) override;
};

// DeleteRangeRequest
class DeleteRangeHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kDeleteRange; }
  void Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
              const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t term_id,
              uint64_t log_id) override;
};

// DeleteBatchRequest
class DeleteBatchHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kDeleteBatch; }
  void Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
              const pb::raft::Request &req, store::RegionMetricsPtr region_metricss, uint64_t term_id,
              uint64_t log_id) override;
};

// SplitHandler
class SplitHandler : public BaseHandler {
 public:
  class SplitClosure : public braft::Closure {
   public:
    SplitClosure(store::RegionPtr region, bool is_child) : region_(region), is_child_(is_child) {}
    ~SplitClosure() override = default;

    void Run() override;

   private:
    bool is_child_;
    store::RegionPtr region_;
  };

  HandlerType GetType() override { return HandlerType::kSplit; }
  void Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
              const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t term_id,
              uint64_t log_id) override;
};

// VectorAddRequest
class VectorAddHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kVectorAdd; }
  void Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
              const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t term_id,
              uint64_t log_id) override;
};

// VectorDeleteRequest
class VectorDeleteHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kVectorDelete; }
  void Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
              const pb::raft::Request &req, store::RegionMetricsPtr region_metricss, uint64_t term_id,
              uint64_t log_id) override;
};

class RaftApplyHandlerFactory : public HandlerFactory {
 public:
  std::shared_ptr<HandlerCollection> Build() override;
};

}  // namespace dingodb

#endif  // DINGODB_HANDLER_RAFT_HANDLER_H_