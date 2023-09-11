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

#ifndef DINGODB_HANDLER_HANDLER_H_
#define DINGODB_HANDLER_HANDLER_H_

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "braft/snapshot.h"
#include "butil/status.h"
#include "common/context.h"
#include "engine/raw_engine.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_metrics_manager.h"
#include "proto/raft.pb.h"

namespace dingodb {

enum class HandlerType {
  // Raft apply log handler
  kPut = pb::raft::PUT,
  kPutIfabsent = pb::raft::PUTIFABSENT,
  kDeleteRange = pb::raft::DELETERANGE,
  kDeleteBatch = pb::raft::DELETEBATCH,
  kSplit = pb::raft::SPLIT,
  kMetaWrite = pb::raft::META_WRITE,
  kCompareAndSet = pb::raft::COMPAREANDSET,
  kSaveSnapshotInApply = pb::raft::SAVE_RAFT_SNAPSHOT,

  // vector
  kVectorAdd = pb::raft::VECTOR_ADD,
  kVectorDelete = pb::raft::VECTOR_DELETE,
  kRebuildVectorIndex = pb::raft::REBUILD_VECTOR_INDEX,

  // txn
  kTxn = pb::raft::TXN,

  // Snapshot
  kSaveSnapshot = 1000,
  kLoadSnapshot = 1001,

  // Vector index
  kVectorIndexLeaderStart = 2000,
  kVectorIndexLeaderStop = 2001,
  kVectorIndexFollowerStart = 2002,
  kVectorIndexFollowerStop = 2003,
};

class Handler {
 public:
  Handler() = default;
  virtual ~Handler() = default;

  virtual HandlerType GetType() = 0;
  // virtual void Handle(std::shared_ptr<Context> ctx, std::shared_ptr<RawEngine> engine,
  //                     const pb::raft::Request &req) = 0;

  virtual int Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                     const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
                     int64_t log_id) = 0;

  virtual int Handle(store::RegionPtr region, std::shared_ptr<RawEngine> engine, int64_t term, int64_t log_index,
                     braft::SnapshotWriter *writer, braft::Closure *done) = 0;
  virtual int Handle(store::RegionPtr region, std::shared_ptr<RawEngine> engine, braft::SnapshotReader *reader) = 0;

  virtual int Handle(store::RegionPtr region, int64_t term_id) = 0;
  virtual int Handle(store::RegionPtr region, butil::Status status) = 0;

  virtual int Handle(store::RegionPtr region, const braft::LeaderChangeContext &ctx) = 0;
};

class BaseHandler : public Handler {
 public:
  BaseHandler() = default;
  ~BaseHandler() override = default;

  // void Handle(std::shared_ptr<Context>, std::shared_ptr<RawEngine>, const pb::raft::Request &) override {
  //   DINGO_LOG(ERROR) << "Not support handle...";
  // }

  int Handle(std::shared_ptr<Context>, store::RegionPtr, std::shared_ptr<RawEngine>, const pb::raft::Request &,
             store::RegionMetricsPtr, int64_t /*term_id*/, int64_t /*log_id*/) override {
    DINGO_LOG(ERROR) << "Not support handle...";
    return 0;
  }

  int Handle(store::RegionPtr, std::shared_ptr<RawEngine>, int64_t, int64_t, braft::SnapshotWriter *,
             braft::Closure *) override {
    DINGO_LOG(ERROR) << "Not support handle...";
    return 0;
  }

  int Handle(store::RegionPtr, std::shared_ptr<RawEngine>, braft::SnapshotReader *) override {
    DINGO_LOG(ERROR) << "Not support handle...";
    return 0;
  }

  int Handle(store::RegionPtr, int64_t) override {
    DINGO_LOG(ERROR) << "Not support handle...";
    return 0;
  }
  int Handle(store::RegionPtr, butil::Status) override {
    DINGO_LOG(ERROR) << "Not support handle...";
    return 0;
  }
  int Handle(store::RegionPtr, const braft::LeaderChangeContext &) override {
    DINGO_LOG(ERROR) << "Not support handle...";
    return 0;
  }
};

// A group hander
class HandlerCollection {
 public:
  HandlerCollection() = default;
  ~HandlerCollection() = default;
  HandlerCollection(const HandlerCollection &) = delete;
  const HandlerCollection &operator=(const HandlerCollection &) = delete;

  void Register(std::shared_ptr<Handler> handler);
  std::shared_ptr<Handler> GetHandler(HandlerType type);
  std::vector<std::shared_ptr<Handler>> GetHandlers();

 private:
  std::unordered_map<HandlerType, std::shared_ptr<Handler>> handlers_;
};

// Build handler factory
class HandlerFactory {
 public:
  HandlerFactory() = default;
  virtual ~HandlerFactory() = default;
  HandlerFactory(const HandlerFactory &) = delete;
  const HandlerFactory &operator=(const HandlerFactory &) = delete;

  virtual std::shared_ptr<HandlerCollection> Build() = 0;
};

}  // namespace dingodb

#endif  // DINGODB_HANDLER_HANDLER_H_