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

#include <unordered_map>

#include "braft/snapshot.h"
#include "common/context.h"
#include "engine/raw_engine.h"
#include "proto/raft.pb.h"

namespace dingodb {

enum class HandlerType {
  // Raft apply log handler
  PUT = pb::raft::PUT,
  PUTIFABSENT = pb::raft::PUTIFABSENT,
  DELETERANGE = pb::raft::DELETERANGE,
  DELETEBATCH = pb::raft::DELETEBATCH,
  META_WRITE = pb::raft::META_WRITE,

  // Snapshot
  SAVE_SNAPSHOT = 1000,
  LOAD_SNAPSHOT = 1001,
};

class Handler {
 public:
  Handler() = default;
  virtual ~Handler() = default;

  virtual HandlerType GetType() = 0;
  virtual void Handle(std::shared_ptr<Context> ctx, std::shared_ptr<RawEngine> engine,
                      const pb::raft::Request &req) = 0;

  virtual void Handle(uint64_t region_id, std::shared_ptr<RawEngine> engine, braft::SnapshotWriter *writer,
                      braft::Closure *done) = 0;
  virtual void Handle(uint64_t region_id, std::shared_ptr<RawEngine> engine, braft::SnapshotReader *reader) = 0;
};

class BaseHandler : public Handler {
 public:
  BaseHandler() = default;
  ~BaseHandler() override = default;

  void Handle(std::shared_ptr<Context> /*ctx*/, std::shared_ptr<RawEngine> /*engine*/,
              const pb::raft::Request & /*req*/) override {
    DINGO_LOG(ERROR) << "Not support handle...";
  }

  void Handle(uint64_t /*region_id*/, std::shared_ptr<RawEngine> /*engine*/, braft::SnapshotWriter * /*writer*/,
              braft::Closure * /*done*/) override {
    DINGO_LOG(ERROR) << "Not support handle...";
  }

  void Handle(uint64_t /*region_id*/, std::shared_ptr<RawEngine> /*engine*/,
              braft::SnapshotReader * /*reader*/) override {
    DINGO_LOG(ERROR) << "Not support handle...";
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