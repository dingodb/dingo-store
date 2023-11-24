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

#ifndef DINGODB_STATE_MACHINE_H_
#define DINGODB_STATE_MACHINE_H_

#include "braft/raft.h"
#include "common/context.h"
#include "proto/raft.pb.h"

namespace dingodb {

class BaseClosure : public braft::Closure {
 public:
  BaseClosure(std::shared_ptr<Context> ctx, std::shared_ptr<pb::raft::RaftCmdRequest> request)
      : ctx_(ctx), request_(request) {}
  BaseClosure(std::shared_ptr<Context> ctx) : ctx_(ctx) {}
  ~BaseClosure() override = default;

  void Run() override;

  std::shared_ptr<Context> GetCtx() { return ctx_; }
  std::shared_ptr<pb::raft::RaftCmdRequest> GetRequest() { return request_; }

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::raft::RaftCmdRequest> request_;
};

// Do snapshot closure
class RaftSnapshotClosure : public braft::Closure {
 public:
  RaftSnapshotClosure(std::shared_ptr<Context> ctx) : ctx_(ctx) {}
  ~RaftSnapshotClosure() override = default;

  void Run() override;

 private:
  std::shared_ptr<Context> ctx_;
};

// Base state machine for store and meta inherit
class BaseStateMachine : public braft::StateMachine {
 public:
  BaseStateMachine() = default;
  ~BaseStateMachine() override = default;

  virtual int64_t GetAppliedIndex() const = 0;
  virtual int64_t GetLastSnapshotIndex() const = 0;

  virtual bool MaySaveSnapshot();
};

}  // namespace dingodb

#endif  // DINGODB_STATE_MACHINE_H_
