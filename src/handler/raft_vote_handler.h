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

#ifndef DINGO_VECTOR_INDEX_HANDLER_H_
#define DINGO_VECTOR_INDEX_HANDLER_H_

#include "handler/handler.h"

namespace dingodb {

// VectorIndexLeaderStart
class VectorIndexLeaderStartHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kVectorIndexLeaderStart; }
  int Handle(store::RegionPtr region, int64_t term_id) override;
};

// VectorIndexLeaderStop
class VectorIndexLeaderStopHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kVectorIndexLeaderStop; }
  int Handle(store::RegionPtr region, butil::Status status) override;
};

// VectorIndexFollowerStart
class VectorIndexFollowerStartHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kVectorIndexFollowerStart; }
  int Handle(store::RegionPtr region, const braft::LeaderChangeContext &ctx) override;
};

// VectorIndexFollowerStop
class VectorIndexFollowerStopHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kVectorIndexFollowerStop; }
  int Handle(store::RegionPtr region, const braft::LeaderChangeContext &ctx) override;
};

// Leader start handler collection
class LeaderStartHandlerFactory : public HandlerFactory {
 public:
  std::shared_ptr<HandlerCollection> Build() override;
};

// Leader stop handler collection
class LeaderStopHandlerFactory : public HandlerFactory {
 public:
  std::shared_ptr<HandlerCollection> Build() override;
};

// Follower start handler collection
class FollowerStartHandlerFactory : public HandlerFactory {
 public:
  std::shared_ptr<HandlerCollection> Build() override;
};

// Follower stop handler collection
class FollowerStopHandlerFactory : public HandlerFactory {
 public:
  std::shared_ptr<HandlerCollection> Build() override;
};

}  // namespace dingodb

#endif  // DINGO_VECTOR_INDEX_HANDLER_H_