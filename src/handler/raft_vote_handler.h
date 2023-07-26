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
  void Handle(store::RegionPtr region, uint64_t term_id) override;
};

// VectorIndexLeaderStop
class VectorIndexLeaderStopHandler : public BaseHandler {
 public:
  HandlerType GetType() override { return HandlerType::kVectorIndexLeaderStop; }
  void Handle(store::RegionPtr region, butil::Status status) override;
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

}  // namespace dingodb

#endif  // DINGO_VECTOR_INDEX_HANDLER_H_