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

#include "handler/raft_handler.h"

#include "common/helper.h"
#include "common/logging.h"

namespace dingodb {

void PutHandler::Handle(std::shared_ptr<Context> ctx, std::shared_ptr<RawEngine> engine, const pb::raft::Request &req) {
  butil::Status status;
  const auto &request = req.put();
  auto writer = engine->NewWriter(request.cf_name());
  if (request.kvs().size() == 1) {
    status = writer->KvPut(request.kvs().Get(0));
  } else {
    status = writer->KvBatchPut(Helper::PbRepeatedToVector(request.kvs()));
  }

  if (ctx) {
    ctx->SetStatus(status);
  }
}

void PutIfAbsentHandler::Handle(std::shared_ptr<Context> ctx, std::shared_ptr<RawEngine> engine,
                                const pb::raft::Request &req) {
  butil::Status status;
  std::vector<bool> key_states;  // NOLINT
  bool key_state;
  const auto &request = req.put_if_absent();
  auto writer = engine->NewWriter(request.cf_name());
  bool const is_write_batch = (request.kvs().size() == 1);
  if (!is_write_batch) {
    status = writer->KvPutIfAbsent(request.kvs().Get(0), key_state);
  } else {
    status = writer->KvBatchPutIfAbsent(Helper::PbRepeatedToVector(request.kvs()), key_states, request.is_atomic());
  }

  if (ctx) {
    ctx->SetStatus(status);
    if (request.kvs().size() != 1) {
      auto *response = dynamic_cast<pb::store::KvBatchPutIfAbsentResponse *>(ctx->Response());
      // std::vector<bool> must do not use foreach
      for (auto &&key_state : key_states) {
        response->add_key_states(key_state);
      }
    } else {  // only one key
      if (!is_write_batch) {
        auto *response = dynamic_cast<pb::store::KvPutIfAbsentResponse *>(ctx->Response());
        response->set_key_state(key_state);
      } else {
        auto *response = dynamic_cast<pb::store::KvBatchPutIfAbsentResponse *>(ctx->Response());
        response->add_key_states(key_state);
      }
    }
  }
}

void DeleteRangeHandler::Handle(std::shared_ptr<Context> ctx, std::shared_ptr<RawEngine> engine,
                                const pb::raft::Request &req) {
  butil::Status status;
  const auto &request = req.delete_range();
  auto writer = engine->NewWriter(request.cf_name());
  for (const auto &range : request.ranges()) {
    status = writer->KvDeleteRange(range);
    if (!status.ok()) {
      break;
    }
  }

  if (ctx) {
    ctx->SetStatus(status);
  }
}

void DeleteBatchHandler::Handle(std::shared_ptr<Context> ctx, std::shared_ptr<RawEngine> engine,
                                const pb::raft::Request &req) {
  butil::Status status;
  const auto &request = req.delete_batch();
  auto writer = engine->NewWriter(request.cf_name());
  if (request.keys().size() == 1) {
    status = writer->KvDelete(request.keys().Get(0));
  } else {
    status = writer->KvDeleteBatch(Helper::PbRepeatedToVector(request.keys()));
  }

  if (ctx) {
    ctx->SetStatus(status);
  }
}

std::shared_ptr<HandlerCollection> RaftApplyHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  handler_collection->Register(std::make_shared<PutHandler>());
  handler_collection->Register(std::make_shared<PutIfAbsentHandler>());
  handler_collection->Register(std::make_shared<DeleteRangeHandler>());
  handler_collection->Register(std::make_shared<DeleteBatchHandler>());

  return handler_collection;
}

}  // namespace dingodb
