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
  DINGO_LOG(INFO) << "PutHandler ...";
  butil::Status status;
  auto &request = req.put();
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
  DINGO_LOG(INFO) << "PutIfAbsentHandler ...";
  butil::Status status;
  std::vector<std::string> put_keys;  // NOLINT
  std::string put_key;                // NOLINT
  auto &request = req.put_if_absent();
  auto writer = engine->NewWriter(request.cf_name());
  if (request.kvs().size() == 1) {
    status = writer->KvPutIfAbsent(request.kvs().Get(0), put_key);
  } else {
    status = writer->KvBatchPutIfAbsent(Helper::PbRepeatedToVector(request.kvs()), put_keys, request.is_atomic());
  }

  if (ctx) {
    ctx->SetStatus(status);
    if (request.kvs().size() != 1) {
      auto *response = dynamic_cast<pb::store::KvBatchPutIfAbsentResponse *>(ctx->Response());
      for (const auto &key : put_keys) {
        response->add_put_keys(key);
      }
    } else {  // only one key
      if ("dingodb.pb.store.KvPutIfAbsentResponse" == ctx->Response()->GetTypeName()) {
        auto *response = dynamic_cast<pb::store::KvPutIfAbsentResponse *>(ctx->Response());
        response->set_put_key(put_key);
      } else {  // dingodb.pb.store.KvBatchPutIfAbsentResponse
        auto *response = dynamic_cast<pb::store::KvBatchPutIfAbsentResponse *>(ctx->Response());
        if (!put_key.empty()) {
          response->add_put_keys(put_key);
        }
      }
    }
  }
}

void DeleteRangeHandler::Handle(std::shared_ptr<Context> ctx, std::shared_ptr<RawEngine> engine,
                                const pb::raft::Request &req) {
  DINGO_LOG(INFO) << "DeleteRangeHandler ...";

  butil::Status status;
  auto &request = req.delete_range();
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
  DINGO_LOG(INFO) << "DeleteBatchHandler ...";

  butil::Status status;
  auto &request = req.delete_batch();
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
