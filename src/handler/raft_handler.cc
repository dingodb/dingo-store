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

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/helper.h"
#include "common/logging.h"
#include "engine/raw_engine.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"

namespace dingodb {

void PutHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                        const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/,
                        uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.put();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->Range();
    for (const auto &kv : request.kvs()) {
      if (range.end_key().compare(kv.key()) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  auto writer = engine->NewWriter(request.cf_name());
  if (request.kvs().size() == 1) {
    status = writer->KvPut(request.kvs().Get(0));
  } else {
    status = writer->KvBatchPut(Helper::PbRepeatedToVector(request.kvs()));
  }

  if (ctx) {
    ctx->SetStatus(status);
  }

  // Update region metrics min/max key
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKey(request.kvs());
  }
}

void PutIfAbsentHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/, uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.put_if_absent();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->Range();
    for (const auto &kv : request.kvs()) {
      if (range.end_key().compare(kv.key()) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  std::vector<bool> key_states;  // NOLINT
  bool key_state;
  auto writer = engine->NewWriter(request.cf_name());
  bool const is_write_batch = (request.kvs().size() != 1);
  if (!is_write_batch) {
    status = writer->KvPutIfAbsent(request.kvs().Get(0), key_state);
  } else {
    status = writer->KvBatchPutIfAbsent(Helper::PbRepeatedToVector(request.kvs()), key_states, request.is_atomic());
  }

  if (ctx) {
    ctx->SetStatus(status);
    if (is_write_batch) {
      auto *response = dynamic_cast<pb::store::KvBatchPutIfAbsentResponse *>(ctx->Response());
      // std::vector<bool> must do not use foreach
      for (auto &&key_state : key_states) {
        response->add_key_states(key_state);
      }
    } else {  // only one key
      pb::store::KvPutIfAbsentResponse *response = dynamic_cast<pb::store::KvPutIfAbsentResponse *>(ctx->Response());
      if (response) {
        response->set_key_state(key_state);
      } else {
        pb::store::KvBatchPutIfAbsentResponse *response =
            dynamic_cast<pb::store::KvBatchPutIfAbsentResponse *>(ctx->Response());
        if (response) {
          response->add_key_states(key_state);
        }
      }
    }
  }

  // Update region metrics min/max key
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKey(request.kvs());
  }
}

void CompareAndSetHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                  std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                  store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/, uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.compare_and_set();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->Range();
    for (const auto &kv : request.kvs()) {
      if (range.end_key().compare(kv.key()) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  std::vector<bool> key_states;  // NOLINT

  auto writer = engine->NewWriter(request.cf_name());
  bool const is_write_batch = (request.kvs().size() != 1);
  status = writer->KvBatchCompareAndSet(Helper::PbRepeatedToVector(request.kvs()),
                                        Helper::PbRepeatedToVector(request.expect_values()), key_states,
                                        request.is_atomic());

  if (ctx) {
    ctx->SetStatus(status);
    if (is_write_batch) {
      auto *response = dynamic_cast<pb::store::KvBatchCompareAndSetResponse *>(ctx->Response());
      // std::vector<bool> must do not use foreach
      for (auto &&key_state : key_states) {
        response->add_key_states(key_state);
      }
    } else {  // only one key
      pb::store::KvCompareAndSetResponse *response =
          dynamic_cast<pb::store::KvCompareAndSetResponse *>(ctx->Response());
      if (response) {
        response->set_key_state(key_states[0]);
      } else {
        pb::store::KvBatchCompareAndSetResponse *response =
            dynamic_cast<pb::store::KvBatchCompareAndSetResponse *>(ctx->Response());
        if (response) {
          response->add_key_states(key_states[0]);
        }
      }
    }
  }

  // Update region metrics min/max key
  if (region_metrics != nullptr) {
    size_t i = 0;
    store::RegionMetrics::PbKeyValues new_kvs;
    store::RegionMetrics::PbKeys delete_keys;
    for (const auto &key_state : key_states) {
      const auto &kv = request.kvs().at(i);
      if (key_state) {
        if (!request.expect_values(i).empty() && kv.value().empty()) {
          delete_keys.Add(std::string(kv.key()));
        }

        if (request.expect_values(i).empty() && !kv.value().empty()) {
          new_kvs.Add(pb::common::KeyValue(kv));
        }
      }
    }

    // add
    region_metrics->UpdateMaxAndMinKey(new_kvs);
    // delete key
    region_metrics->UpdateMaxAndMinKeyPolicy(delete_keys);
  }
}

void DeleteRangeHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/, uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.delete_range();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->Range();
    for (const auto &delete_range : request.ranges()) {
      if (range.end_key().compare(delete_range.end_key()) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  auto reader = engine->NewReader(request.cf_name());
  auto writer = engine->NewWriter(request.cf_name());
  uint64_t delete_count = 0;
  if (1 == request.ranges().size()) {
    uint64_t internal_delete_count = 0;
    const auto &range = request.ranges()[0];
    status = reader->KvCount(range.start_key(), range.end_key(), internal_delete_count);
    if (status.ok() && 0 != internal_delete_count) {
      status = writer->KvDeleteRange(range);
    }
    delete_count = internal_delete_count;
  } else {
    auto snapshot = engine->GetSnapshot();
    for (const auto &range : request.ranges()) {
      uint64_t internal_delete_count = 0;
      status = reader->KvCount(snapshot, range.start_key(), range.end_key(), internal_delete_count);
      if (!status.ok()) {
        delete_count = 0;
        break;
      }
      delete_count += internal_delete_count;
    }

    if (status.ok() && 0 != delete_count) {
      status = writer->KvBatchDeleteRange(Helper::PbRepeatedToVector(request.ranges()));
    }
  }

  if (ctx && ctx->Response()) {
    auto *response = dynamic_cast<pb::store::KvDeleteRangeResponse *>(ctx->Response());
    if (response) {
      ctx->SetStatus(status);
      response->set_delete_count(delete_count);
    }
  }

  // Update region metrics min/max key policy
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKeyPolicy(request.ranges());
  }
}

void DeleteBatchHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/, uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.delete_batch();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->Range();
    for (const auto &key : request.keys()) {
      if (range.end_key().compare(key) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  auto reader = engine->NewReader(request.cf_name());
  std::vector<bool> key_states(request.keys().size(), false);
  auto snapshot = engine->GetSnapshot();
  size_t i = 0;
  for (const auto &key : request.keys()) {
    std::string value;
    status = reader->KvGet(snapshot, key, value);
    if (status.ok()) {
      key_states[i] = true;
    }
    i++;
  }

  auto writer = engine->NewWriter(request.cf_name());
  if (request.keys().size() == 1) {
    status = writer->KvDelete(request.keys().Get(0));
  } else {
    status = writer->KvBatchDelete(Helper::PbRepeatedToVector(request.keys()));
  }

  if (ctx && ctx->Response()) {
    auto *response = dynamic_cast<pb::store::KvBatchDeleteResponse *>(ctx->Response());
    ctx->SetStatus(status);
    for (const auto &state : key_states) {
      response->add_key_states(state);
    }
  }

  // Update region metrics min/max key policy
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKeyPolicy(request.keys());
  }
}

void SplitHandler::SplitClosure::Run() {
  std::unique_ptr<SplitClosure> self_guard(this);
  if (!status().ok()) {
    DINGO_LOG(ERROR) << fmt::format("split region {}, finish snapshot failed", region_->Id());
  } else {
    DINGO_LOG(INFO) << fmt::format("split region {}, finish snapshot success", region_->Id());
  }

  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  if (is_child_) {
    if (status().ok()) {
      store_region_meta->UpdateState(region_, pb::common::StoreRegionState::NORMAL);
    }

  } else {
    store_region_meta->UpdateState(region_, pb::common::StoreRegionState::NORMAL);
    Heartbeat::TriggerStoreHeartbeat(region_->Id());
  }
}

void SplitHandler::Handle(std::shared_ptr<Context>, store::RegionPtr from_region, std::shared_ptr<RawEngine>,
                          const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/,
                          uint64_t /*log_id*/) {
  const auto &request = req.split();
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();

  auto to_region = store_region_meta->GetRegion(request.to_region_id());
  if (to_region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("split region {} to {}, child region not found", request.from_region_id(),
                                    request.to_region_id());
    return;
  }

  DINGO_LOG(DEBUG) << fmt::format("split region {} to {}, begin...", from_region->Id(), to_region->Id());
  if (to_region->State() != pb::common::StoreRegionState::STANDBY) {
    DINGO_LOG(WARNING) << fmt::format("split region {} to {}, child region state is not standby", from_region->Id(),
                                      to_region->Id());
    return;
  }

  // Set region state spliting
  store_region_meta->UpdateState(from_region, pb::common::StoreRegionState::SPLITTING);

  pb::common::Range to_range;
  // Set child range
  to_range.set_start_key(request.split_key());
  to_range.set_end_key(to_region->Range().end_key());
  if (to_range.end_key().compare(request.split_key()) < 0) {
    to_range.set_end_key(from_region->Range().end_key());
  }
  Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->UpdateRange(to_region, to_range);

  // Set parent range
  pb::common::Range from_range;
  from_range.set_start_key(from_region->Range().start_key());
  from_range.set_end_key(request.split_key());
  Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->UpdateRange(from_region, from_range);
  DINGO_LOG(DEBUG) << fmt::format("split region {} to {}, from region range[{}-{}] to region range[{}-{}]",
                                  from_region->Id(), to_region->Id(), Helper::StringToHex(from_range.start_key()),
                                  Helper::StringToHex(from_range.end_key()), Helper::StringToHex(to_range.start_key()),
                                  Helper::StringToHex(to_range.end_key()));

  DINGO_LOG(DEBUG) << fmt::format("split region {} to {}, parent do snapshot", from_region->Id(), to_region->Id());
  // Do parent region snapshot
  auto engine = Server::GetInstance()->GetEngine();
  std::shared_ptr<Context> from_ctx = std::make_shared<Context>();
  from_ctx->SetDone(new SplitHandler::SplitClosure(from_region, false));
  engine->DoSnapshot(from_ctx, from_region->Id());

  DINGO_LOG(DEBUG) << fmt::format("split region {} to {}, child do snapshot", from_region->Id(), to_region->Id());
  // Do child region snapshot
  std::shared_ptr<Context> to_ctx = std::make_shared<Context>();
  to_ctx->SetDone(new SplitHandler::SplitClosure(to_region, true));
  engine->DoSnapshot(to_ctx, to_region->Id());
  Heartbeat::TriggerStoreHeartbeat(to_region->Id());

  // Update region metrics min/max key policy
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKeyPolicy();
  }
}

void VectorAddHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                              const pb::raft::Request &req, store::RegionMetricsPtr /*region_metrics*/,
                              uint64_t /*term_id*/, uint64_t log_id) {
  butil::Status status;
  const auto &request = req.vector_add();
  // region is spliting, check key out range
  // if (region->State() == pb::common::StoreRegionState::SPLITTING) {
  //   const auto &range = region->Range();
  //   for (const auto &kv : request.kvs()) {
  //     if (range.end_key().compare(kv.key()) <= 0) {
  //       if (ctx) {
  //         status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
  //         ctx->SetStatus(status);
  //       }
  //       return;
  //     }
  //   }
  // }

  auto writer = engine->NewWriter(request.cf_name());
  if (request.vectors_size() > 0) {
    status = writer->VectorAdd(region->Id(), log_id, Helper::PbRepeatedToVector(request.vectors()));
  }

  if (ctx) {
    ctx->SetStatus(status);
  }

  // Update region metrics min/max key
  // if (region_metrics != nullptr) {
  //   region_metrics->UpdateMaxAndMinKey(request.kvs());
  // }
}

void VectorDeleteHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                 std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                 store::RegionMetricsPtr /*region_metrics*/, uint64_t /*term_id*/, uint64_t log_id) {
  butil::Status status;
  const auto &request = req.vector_delete();
  // region is spliting, check key out range
  // if (region->State() == pb::common::StoreRegionState::SPLITTING) {
  //   const auto &range = region->Range();
  //   for (const auto &key : request.keys()) {
  //     if (range.end_key().compare(key) <= 0) {
  //       if (ctx) {
  //         status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
  //         ctx->SetStatus(status);
  //       }
  //       return;
  //     }
  //   }
  // }

  auto writer = engine->NewWriter(request.cf_name());
  if (request.ids_size() > 0) {
    std::vector<uint64_t> ids;
    for (const auto &id : request.ids()) {
      ids.push_back(id);
    }
    status = writer->VectorDelete(region->Id(), log_id, ids);
  }

  if (ctx) {
    ctx->SetStatus(status);
  }

  // Update region metrics min/max key
  // if (region_metrics != nullptr) {
  //   region_metrics->UpdateMaxAndMinKey(request.kvs());
  // }
}

std::shared_ptr<HandlerCollection> RaftApplyHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  handler_collection->Register(std::make_shared<PutHandler>());
  handler_collection->Register(std::make_shared<PutIfAbsentHandler>());
  handler_collection->Register(std::make_shared<DeleteRangeHandler>());
  handler_collection->Register(std::make_shared<DeleteBatchHandler>());
  handler_collection->Register(std::make_shared<SplitHandler>());
  handler_collection->Register(std::make_shared<CompareAndSetHandler>());
  handler_collection->Register(std::make_shared<VectorAddHandler>());
  handler_collection->Register(std::make_shared<VectorDeleteHandler>());

  return handler_collection;
}

}  // namespace dingodb
