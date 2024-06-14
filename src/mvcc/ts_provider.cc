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

#include "mvcc/ts_provider.h"

#include <atomic>

#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "fmt/core.h"
#include "proto/meta.pb.h"

namespace dingodb {

namespace mvcc {

bool TsProvider::Init() {
  return true;

  BatchTs* batch_ts = new BatchTs();
  if (!TakeBatchTsFromTSO(*batch_ts)) {
    return false;
  }

  head_.store(batch_ts);

  return true;
}

void TsProvider::AddBatchTs(const BatchTs& batch_ts) {
  BatchTs* new_batch_ts = new BatchTs();
  new_batch_ts->start_ts.store(batch_ts.start_ts.load(std::memory_order_relaxed), std::memory_order_relaxed);
  new_batch_ts->end_ts = batch_ts.end_ts;
  new_batch_ts->next = nullptr;

  AddBatchTs(new_batch_ts);
}

void TsProvider::AddBatchTs(BatchTs* batch_ts) {
  std::atomic<BatchTs*>* head = &head_;
  while (head->load(std::memory_order_relaxed) != nullptr) {
    BatchTs* temp = nullptr;
    if (head->load()->next.compare_exchange_weak(temp, batch_ts)) {
      return;
    } else {
      head = &head->load()->next;
    }
  }
}

void TsProvider::TakeBatchTsFromTSOWrapper() {
  int taking_count = taking_count_.fetch_add(1, std::memory_order_relaxed);
  if (taking_count == 1) {
    BatchTs batch_ts;
    bool ret = TakeBatchTsFromTSO(batch_ts);
    if (ret) {
      AddBatchTs(batch_ts);
    }
    cond_.DecreaseBroadcast();
  } else {
    cond_.TimedWait(10);
  }

  taking_count_.fetch_sub(1);
}

bool TsProvider::TakeBatchTsFromTSO(BatchTs& batch_ts) {
  pb::meta::TsoRequest tso_request;

  tso_request.set_op_type(pb::meta::TsoOpType::OP_GEN_TSO);
  tso_request.set_count(100);

  pb::meta::TsoResponse tso_response;
  auto status = coordinator_interaction_->SendRequest("TsoRequest", tso_request, tso_response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("Get remote ts failed, error: {} {}", status.error_code(), status.error_str());
    return false;
  }

  return true;
}

}  // namespace mvcc

}  // namespace dingodb
