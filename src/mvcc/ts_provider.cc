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
#include <cstdint>
#include <memory>

#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/meta.pb.h"

DEFINE_uint32(ts_provider_batch_size, 100, "get tso batch size");
DEFINE_uint32(ts_provider_send_retry_num, 8, "send tso request retry num");
DEFINE_uint32(ts_provider_max_retry_num, 16, "get tso max retry num");
DEFINE_uint32(ts_provider_renew_max_retry_num, 16, "renew max retry num");

DEFINE_uint32(ts_provider_clean_dead_interval_ms, 3000, "clean dead interval time");
DEFINE_uint32(ts_provider_batch_ts_stale_interval_ms, 3000, "batch ts stale interval time");

namespace dingodb {

namespace mvcc {

static int64_t ComposeTs(int64_t physical, int64_t logical) { return (physical << 18) + logical; }

BatchTs::BatchTs(int64_t physical, int64_t logical, uint32_t count)
    : physical_(physical), start_ts_(ComposeTs(physical, logical)), end_ts_(ComposeTs(physical, logical + count)) {}

BatchTsList::BatchTsList() {
  head_.store(BatchTs::New());
  tail_.store(head_.load());

  dead_head_.store(BatchTs::New());
  dead_tail_.store(dead_head_.load());
}

BatchTsList::~BatchTsList() {
  {
    BatchTs* node = head_.load();
    while (node != nullptr) {
      BatchTs* next = node->next.load();
      delete node;
      node = next;
    }

    head_.store(nullptr);
    tail_.store(nullptr);
  }

  {
    BatchTs* node = dead_head_.load();
    while (node != nullptr) {
      BatchTs* next = node->next.load();
      delete node;
      node = next;
    }

    dead_head_.store(nullptr);
    dead_tail_.store(nullptr);
  }
}

void BatchTsList::Push(BatchTs* batch_ts) {
  CHECK(batch_ts != nullptr) << "push batch_ts is nullptr.";

  BatchTs* tail = nullptr;
  BatchTs* tail_next = nullptr;

  for (;;) {
    tail = tail_.load();
    tail_next = tail->next.load();

    CHECK(tail != nullptr) << "tail is nullptr.";

    if (tail != tail_.load()) {
      continue;
    }

    if (tail_next != nullptr) {
      tail_.compare_exchange_weak(tail, tail_next);
      continue;
    }

    if (tail->next.compare_exchange_weak(tail_next, batch_ts)) {
      tail_.compare_exchange_weak(tail, batch_ts);
      active_count_.fetch_add(1, std::memory_order_relaxed);
      push_count_.fetch_add(1);
      last_physical_.store(batch_ts->Physical(), std::memory_order_relaxed);
      break;
    }
  }
}

bool BatchTsList::IsStale(BatchTs* batch_ts) {
  return (batch_ts->Physical() + FLAGS_ts_provider_batch_ts_stale_interval_ms) <
         last_physical_.load(std::memory_order_relaxed);
}

int64_t BatchTsList::GetTs() {
  BatchTs* head = nullptr;
  BatchTs* tail = nullptr;
  BatchTs* head_next = nullptr;

  for (;;) {
    head = head_.load();
    tail = tail_.load();
    head_next = head->next.load();

    CHECK(head != nullptr) << "head is nullptr.";

    if (!IsStale(head)) {
      int64_t ts = head->GetTs();
      if (ts > 0) {
        return ts;
      }
    }

    if (head == tail && head_next == nullptr) {
      return 0;
    }

    if (head == tail && head_next != nullptr) {
      tail_.compare_exchange_weak(tail, head_next);
      continue;
    }

    if (head_.compare_exchange_weak(head, head_next)) {
      PushDead(head);
      active_count_.fetch_sub(1, std::memory_order_relaxed);
      pop_count_.fetch_add(1);
    }
  }

  return 0;
}

void BatchTsList::PushDead(BatchTs* batch_ts) {
  batch_ts->next.store(nullptr);
  batch_ts->SetDeadTime(Helper::TimestampMs());

  BatchTs* tail = nullptr;
  BatchTs* tail_next = nullptr;

  for (;;) {
    tail = dead_tail_.load();
    tail_next = tail->next.load();

    CHECK(tail != nullptr) << "dead tail is nullptr.";

    if (tail != dead_tail_.load()) {
      continue;
    }

    if (tail_next != nullptr) {
      dead_tail_.compare_exchange_weak(tail, tail_next);
      continue;
    }

    if (tail->next.compare_exchange_weak(tail_next, batch_ts)) {
      dead_tail_.compare_exchange_weak(tail, batch_ts);
      dead_count_.fetch_add(1, std::memory_order_relaxed);
      dead_push_count_.fetch_add(1);
      break;
    }
  }
}

void BatchTsList::CleanDead() {
  int64_t clean_ms = Helper::TimestampMs() - FLAGS_ts_provider_clean_dead_interval_ms;

  BatchTs* head = nullptr;
  BatchTs* tail = nullptr;
  BatchTs* head_next = nullptr;

  for (;;) {
    head = dead_head_.load();
    tail = dead_tail_.load();
    head_next = head->next.load();

    CHECK(head != nullptr) << "dead head is nullptr.";

    if (head->DeadTime() >= clean_ms) {
      return;
    }

    if (head == tail && head_next == nullptr) {
      return;
    }

    if (head == tail && head_next != nullptr) {
      tail_.compare_exchange_weak(tail, head_next);
      continue;
    }

    if (dead_head_.compare_exchange_weak(head, head_next)) {
      dead_count_.fetch_sub(1, std::memory_order_relaxed);
      dead_pop_count_.fetch_add(1);
      delete head;
    }
  }
}

void BatchTsList::Flush() {
  BatchTs* head = nullptr;
  BatchTs* tail = nullptr;
  BatchTs* head_next = nullptr;

  for (;;) {
    head = head_.load();
    tail = tail_.load();
    head_next = head->next.load();

    head->Flush();

    if (head == tail && head_next == nullptr) {
      return;
    }

    if (head == tail && head_next != nullptr) {
      tail_.compare_exchange_weak(tail, head_next);
      continue;
    }

    if (head_.compare_exchange_weak(head, head_next)) {
      delete head;
      active_count_.fetch_sub(1, std::memory_order_relaxed);
    }
  }
}

uint32_t BatchTsList::ActualCount() {
  uint32_t count = 0;
  BatchTs* node = head_.load();
  while (node) {
    ++count;
    node = node->next.load();
  }

  return count;
}

uint32_t BatchTsList::ActualDeadCount() {
  uint32_t count = 0;
  BatchTs* node = dead_head_.load();
  while (node) {
    ++count;
    node = node->next.load();
  }

  return count;
}

std::string BatchTsList::DebugInfo() {
  return fmt::format(
      "actual_count({})  active_count({}) push_count({}) pop_count({}) actual_dead_count({}) dead_count({}) "
      "dead_push_count({}) dead_pop_count({})",
      ActualCount(), active_count_.load(), push_count_.load(), pop_count_.load(), ActualDeadCount(), dead_count_.load(),
      dead_push_count_.load(), dead_pop_count_.load());
}

bool TsProvider::Init() {
  bool ret = worker_->Init();
  return ret;
}

int64_t TsProvider::GetTs() {
  uint32_t retry_count = 0;
  for (; retry_count < FLAGS_ts_provider_max_retry_num; ++retry_count) {
    int64_t ts = batch_ts_list_->GetTs();
    if (ts > 0) {
      get_ts_count_.fetch_add(1, std::memory_order_relaxed);
      return ts;
    }

    LaunchRenewBatchTs(true);
  }

  if (retry_count == FLAGS_ts_provider_max_retry_num) {
    DINGO_LOG(ERROR) << fmt::format("get ts retry({}) too much.", retry_count);
  }

  get_ts_fail_count_.fetch_add(1, std::memory_order_relaxed);

  return 0;
}

void TsProvider::RenewBatchTs() {
  for (uint32_t retry_count = 0; retry_count < FLAGS_ts_provider_renew_max_retry_num; ++retry_count) {
    BatchTs* batch_ts = SendTsoRequest();
    if (batch_ts == nullptr) {
      bthread_usleep(2000);
      continue;
    }

    batch_ts_list_->Push(batch_ts);
    renew_epoch_.fetch_add(1, std::memory_order_relaxed);
    // clean dead BatchTs
    batch_ts_list_->CleanDead();
    return;
  }

  DINGO_LOG(ERROR) << fmt::format("renew retry({}) too much.", FLAGS_ts_provider_renew_max_retry_num);
}

void TsProvider::LaunchRenewBatchTs(bool is_sync) {
  auto task = std::make_shared<TakeBatchTsTask>(is_sync, RenewEpoch(), GetSelfPtr());
  bool ret = worker_->Execute(task);
  if (!ret) {
    DINGO_LOG(ERROR) << "Launch renew batch ts failed.";
    return;
  }

  if (is_sync) {
    task->Wait();
  }
}

void TsProvider::TriggerRenewBatchTs() { LaunchRenewBatchTs(false); }

std::string TsProvider::DebugInfo() {
  return fmt::format("{} ts_count({}/{}) renew({})", batch_ts_list_->DebugInfo(),
                     get_ts_count_.load(std::memory_order_relaxed), get_ts_fail_count_.load(std::memory_order_relaxed),
                     RenewEpoch());
}

// for test
BatchTs* GenBatchTsForTest() {
  static int64_t physical = Helper::TimestampMs();
  static int64_t logical = 0;

  int64_t now_ms = Helper::TimestampMs();
  mvcc::BatchTs* batch_ts = nullptr;
  if (now_ms > physical) {
    logical = 0;
    physical = now_ms;
  }

  batch_ts = mvcc::BatchTs::New(physical, logical, 100);
  logical += 100;

  return batch_ts;
}

BatchTs* TsProvider::SendTsoRequest() {
  // return GenBatchTsForTest();

  pb::meta::TsoRequest tso_request;
  tso_request.set_op_type(pb::meta::TsoOpType::OP_GEN_TSO);
  tso_request.set_count(FLAGS_ts_provider_batch_size);

  pb::meta::TsoResponse tso_response;
  auto status = interaction_->SendRequest(pb::common::ServiceTypeMeta, "TsoService", tso_request, tso_response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("Get remote ts failed, error: {} {}", status.error_code(), status.error_str());
    return nullptr;
  }

  const auto& timestamp = tso_response.start_timestamp();
  DINGO_LOG(DEBUG) << fmt::format("tso response: {} {} {}", timestamp.physical(), timestamp.logical(),
                                  tso_response.count());

  return BatchTs::New(timestamp.physical(), timestamp.logical(), tso_response.count());
}

}  // namespace mvcc

}  // namespace dingodb
