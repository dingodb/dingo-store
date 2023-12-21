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

#include "common/latch.h"

#include <cassert>
#include <utility>

#include "butil/scoped_lock.h"
#include "gflags/gflags.h"

namespace dingodb {

DEFINE_uint32(latch_slot_num, 2048, "latch slot num");

const size_t kWaitingListShrinkSize = 8;
const size_t kWaitingListMaxCapacity = 16;

std::optional<uint64_t> Latch::GetFirstReqByHash(uint64_t hash) {
  for (const auto& [h, cid] : waiting) {
    if (h == hash) {
      return cid;
    }
  }
  return std::nullopt;
}

std::optional<std::pair<uint64_t, uint64_t>> Latch::PopFront(uint64_t key_hash) {
  if (!waiting.empty() && waiting.front().first == key_hash) {
    auto item = waiting.front();
    waiting.pop_front();
    MaybeShrink();
    return item;
  }
  for (auto it = waiting.begin(); it != waiting.end(); ++it) {
    if (it->first == key_hash) {
      auto item = *it;
      waiting.erase(it);
      return item;
    }
  }
  return std::nullopt;
}

void Latch::WaitForWake(uint64_t key_hash, uint64_t cid) { waiting.push_back(std::make_pair(key_hash, cid)); }

void Latch::PushPreemptive(uint64_t key_hash, uint64_t cid) { waiting.push_front(std::make_pair(key_hash, cid)); }

void Latch::MaybeShrink() {
  waiting.erase(std::remove_if(waiting.begin(), waiting.end(), [](const auto& item) { return item.first == 0; }),
                waiting.end());

  if (waiting.size() > kWaitingListMaxCapacity && waiting.size() < kWaitingListShrinkSize) {
    waiting.shrink_to_fit();
  }
}

Lock::Lock(const std::vector<std::string>& keys) {
  for (const auto& key : keys) {
    requiredHashes.push_back(Hash(key));
  }
  std::sort(requiredHashes.begin(), requiredHashes.end());
  auto last = std::unique(requiredHashes.begin(), requiredHashes.end());
  requiredHashes.erase(last, requiredHashes.end());
}

bool Lock::Acquired() const { return requiredHashes.size() == ownedCount; }

void Lock::ForceAssumeAcquired() { ownedCount = requiredHashes.size(); }

bool Lock::IsWriteLock() const { return !requiredHashes.empty(); }

uint64_t Lock::Hash(const std::string& key) {
  // Simple hashing for demonstration. In production, use a better hash function.
  return std::hash<std::string>{}(key);
}

Latches::Latches(size_t size) {
  slots_size = NextPowerOfTwo(size);
  slots_ptr = new std::vector<Slot>(slots_size);
}

Latches::Latches() {
  slots_size = NextPowerOfTwo(FLAGS_latch_slot_num);
  slots_ptr = new std::vector<Slot>(slots_size);
}

Latches::~Latches() { delete slots_ptr; }

// CAUTION: this function is not safe, need to call before any usage begin
void Latches::SetSlotNum(size_t size) {
  slots_size = NextPowerOfTwo(size);
  slots_ptr->resize(slots_size);
}

bool Latches::Acquire(Lock* lock, uint64_t who) const {
  size_t acquired_count = 0;
  for (size_t i = lock->ownedCount; i < lock->requiredHashes.size(); ++i) {
    auto key_hash = lock->requiredHashes[i];
    BAIDU_SCOPED_LOCK(GetSlot(key_hash)->mutex);
    Latch& latch = GetSlot(key_hash)->latch;

    auto first_req = latch.GetFirstReqByHash(key_hash);
    if (first_req.has_value()) {
      if (first_req == who) {
        ++acquired_count;
      } else {
        latch.WaitForWake(key_hash, who);
        break;
      }
    } else {
      latch.WaitForWake(key_hash, who);
      ++acquired_count;
    }
  }

  lock->ownedCount += acquired_count;
  return lock->Acquired();
}

std::vector<uint64_t> Latches::Release(Lock* lock, uint64_t who,
                                       std::optional<std::pair<uint64_t, Lock*>> keep_latches_for_next_cmd) const {
  uint64_t keep_latches_for_cid = 0;
  std::vector<uint64_t>::iterator keep_latches_it;

  std::pair<uint64_t, Lock*>* keep_latchtes_for_next_cmd_pair = nullptr;
  if (keep_latches_for_next_cmd.has_value()) {
    keep_latchtes_for_next_cmd_pair = &keep_latches_for_next_cmd.value();
    keep_latches_for_cid = keep_latchtes_for_next_cmd_pair->first;
    keep_latches_it = keep_latchtes_for_next_cmd_pair->second->requiredHashes.begin();
  }

  std::vector<uint64_t> wakeup_list;
  for (auto key_hash_iter = lock->requiredHashes.begin();
       key_hash_iter != lock->requiredHashes.begin() + lock->ownedCount; ++key_hash_iter) {
    BAIDU_SCOPED_LOCK(GetSlot(*key_hash_iter)->mutex);
    auto* slot = this->GetSlot(*key_hash_iter);
    auto* latch = &slot->latch;
    auto value = latch->PopFront(*key_hash_iter);
    assert(value.has_value());
    auto v = value.value().first;
    auto front = value.value().second;
    assert(front == who);
    assert(v == *key_hash_iter);

    bool keep_for_next_cmd = false;
    if (keep_latchtes_for_next_cmd_pair != nullptr &&
        keep_latches_it != keep_latchtes_for_next_cmd_pair->second->requiredHashes.end()) {
      assert(*keep_latches_it >= *key_hash_iter);
      if (*keep_latches_it == *key_hash_iter) {
        ++keep_latches_it;
        keep_for_next_cmd = true;
      }
    }

    if (!keep_for_next_cmd) {
      auto wakeup = latch->GetFirstReqByHash(*key_hash_iter);
      if (wakeup.has_value()) {
        wakeup_list.push_back(wakeup.value());
      }
    } else {
      latch->PushPreemptive(*key_hash_iter, keep_latches_for_cid);
    }
  }

  assert(keep_latchtes_for_next_cmd_pair == nullptr ||
         keep_latches_it == keep_latchtes_for_next_cmd_pair->second->requiredHashes.end());

  return wakeup_list;
}

size_t Latches::NextPowerOfTwo(size_t n) {
  if (n == 0) {
    return 1;
  }
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  return n + 1;
}

Slot* Latches::GetSlot(uint64_t hash) const {
  auto index = hash & (slots_size - 1);
  return &(*slots_ptr)[index];
}

}  // namespace dingodb
