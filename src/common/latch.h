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

#ifndef DINGODB_COMMON_LATCH_H_
#define DINGODB_COMMON_LATCH_H_

#include <deque>
#include <optional>
#include <vector>

#include "bthread/mutex.h"
#include "bthread/types.h"

namespace dingodb {

class Latch {
 public:
  std::deque<std::pair<uint64_t, uint64_t>> waiting{};

  Latch() = default;

  std::optional<uint64_t> GetFirstReqByHash(uint64_t hash);

  std::optional<std::pair<uint64_t, uint64_t>> PopFront(uint64_t key_hash);

  void WaitForWake(uint64_t key_hash, uint64_t cid);

  void PushPreemptive(uint64_t key_hash, uint64_t cid);

 private:
  void MaybeShrink();
};

class Lock {
 public:
  std::vector<uint64_t> requiredHashes{};
  size_t ownedCount = 0;

  Lock(const std::vector<std::string>& keys);

  bool Acquired() const;

  void ForceAssumeAcquired();

  bool IsWriteLock() const;

  static uint64_t Hash(const std::string& key);
};

struct Slot {
  Slot() { CHECK_EQ(0, bthread_mutex_init(&mutex, nullptr)); }
  ~Slot() { CHECK_EQ(0, bthread_mutex_destroy(&mutex)); }
  bthread_mutex_t mutex;
  Latch latch;
};

class Latches {
 public:
  explicit Latches(size_t size);
  explicit Latches();
  ~Latches();

  void SetSlotNum(size_t size);

  bool Acquire(Lock* lock, uint64_t who) const;

  // std::vector<uint64_t> Release(const Lock& lock, uint64_t who) const;
  std::vector<uint64_t> Release(Lock* lock, uint64_t who,
                                std::optional<std::pair<uint64_t, Lock*>> keep_latches_for_next_cmd) const;

  size_t GetSlotIndex(uint64_t hash) const;
  Slot* GetSlot(uint64_t hash) const;

  std::vector<Slot>* slots_ptr;
  size_t slots_size;

  static size_t NextPowerOfTwo(size_t n);
};

}  // namespace dingodb

#endif