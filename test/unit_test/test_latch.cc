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

#include <gtest/gtest.h>
#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/string_printf.h"
#include "butil/synchronization/lock.h"
#include "common/helper.h"
#include "common/latch.h"
#include "common/logging.h"

class DingoLatchTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST(DingoLatchTest, next_power_of_two) {
  auto tmp = dingodb::Latches::NextPowerOfTwo(256);
  EXPECT_EQ(tmp, 256);

  tmp = dingodb::Latches::NextPowerOfTwo(257);
  EXPECT_EQ(tmp, 512);

  tmp = dingodb::Latches::NextPowerOfTwo(15);
  EXPECT_EQ(tmp, 16);

  tmp = dingodb::Latches::NextPowerOfTwo(512);
  EXPECT_EQ(tmp, 512);

  tmp = dingodb::Latches::NextPowerOfTwo(513);
  EXPECT_EQ(tmp, 1024);

  tmp = dingodb::Latches::NextPowerOfTwo(1024);
  EXPECT_EQ(tmp, 1024);

  tmp = dingodb::Latches::NextPowerOfTwo(1025);
  EXPECT_EQ(tmp, 2048);

  tmp = dingodb::Latches::NextPowerOfTwo(2048);
  EXPECT_EQ(tmp, 2048);

  tmp = dingodb::Latches::NextPowerOfTwo(2049);
  EXPECT_EQ(tmp, 4096);

  tmp = dingodb::Latches::NextPowerOfTwo(4096);
  EXPECT_EQ(tmp, 4096);
}

TEST(DingoLatchTest, wakeup) {
  dingodb::Latches latches(1024);

  std::vector<std::string> keys_a{"k1", "k3", "k5"};
  std::vector<std::string> keys_b{"k4", "k5", "k6"};
  int64_t cid_a = 1;
  int64_t cid_b = 2;

  dingodb::Lock lock_a(keys_a);
  dingodb::Lock lock_b(keys_b);

  // a acquire lock success
  auto acquired_a = latches.Acquire(&lock_a, cid_a);
  EXPECT_EQ(acquired_a, true);

  // b acquire lock failed
  auto acquired_b = latches.Acquire(&lock_b, cid_b);
  EXPECT_EQ(acquired_b, false);

  // a release lock, and get wakeup list
  auto wakeup = latches.Release(&lock_a, cid_a, std::nullopt);
  EXPECT_EQ(wakeup[0], cid_b);

  // b acquire lock success
  acquired_b = latches.Acquire(&lock_b, cid_b);
  EXPECT_EQ(acquired_b, true);
}

TEST(DingoLatchTest, wakeup_by_multi_cmds) {
  dingodb::Latches latches(256);

  std::vector<std::string> keys_a{"k1", "k2", "k3"};
  std::vector<std::string> keys_b{"k4", "k5", "k6"};
  std::vector<std::string> keys_c{"k3", "k4"};
  dingodb::Lock lock_a(keys_a);
  dingodb::Lock lock_b(keys_b);
  dingodb::Lock lock_c(keys_c);
  uint64_t cid_a = 1;
  uint64_t cid_b = 2;
  uint64_t cid_c = 3;

  // a acquire lock success
  auto acquired_a = latches.Acquire(&lock_a, cid_a);
  EXPECT_EQ(acquired_a, true);

  // b acquire lock success
  auto acquired_b = latches.Acquire(&lock_b, cid_b);
  EXPECT_EQ(acquired_b, true);

  // c acquire lock failed, cause a occupied slot 3
  auto acquired_c = latches.Acquire(&lock_c, cid_c);
  EXPECT_EQ(acquired_c, false);

  // a release lock, and get wakeup list
  auto wakeup = latches.Release(&lock_a, cid_a, std::nullopt);
  EXPECT_EQ(wakeup[0], cid_c);

  // c acquire lock failed again, cause b occupied slot 4
  acquired_c = latches.Acquire(&lock_c, cid_c);
  EXPECT_EQ(acquired_c, false);

  // b release lock, and get wakeup list
  wakeup = latches.Release(&lock_b, cid_b, std::nullopt);
  EXPECT_EQ(wakeup[0], cid_c);

  // finally c acquire lock success
  acquired_c = latches.Acquire(&lock_c, cid_c);
  EXPECT_EQ(acquired_c, true);
}

TEST(DingoLatchTest, wakeup_by_small_latch_slot) {
  dingodb::Latches latches(5);

  std::vector<std::string> keys_a{"k1", "k2", "k3"};
  std::vector<std::string> keys_b{"k6", "k7", "k8"};
  std::vector<std::string> keys_c{"k3", "k4"};
  std::vector<std::string> keys_d{"k7", "k10"};
  dingodb::Lock lock_a(keys_a);
  dingodb::Lock lock_b(keys_b);
  dingodb::Lock lock_c(keys_c);
  dingodb::Lock lock_d(keys_d);
  u_int64_t cid_a = 1;
  u_int64_t cid_b = 2;
  u_int64_t cid_c = 3;
  u_int64_t cid_d = 4;

  auto acquired_a = latches.Acquire(&lock_a, cid_a);
  EXPECT_EQ(acquired_a, true);

  // c acquire lock failed, cause a occupied slot 3
  auto acquired_c = latches.Acquire(&lock_c, cid_c);
  EXPECT_EQ(acquired_c, false);

  // b acquire lock success
  auto acquired_b = latches.Acquire(&lock_b, cid_b);
  EXPECT_EQ(acquired_b, true);

  // d acquire lock failed, cause a occupied slot 7
  auto acquired_d = latches.Acquire(&lock_d, cid_d);
  EXPECT_EQ(acquired_d, false);

  // a release lock, and get wakeup list
  auto wakeup = latches.Release(&lock_a, cid_a, std::nullopt);
  EXPECT_EQ(wakeup[0], cid_c);

  // c acquire lock success
  acquired_c = latches.Acquire(&lock_c, cid_c);
  EXPECT_EQ(acquired_c, true);

  // b release lock, and get wakeup list
  wakeup = latches.Release(&lock_b, cid_b, std::nullopt);
  EXPECT_EQ(wakeup[0], cid_d);

  // finally d acquire lock success
  acquired_d = latches.Acquire(&lock_d, cid_d);
  EXPECT_EQ(acquired_d, true);
}

void CheckLatchHolder(dingodb::Latches* latches, const std::string& key, std::optional<uint64_t> expected_holder_cid) {
  uint64_t hash = dingodb::Lock::Hash(key);
  auto* slot = latches->GetSlot(hash);
  BAIDU_SCOPED_LOCK(slot->mutex);
  std::optional<uint64_t> actual_holder = slot->latch.GetFirstReqByHash(hash);
  assert(actual_holder == expected_holder_cid);
}

bool IsLatchesEmpty(dingodb::Latches* latches) {
  for (uint64_t i = 0; i < latches->slots_size; ++i) {
    auto* slot = latches->GetSlot(i);
    BAIDU_SCOPED_LOCK(slot->mutex);
    const auto& waiting = slot->latch.waiting;
    if (!waiting.empty()) {
      return false;
    }
    // if (std::any_of(waiting.begin(), waiting.end(), [](const auto& x) { return x.has_value(); })) {
    //   return false;
    // }
  }
  return true;
}

void TestPartiallyReleasingImpl(uint64_t size) {
  dingodb::Latches latches(size);

  // Single key.
  {
    std::vector<std::string> key{"k1"};
    dingodb::Lock lock(key);
    EXPECT_EQ(latches.Acquire(&lock, 1), true);
    EXPECT_EQ(!IsLatchesEmpty(&latches), true);
    dingodb::Lock lock2(key);
    auto wakeup = latches.Release(&lock, 1, std::make_pair(2, &lock2));
    EXPECT_EQ(wakeup.empty(), true);
    CheckLatchHolder(&latches, key.at(0), 2);
    lock2.ForceAssumeAcquired();
    wakeup = latches.Release(&lock2, 2, std::nullopt);
    EXPECT_EQ(wakeup.empty(), true);
    EXPECT_EQ(IsLatchesEmpty(&latches), true);
  }

  // Single key with queueing commands.
  {
    std::vector<std::string> key{"k1"};
    dingodb::Lock lock(key);
    dingodb::Lock queueing_lock(key);
    EXPECT_EQ(latches.Acquire(&lock, 3), true);
    EXPECT_EQ(!latches.Acquire(&queueing_lock, 4), true);
    dingodb::Lock lock2(key);
    auto wakeup = latches.Release(&lock, 3, std::make_pair(5, &lock2));
    EXPECT_EQ(wakeup.empty(), true);
    CheckLatchHolder(&latches, key.at(0), 5);
    lock2.ForceAssumeAcquired();
    wakeup = latches.Release(&lock2, 5, std::nullopt);
    // EXPECT_EQ(wakeup, vec ![4u64], true);
    EXPECT_EQ(latches.Acquire(&queueing_lock, 4), true);
    wakeup = latches.Release(&queueing_lock, 4, std::nullopt);
    EXPECT_EQ(wakeup.empty(), true);
    EXPECT_EQ(IsLatchesEmpty(&latches), true);
  }

  // Multi keys, keep all.
  {
    std::vector<std::string> keys{"k0", "k2", "k3", "k4"};
    dingodb::Lock lock(keys);
    EXPECT_EQ(latches.Acquire(&lock, 11), true);
    dingodb::Lock lock2(keys);
    auto wakeup = latches.Release(&lock, 11, std::make_pair(12, &lock2));
    EXPECT_EQ(wakeup.empty(), true);

    for (auto& key : keys) {
      CheckLatchHolder(&latches, key, 12);
    }
    assert(!IsLatchesEmpty(&latches));
    lock2.ForceAssumeAcquired();
    wakeup = latches.Release(&lock2, 12, std::nullopt);
    assert(wakeup.empty());
    assert(IsLatchesEmpty(&latches));
  }

  // Multi keys, keep all, with queueing command.
  {
    std::vector<std::string> keys{"k0", "k2", "k3", "k4"};
    dingodb::Lock lock(keys);
    assert(latches.Acquire(&lock, 11));
    std::vector<dingodb::Lock> queueing_locks;
    queueing_locks.reserve(keys.size());
    for (auto& k : keys) {
      queueing_locks.push_back(dingodb::Lock({k}));
    }
    for (int cid = 12; cid < 16; ++cid) {
      assert(!latches.Acquire(&queueing_locks[cid - 12], cid));
    }
    dingodb::Lock lock2(keys);
    auto wakeup = latches.Release(&lock, 11, std::make_pair(17, &lock2));
    assert(wakeup.empty());
    for (auto& key : keys) {
      CheckLatchHolder(&latches, key, 17);
    }
    assert(!IsLatchesEmpty(&latches));
    lock2.ForceAssumeAcquired();
    wakeup = latches.Release(&lock2, 17, std::nullopt);
    std::sort(wakeup.begin(), wakeup.end());
    // Wake up queueing commands.
    std::vector<u_int64_t> wakeup_equal{12, 13, 14, 15};
    assert(wakeup == wakeup_equal);
    for (int cid = 12; cid < 16; ++cid) {
      assert(latches.Acquire(&queueing_locks[cid - 12], cid));
      auto wakeup = latches.Release(&queueing_locks[cid - 12], cid, std::nullopt);
      assert(wakeup.empty());
    }
    assert(IsLatchesEmpty(&latches));

    // 4 keys, keep 2 of them.
    for (int i1 = 0; i1 < 3; ++i1) {
      for (int i2 = i1 + 1; i2 < 4; ++i2) {
        dingodb::Lock lock(keys);
        assert(latches.Acquire(&lock, 21));
        dingodb::Lock lock2({keys[i1], keys[i2]});
        auto wakeup = latches.Release(&lock, 21, std::make_pair(22, &lock2));
        assert(wakeup.empty());
        CheckLatchHolder(&latches, keys[i1], 22);
        CheckLatchHolder(&latches, keys[i2], 22);
        lock2.ForceAssumeAcquired();
        wakeup = latches.Release(&lock2, 22, std::nullopt);
        assert(wakeup.empty());
        assert(IsLatchesEmpty(&latches));
      }
    }

    // 4 keys keep 2 of them, with queueing commands.
    for (uint64_t k1 = 0; k1 < 3; ++k1) {
      for (uint64_t k2 = k1 + 1; k2 < 4; ++k2) {
        dingodb::Lock lock(keys);
        assert(latches.Acquire(&lock, 21));

        std::vector<dingodb::Lock> queueing_locks;
        queueing_locks.reserve(keys.size());
        for (auto& k : keys) {
          queueing_locks.push_back(dingodb::Lock({k}));
        }
        for (int cid = 22; cid < 26; ++cid) {
          assert(!latches.Acquire(&queueing_locks[cid - 22], cid));
        }

        dingodb::Lock lock2({keys[k1], keys[k2]});
        auto wakeup = latches.Release(&lock, 21, std::make_pair(27, &lock2));
        assert(wakeup.size() == 2);

        // The latch of k1 and k2 is preempted, and queueing locks on the other two keys
        // will be woken up.
        auto tmp_k1 = k1 + 22;
        auto tmp_k2 = k2 + 22;
        if (tmp_k1 > 25) {
          tmp_k1 -= 4;
        }
        if (tmp_k2 > 25) {
          tmp_k2 -= 4;
        }
        // DINGO_LOG(INFO) << "k1: " << k1 << ", k2: " << k2;
        // DINGO_LOG(INFO) << "tmp_k1: " << tmp_k1 << ", tmp_k2: " << tmp_k2;

        std::vector<uint64_t> preempted_cids = {tmp_k1, tmp_k2};
        std::vector<uint64_t> expected_wakeup_cids;
        for (uint64_t i = 22; i < 26; ++i) {
          if (std::find(preempted_cids.begin(), preempted_cids.end(), i) == preempted_cids.end()) {
            expected_wakeup_cids.push_back(i);
          }
        }
        std::sort(wakeup.begin(), wakeup.end());
        // print all elements of wakeup and expected_wakeup_cids
        // DINGO_LOG(INFO) << "wakeup: ";
        // for (auto& i : wakeup) {
        //   DINGO_LOG(INFO) << i << " ";
        // }
        // DINGO_LOG(INFO) << "expected_wakeup_cids: ";
        // for (auto& i : expected_wakeup_cids) {
        //   DINGO_LOG(INFO) << i << " ";
        // }
        assert(wakeup == expected_wakeup_cids);

        CheckLatchHolder(&latches, keys[k1], 27);
        CheckLatchHolder(&latches, keys[k2], 27);

        lock2.ForceAssumeAcquired();
        wakeup = latches.Release(&lock2, 27, std::nullopt);
        std::sort(wakeup.begin(), wakeup.end());
        assert(wakeup == preempted_cids);

        for (int cid = 22; cid < 26; ++cid) {
          assert(latches.Acquire(&queueing_locks[cid - 22], cid));
          auto wakeup = latches.Release(&queueing_locks[cid - 22], cid, std::nullopt);
          assert(wakeup.empty());
        }

        assert(IsLatchesEmpty(&latches));
      }
    }
  }
}

TEST(DingoLatchTest, partially_releasing) {
  TestPartiallyReleasingImpl(256);
  TestPartiallyReleasingImpl(128);
  TestPartiallyReleasingImpl(64);
  TestPartiallyReleasingImpl(4);
  TestPartiallyReleasingImpl(2);
}