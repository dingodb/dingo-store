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

#include <atomic>
#include <cstdint>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include "common/helper.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "fmt/core.h"
#include "mvcc/ts_provider.h"

namespace dingodb {

const uint32_t kBatchTsSzie = 100;

static int64_t ComposeTs(int64_t physical, int64_t logical) { return (physical << 18) + logical; }

mvcc::BatchTs* GenBatchTs() {
  static std::atomic<int64_t> id = 1;
  static int64_t physical = Helper::TimestampMs();
  static int64_t logical = 0;

  int64_t now_ms = Helper::TimestampMs();
  mvcc::BatchTs* batch_ts = nullptr;
  if (now_ms > physical) {
    logical = 0;
    physical = now_ms;
  }

  batch_ts = mvcc::BatchTs::New(physical, logical, kBatchTsSzie);
  logical += kBatchTsSzie;

  return batch_ts;
}

class BatchTsListTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(BatchTsListTest, SingleThread) {
  mvcc::BatchTsList batch_ts_list;

  int push_size = 100;
  for (int i = 0; i < push_size; ++i) {
    batch_ts_list.Push(GenBatchTs());
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_EQ(push_size, batch_ts_list.ActualCount());

  int64_t count = 0;
  for (;;) {
    int64_t ts = batch_ts_list.GetTs();
    if (ts == 0) {
      break;
    }
    ++count;
  }

  ASSERT_EQ(push_size * kBatchTsSzie, count);
  ASSERT_EQ(0, batch_ts_list.ActualCount());
}

TEST_F(BatchTsListTest, MultiThread) {
  mvcc::BatchTsList batch_ts_list;

  int push_size = 1000;
  for (int i = 0; i < push_size; ++i) {
    batch_ts_list.Push(GenBatchTs());
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_EQ(push_size, batch_ts_list.ActualCount());

  std::atomic<int64_t> total_count = 0;
  int thread_num = 10;
  std::vector<std::thread> threads;
  threads.reserve(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    threads.push_back(std::thread([i, &batch_ts_list, &total_count] {
      int thread_no = i;
      int64_t count = 0;
      for (;;) {
        int64_t ts = batch_ts_list.GetTs();
        if (ts == 0) {
          break;
        }
        ++count;
      }

      // std::cout << fmt::format("thread({}) count({})", thread_no, count) << std::endl;
      total_count.fetch_add(count);
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  std::cout << "finish......" << std::endl;

  ASSERT_EQ(push_size * kBatchTsSzie, total_count.load());
  ASSERT_EQ(0, batch_ts_list.ActualCount());
  ASSERT_EQ(0, batch_ts_list.ActiveCount());
}

TEST_F(BatchTsListTest, MultiThreadLongTimeRun) {
  mvcc::BatchTsList batch_ts_list;

  int push_size = 10;
  for (int i = 0; i < push_size; ++i) {
    batch_ts_list.Push(GenBatchTs());
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_EQ(push_size + 1, batch_ts_list.ActualCount());

  int producer_num = 8;
  int consumer_num = 10;
  std::vector<std::thread> threads;
  threads.reserve(producer_num + consumer_num);

  // producer
  for (int i = 0; i < producer_num; ++i) {
    threads.push_back(std::thread([i, &batch_ts_list] {
      int thread_no = i;
      for (uint32_t j = 0; j < 100000; ++j) {
        batch_ts_list.Push(GenBatchTs());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }

      std::cout << fmt::format("producer thread({}) finish...", thread_no) << std::endl;
    }));
  }

  // consumer
  std::atomic<int64_t> ts_count = 0;
  for (int i = 0; i < consumer_num; ++i) {
    threads.push_back(std::thread([i, &batch_ts_list, &ts_count] {
      int thread_no = i;
      for (;;) {
        int64_t ts = batch_ts_list.GetTs();
        if (ts == 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
          ts_count.fetch_add(1, std::memory_order_relaxed);
        }
      }

      std::cout << fmt::format("consumer thread({}) finish...", thread_no) << std::endl;
    }));
  }

  // monitor status
  for (;;) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::cout << batch_ts_list.DebugInfo() << fmt::format(" total_count({})", ts_count.load()) << std::endl;
    batch_ts_list.CleanDead();
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

class TsProviderTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TsProviderTest, GetTs) {
  auto ts_provider = mvcc::TsProvider::New(nullptr);

  ASSERT_TRUE(ts_provider->Init());

  int consumer_num = 4;
  std::vector<std::thread> threads;
  threads.reserve(consumer_num);

  // consumer
  std::atomic<int64_t> ts_count = 0;
  for (int i = 0; i < consumer_num; ++i) {
    threads.push_back(std::thread([i, &ts_provider, &ts_count] {
      int thread_no = i;
      for (;;) {
        int64_t ts = ts_provider->GetTs();
        if (ts == 0) {
          std::cout << "ts is 0" << std::endl;
          std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        } else {
          ts_count.fetch_add(1, std::memory_order_relaxed);
        }
      }

      std::cout << fmt::format("consumer thread({}) finish...", thread_no) << std::endl;
    }));
  }

  // monitor
  for (;;) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    std::cout << ts_provider->DebugInfo() << " ts_count: " << ts_count.load() << std::endl;
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

}  // namespace dingodb