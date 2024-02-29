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

#include <string>
#include <thread>
#include <vector>

#include "common/helper.h"
#include "common/uuid.h"
#include "fmt/core.h"

class TrackerTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TrackerTest, SetTime) {
  dingodb::pb::common::RequestInfo request_info;
  request_info.set_request_id(1000000);
  auto tracker = dingodb::Tracker::New(request_info);
  ASSERT_NE(nullptr, tracker);

  int ms = 1;
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));

  tracker->SetServiceQueueWaitTime();
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));

  tracker->SetPrepairCommitTime();
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));

  tracker->SetRaftCommitTime();
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));

  tracker->SetRaftQueueWaitTime();
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));

  tracker->SetRaftApplyTime();
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));

  tracker->SetTotalRpcTime();

  ASSERT_LE(1 * ms * 1000 * 1000, tracker->ServiceQueueWaitTime());
  ASSERT_LE(2 * ms * 1000 * 1000, tracker->PrepairCommitTime());
  ASSERT_LE(3 * ms * 1000 * 1000, tracker->RaftCommitTime());
  ASSERT_LE(4 * ms * 1000 * 1000, tracker->RaftQueueWaitTime());
  ASSERT_LE(5 * ms * 1000 * 1000, tracker->RaftApplyTime());
  ASSERT_LE(6 * ms * 1000 * 1000, tracker->TotalRpcTime());
}