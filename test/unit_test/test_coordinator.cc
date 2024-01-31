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

#include "coordinator/coordinator_control.h"  // Include the header file where GenWatchBitSet is defined
#include "proto/meta.pb.h"                    // Include the protobuf file where MetaEventType is defined

class CoordinatorControlTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(CoordinatorControlTest, GenWatchBitSetFromRequest) {
  {
    dingodb::pb::meta::WatchCreateRequest request;

    // Add event types to the request
    for (int i = 0; i < 5; ++i) {
      request.add_event_types(static_cast<dingodb::pb::meta::MetaEventType>(i));
    }

    std::bitset<WATCH_BITSET_SIZE> bitset = dingodb::CoordinatorControl::GenWatchBitSet(request);

    // Check that the bitset has the correct bits set
    for (int i = 0; i < 5; ++i) {
      EXPECT_TRUE(bitset.test(i));
    }
  }

  {
    dingodb::pb::meta::WatchCreateRequest request;

    // Add event types to the request
    for (int i = 3; i < 5; ++i) {
      request.add_event_types(static_cast<dingodb::pb::meta::MetaEventType>(i));
    }

    std::bitset<WATCH_BITSET_SIZE> bitset = dingodb::CoordinatorControl::GenWatchBitSet(request);

    // Check that the bitset has the correct bits set
    for (int i = 3; i < 5; ++i) {
      EXPECT_TRUE(bitset.test(i));
    }
  }

  {
    dingodb::pb::meta::WatchCreateRequest request;

    // Add event types to the request
    for (int i = 4; i < 8; ++i) {
      request.add_event_types(static_cast<dingodb::pb::meta::MetaEventType>(i));
    }

    std::bitset<WATCH_BITSET_SIZE> bitset = dingodb::CoordinatorControl::GenWatchBitSet(request);

    // Check that the bitset has the correct bits set
    for (int i = 4; i < 8; ++i) {
      EXPECT_TRUE(bitset.test(i));
    }
  }

  {
    dingodb::pb::meta::WatchCreateRequest request;

    // Add event types to the request
    for (int i = 8; i < 12; ++i) {
      request.add_event_types(static_cast<dingodb::pb::meta::MetaEventType>(i));
    }

    std::bitset<WATCH_BITSET_SIZE> bitset = dingodb::CoordinatorControl::GenWatchBitSet(request);

    // Check that the bitset has the correct bits set
    for (int i = 8; i < 12; ++i) {
      EXPECT_TRUE(bitset.test(i));
    }
  }
}

TEST_F(CoordinatorControlTest, GenWatchBitSetFromSet) {
  {
    std::set<dingodb::pb::meta::MetaEventType> event_types;

    // Add event types to the set
    for (int i = 0; i < 5; ++i) {
      event_types.insert(static_cast<dingodb::pb::meta::MetaEventType>(i));
    }

    std::bitset<WATCH_BITSET_SIZE> bitset = dingodb::CoordinatorControl::GenWatchBitSet(event_types);

    // Check that the bitset has the correct bits set
    for (int i = 0; i < 5; ++i) {
      EXPECT_TRUE(bitset.test(i));
    }
  }

  {
    std::set<dingodb::pb::meta::MetaEventType> event_types;

    // Add event types to the set
    for (int i = 3; i < 5; ++i) {
      event_types.insert(static_cast<dingodb::pb::meta::MetaEventType>(i));
    }

    std::bitset<WATCH_BITSET_SIZE> bitset = dingodb::CoordinatorControl::GenWatchBitSet(event_types);

    // Check that the bitset has the correct bits set
    for (int i = 3; i < 5; ++i) {
      EXPECT_TRUE(bitset.test(i));
    }
  }

  {
    std::set<dingodb::pb::meta::MetaEventType> event_types;

    // Add event types to the set
    for (int i = 4; i < 8; ++i) {
      event_types.insert(static_cast<dingodb::pb::meta::MetaEventType>(i));
    }

    std::bitset<WATCH_BITSET_SIZE> bitset = dingodb::CoordinatorControl::GenWatchBitSet(event_types);

    // Check that the bitset has the correct bits set
    for (int i = 4; i < 8; ++i) {
      EXPECT_TRUE(bitset.test(i));
    }
  }

  {
    std::set<dingodb::pb::meta::MetaEventType> event_types;

    // Add event types to the set
    for (int i = 8; i < 12; ++i) {
      event_types.insert(static_cast<dingodb::pb::meta::MetaEventType>(i));
    }

    std::bitset<WATCH_BITSET_SIZE> bitset = dingodb::CoordinatorControl::GenWatchBitSet(event_types);

    // Check that the bitset has the correct bits set
    for (int i = 8; i < 12; ++i) {
      EXPECT_TRUE(bitset.test(i));
    }
  }
}