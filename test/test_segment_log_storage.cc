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

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <random>
#include <string>

#include "braft/log_entry.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "log/segment_log_storage.h"
#include "proto/raft.pb.h"

class SegmentLogStorageTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    log_stroage = std::make_shared<dingodb::SegmentLogStorage>("/tmp/log", 100, 8 * 1024 * 1024);
    static braft::ConfigurationManager configuration_manager;
    log_stroage->Init(&configuration_manager);
  }
  static void TearDownTestSuite() {
    log_stroage->Reset(log_stroage->LastLogIndex() + 1);
    log_stroage->GcInstance("/tmp/log");

    std::filesystem::remove_all("/tmp/log");
  }

  void SetUp() override {}
  void TearDown() override {}

 public:
  static std::shared_ptr<dingodb::SegmentLogStorage> log_stroage;
};

std::shared_ptr<dingodb::SegmentLogStorage> SegmentLogStorageTest::log_stroage = nullptr;

float GenRandomFloat() {
  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_real_distribution<> distrib(1, 100);

  return distrib(rng);
}

braft::LogEntry* GenLogEntry() {
  static uint64_t auto_vector_id = 0;
  static uint64_t auto_log_index = 0;

  auto* log_entry = new braft::LogEntry();
  log_entry->AddRef();

  log_entry->type = braft::ENTRY_TYPE_DATA;
  log_entry->id.term = 1;
  log_entry->id.index = ++auto_log_index;

  dingodb::pb::raft::RaftCmdRequest raft_cmd;
  auto* request = raft_cmd.add_requests();
  request->set_cmd_type(dingodb::pb::raft::VECTOR_ADD);
  auto* vector_add = request->mutable_vector_add();

  for (int i = 0; i < 10; ++i) {
    dingodb::pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(++auto_vector_id);
    auto* vector = vector_with_id.mutable_vector();
    vector->set_dimension(256);
    vector->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < 256; ++j) {
      vector->add_float_values(GenRandomFloat());
    }

    vector_add->add_vectors()->Swap(&vector_with_id);
  }

  butil::IOBufAsZeroCopyOutputStream wrapper(&log_entry->data);
  raft_cmd.SerializeToZeroCopyStream(&wrapper);

  return log_entry;
}

TEST_F(SegmentLogStorageTest, AppendEntries) {
  uint64_t begin_log_index = log_stroage->FirstLogIndex();

  const int k_log_entry_count = 1000;
  for (int i = 0; i < k_log_entry_count; ++i) {
    auto* log_entry = GenLogEntry();
    log_stroage->AppendEntry(log_entry);
    log_entry->Release();
  }

  DINGO_LOG(INFO) << fmt::format("first_log_id: {} last_log_id: {}", log_stroage->FirstLogIndex(),
                                 log_stroage->LastLogIndex());

  EXPECT_EQ(begin_log_index + k_log_entry_count, log_stroage->LastLogIndex() + 1);
}

TEST_F(SegmentLogStorageTest, GetEntrys) {
  const int k_log_entry_count = 1000;
  for (int i = 0; i < k_log_entry_count; ++i) {
    auto* log_entry = GenLogEntry();
    log_stroage->AppendEntry(log_entry);
    log_entry->Release();
  }

  uint64_t begin_index = 600;
  uint64_t end_index = 1000;
  auto log_entrys = log_stroage->GetEntrys(begin_index, end_index);

  DINGO_LOG(INFO) << fmt::format("log entrys count {}", log_entrys.size());

  EXPECT_EQ(end_index - begin_index + 1, log_entrys.size());
}