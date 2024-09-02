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
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "braft/enum.pb.h"
#include "braft/log_entry.h"
#include "bthread/bthread.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "log/rocks_log_storage.h"

const std::string kRootPath = "./unit_test";
const std::string kLogPath = kRootPath + "/rocks_log_storage";

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

// rand string
static std::string GenRandomString(int len) {
  std::string result;
  int alphabet_len = sizeof(kAlphabet);

  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
  for (int i = 0; i < len; ++i) {
    result.append(1, kAlphabet[distrib(rng) % alphabet_len]);
  }

  return result;
}

class RocksLogStorageTest : public testing::Test {
 protected:
  static void SetUpTestSuite() { dingodb::Helper::CreateDirectories(kLogPath); }

  static void TearDownTestSuite() {
    // dingodb::Helper::RemoveAllFileOrDirectory(kLogPath);
  }

  void SetUp() override {}
  void TearDown() override {}
};

braft::LogEntry* GenBraftLogEntry(int64_t region_id, int64_t index, dingodb::wal::LogEntryType type, int term) {
  braft::LogEntry* braft_entry = new braft::LogEntry();
  braft_entry->AddRef();

  braft_entry->id.term = term;
  braft_entry->id.index = index;

  if (type == dingodb::wal::LogEntryType::kEntryTypeConfiguration) {
    braft_entry->type = braft::EntryType::ENTRY_TYPE_CONFIGURATION;

    std::vector<braft::PeerId>* peers = new std::vector<braft::PeerId>();
    peers->push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20001"));
    peers->push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20002"));
    peers->push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20003"));

    std::vector<braft::PeerId>* old_peers = new std::vector<braft::PeerId>();
    old_peers->push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20001"));
    old_peers->push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20002"));
    old_peers->push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20003"));

    braft_entry->peers = peers;
    braft_entry->old_peers = old_peers;

  } else if (type == dingodb::wal::LogEntryType::kEntryTypeData) {
    braft_entry->type = braft::EntryType::ENTRY_TYPE_DATA;

    dingodb::pb::raft::RaftCmdRequest raft_request;

    raft_request.mutable_header()->set_region_id(region_id);
    raft_request.mutable_header()->mutable_epoch()->set_conf_version(3);
    raft_request.mutable_header()->mutable_epoch()->set_version(4);

    auto* request = raft_request.add_requests();
    request->set_cmd_type(dingodb::pb::raft::CmdType::PUT);
    request->mutable_put()->set_cf_name("data");

    auto* kv = request->mutable_put()->add_kvs();
    kv->set_key("hello_" + std::to_string(index));
    kv->set_value(GenRandomString(256));

    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    raft_request.SerializeToZeroCopyStream(&wrapper);

    braft_entry->data.swap(data);
  }

  return braft_entry;
}

dingodb::wal::LogEntry GenLogEntry(int64_t region_id, int64_t index, dingodb::wal::LogEntryType type, int term) {
  dingodb::wal::LogEntry entry;
  entry.type = type;
  entry.term = term;
  entry.index = index;
  entry.region_id = region_id;

  if (type == dingodb::wal::LogEntryType::kEntryTypeConfiguration) {
    braft::LogEntry* braft_entry = new braft::LogEntry();
    std::vector<braft::PeerId> peers;
    peers.push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20001"));
    peers.push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20002"));
    peers.push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20003"));

    std::vector<braft::PeerId> old_peers;
    old_peers.push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20001"));
    old_peers.push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20002"));
    old_peers.push_back(dingodb::Helper::StringToPeerId("127.0.0.1:20003"));

    braft_entry->peers = &peers;
    braft_entry->old_peers = &old_peers;

    butil::IOBuf* data = new butil::IOBuf();
    auto status = braft::serialize_configuration_meta(braft_entry, *data);
    if (!status.ok()) {
      DINGO_LOG(INFO) << "serialize configuration meta fail.";
    }

    braft_entry->Release();
    entry.in_data = data;
  } else if (type == dingodb::wal::LogEntryType::kEntryTypeData) {
    dingodb::pb::raft::RaftCmdRequest raft_request;

    raft_request.mutable_header()->set_region_id(region_id);
    raft_request.mutable_header()->mutable_epoch()->set_conf_version(3);
    raft_request.mutable_header()->mutable_epoch()->set_version(4);

    auto* request = raft_request.add_requests();
    request->set_cmd_type(dingodb::pb::raft::CmdType::PUT);
    request->mutable_put()->set_cf_name("data");

    auto* kv = request->mutable_put()->add_kvs();
    kv->set_key("hello_" + std::to_string(index));
    kv->set_value(GenRandomString(256));

    butil::IOBuf* data = new butil::IOBuf();
    butil::IOBufAsZeroCopyOutputStream wrapper(data);
    raft_request.SerializeToZeroCopyStream(&wrapper);
    entry.in_data = data;
  }

  return entry;
}

// [start_index, end_index]
struct IndexRangeOption {
  int64_t region_id;
  int64_t start_index;
  int64_t end_index;
  int64_t term;
  dingodb::wal::LogEntryType type;
};

std::vector<braft::LogEntry*> GenBraftLogEntries(const std::vector<IndexRangeOption>& options) {
  std::vector<braft::LogEntry*> entries;
  for (const auto& option : options) {
    for (int i = option.start_index; i <= option.end_index; ++i) {
      entries.push_back(GenBraftLogEntry(option.region_id, i, option.type, option.term));
    }
  }

  return entries;
}

std::vector<dingodb::wal::LogEntry> GenLogEntries(const std::vector<IndexRangeOption>& options) {
  std::vector<dingodb::wal::LogEntry> entries;
  for (const auto& option : options) {
    for (int i = option.start_index; i <= option.end_index; ++i) {
      entries.push_back(GenLogEntry(option.region_id, i, option.type, option.term));
    }
  }

  return entries;
}

TEST_F(RocksLogStorageTest, Append) {
  std::string path = fmt::format("{}/{}", kLogPath, "simple");
  auto log_storage = dingodb::wal::RocksLogStorage::New(path);
  ASSERT_TRUE(log_storage != nullptr);

  log_storage->RegisterClientType(dingodb::wal::ClientType::kRaft);

  ASSERT_TRUE(log_storage->Init());

  ASSERT_TRUE(log_storage->RegisterRegion(10000));
  ASSERT_TRUE(log_storage->RegisterRegion(10001));
  ASSERT_TRUE(log_storage->RegisterRegion(10002));

  // arrange data
  {
    dingodb::wal::Mutation mutation;
    mutation.region_id = 10000;
    mutation.type = dingodb::wal::Mutation::Type::kAppendLogEntry;

    std::vector<IndexRangeOption> options = {
        IndexRangeOption{mutation.region_id, 1, 10, 1, dingodb::wal::LogEntryType::kEntryTypeData},
        IndexRangeOption{mutation.region_id, 11, 12, 2, dingodb::wal::LogEntryType::kEntryTypeConfiguration},
        IndexRangeOption{mutation.region_id, 13, 50, 3, dingodb::wal::LogEntryType::kEntryTypeData},
    };

    std::vector<dingodb::wal::LogEntry> log_entries = GenLogEntries(options);
    mutation.log_entries.swap(log_entries);

    ASSERT_TRUE(log_storage->CommitMutation(&mutation));
  }

  {
    dingodb::wal::Mutation mutation;
    mutation.region_id = 10001;
    mutation.type = dingodb::wal::Mutation::Type::kAppendLogEntry;

    std::vector<IndexRangeOption> options = {
        IndexRangeOption{mutation.region_id, 1, 14, 1, dingodb::wal::LogEntryType::kEntryTypeData},
        IndexRangeOption{mutation.region_id, 15, 16, 2, dingodb::wal::LogEntryType::kEntryTypeConfiguration},
        IndexRangeOption{mutation.region_id, 17, 80, 3, dingodb::wal::LogEntryType::kEntryTypeData},
    };

    std::vector<dingodb::wal::LogEntry> log_entries = GenLogEntries(options);
    mutation.log_entries.swap(log_entries);

    ASSERT_TRUE(log_storage->CommitMutation(&mutation));
  }

  {
    dingodb::wal::Mutation mutation;
    mutation.region_id = 10002;
    mutation.type = dingodb::wal::Mutation::Type::kAppendLogEntry;

    std::vector<IndexRangeOption> options = {
        IndexRangeOption{mutation.region_id, 1, 20, 1, dingodb::wal::LogEntryType::kEntryTypeData},
        IndexRangeOption{mutation.region_id, 21, 22, 2, dingodb::wal::LogEntryType::kEntryTypeConfiguration},
        IndexRangeOption{mutation.region_id, 23, 90, 3, dingodb::wal::LogEntryType::kEntryTypeData},
    };

    std::vector<dingodb::wal::LogEntry> log_entries = GenLogEntries(options);
    mutation.log_entries.swap(log_entries);

    ASSERT_TRUE(log_storage->CommitMutation(&mutation));
  }

  log_storage->PrintLogIndexMeta();

  {
    EXPECT_EQ(1, log_storage->FirstLogIndex(10000));
    EXPECT_EQ(50, log_storage->LastLogIndex(10000));

    EXPECT_EQ(1, log_storage->FirstLogIndex(10001));
    EXPECT_EQ(80, log_storage->LastLogIndex(10001));

    EXPECT_EQ(1, log_storage->FirstLogIndex(10002));
    EXPECT_EQ(90, log_storage->LastLogIndex(10002));
  }

  {
    ASSERT_EQ(1, log_storage->GetTerm(10000, 1));
    EXPECT_EQ(2, log_storage->GetTerm(10000, 11));
    EXPECT_EQ(3, log_storage->GetTerm(10000, 20));

    EXPECT_EQ(1, log_storage->GetTerm(10001, 1));
    EXPECT_EQ(2, log_storage->GetTerm(10001, 15));
    EXPECT_EQ(3, log_storage->GetTerm(10001, 50));

    EXPECT_EQ(1, log_storage->GetTerm(10002, 1));
    EXPECT_EQ(2, log_storage->GetTerm(10002, 21));
    EXPECT_EQ(3, log_storage->GetTerm(10002, 60));
  }

  {
    dingodb::wal::LogEntryPtr log_entry;
    int64_t region_id = 10000;

    log_entry = log_storage->GetEntry(region_id, 1);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeData, log_entry->type);
    EXPECT_EQ(1, log_entry->index);
    EXPECT_EQ(1, log_entry->term);

    log_entry = log_storage->GetEntry(region_id, 7);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeData, log_entry->type);
    EXPECT_EQ(7, log_entry->index);
    EXPECT_EQ(1, log_entry->term);

    log_entry = log_storage->GetEntry(region_id, 11);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeConfiguration, log_entry->type);
    EXPECT_EQ(11, log_entry->index);
    EXPECT_EQ(2, log_entry->term);

    log_entry = log_storage->GetEntry(region_id, 30);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeData, log_entry->type);
    EXPECT_EQ(30, log_entry->index);
    EXPECT_EQ(3, log_entry->term);
  }

  {
    dingodb::wal::LogEntryPtr log_entry;
    int64_t region_id = 10001;

    log_entry = log_storage->GetEntry(region_id, 1);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeData, log_entry->type);
    EXPECT_EQ(1, log_entry->index);
    EXPECT_EQ(1, log_entry->term);

    log_entry = log_storage->GetEntry(region_id, 7);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeData, log_entry->type);
    EXPECT_EQ(7, log_entry->index);
    EXPECT_EQ(1, log_entry->term);

    log_entry = log_storage->GetEntry(region_id, 15);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeConfiguration, log_entry->type);
    EXPECT_EQ(15, log_entry->index);
    EXPECT_EQ(2, log_entry->term);

    log_entry = log_storage->GetEntry(region_id, 50);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeData, log_entry->type);
    EXPECT_EQ(50, log_entry->index);
    EXPECT_EQ(3, log_entry->term);
  }

  {
    dingodb::wal::LogEntryPtr log_entry;
    int64_t region_id = 10002;

    log_entry = log_storage->GetEntry(region_id, 1);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeData, log_entry->type);
    EXPECT_EQ(1, log_entry->index);
    EXPECT_EQ(1, log_entry->term);

    log_entry = log_storage->GetEntry(region_id, 7);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeData, log_entry->type);
    EXPECT_EQ(7, log_entry->index);
    EXPECT_EQ(1, log_entry->term);

    log_entry = log_storage->GetEntry(region_id, 21);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeConfiguration, log_entry->type);
    EXPECT_EQ(21, log_entry->index);
    EXPECT_EQ(2, log_entry->term);

    log_entry = log_storage->GetEntry(region_id, 80);
    ASSERT_TRUE(log_entry != nullptr);
    EXPECT_EQ(region_id, log_entry->region_id);
    EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeData, log_entry->type);
    EXPECT_EQ(80, log_entry->index);
    EXPECT_EQ(3, log_entry->term);
  }

  {
    auto log_entries = log_storage->GetConfigurations(10000);
    EXPECT_EQ(2, log_entries.size());
    for (auto& log_entry : log_entries) {
      EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeConfiguration, log_entry.type);
      EXPECT_EQ(10000, log_entry.region_id);
      EXPECT_EQ(2, log_entry.term);

      butil::IOBuf data;
      data = log_entry.out_data;
      braft::LogEntry* entry = new braft::LogEntry();
      entry->AddRef();
      butil::Status status = parse_configuration_meta(data, entry);
      ASSERT_TRUE(status.ok());
      entry->Release();
    }
  }

  {
    auto log_entries = log_storage->GetConfigurations(10001);
    EXPECT_EQ(2, log_entries.size());
    for (auto& log_entry : log_entries) {
      EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeConfiguration, log_entry.type);
      EXPECT_EQ(10001, log_entry.region_id);
      EXPECT_EQ(2, log_entry.term);

      butil::IOBuf data;
      data = log_entry.out_data;
      braft::LogEntry* entry = new braft::LogEntry();
      entry->AddRef();
      butil::Status status = parse_configuration_meta(data, entry);
      ASSERT_TRUE(status.ok());
      entry->Release();
    }
  }

  {
    auto log_entries = log_storage->GetConfigurations(10002);
    EXPECT_EQ(2, log_entries.size());
    for (auto& log_entry : log_entries) {
      EXPECT_EQ(dingodb::wal::LogEntryType::kEntryTypeConfiguration, log_entry.type);
      EXPECT_EQ(10002, log_entry.region_id);
      EXPECT_EQ(2, log_entry.term);

      butil::IOBuf data;
      data = log_entry.out_data;
      braft::LogEntry* entry = new braft::LogEntry();
      entry->AddRef();
      butil::Status status = parse_configuration_meta(data, entry);
      ASSERT_TRUE(status.ok());
      entry->Release();
    }
  }

  log_storage->Close();
}

TEST_F(RocksLogStorageTest, GetTerm) {
  std::string path = fmt::format("{}/{}", kLogPath, "getterm");
  auto log_storage = dingodb::wal::RocksLogStorage::New(path);
  ASSERT_TRUE(log_storage != nullptr);

  int64_t region_id = 10000;

  log_storage->RegisterClientType(dingodb::wal::ClientType::kRaft);

  ASSERT_TRUE(log_storage->Init());

  ASSERT_TRUE(log_storage->RegisterRegion(region_id));

  {
    dingodb::wal::Mutation mutation;
    mutation.type = dingodb::wal::Mutation::Type::kAppendLogEntry;
    mutation.region_id = region_id;

    std::vector<IndexRangeOption> options = {
        IndexRangeOption{region_id, 1, 60, 1, dingodb::wal::LogEntryType::kEntryTypeData},
    };

    std::vector<dingodb::wal::LogEntry> log_entries = GenLogEntries(options);
    mutation.log_entries.swap(log_entries);

    ASSERT_TRUE(log_storage->CommitMutation(&mutation));

    int64_t term = log_storage->GetTerm(region_id, 30);
    ASSERT_EQ(1, term);
  }

  {
    dingodb::wal::Mutation mutation;
    mutation.type = dingodb::wal::Mutation::Type::kAppendLogEntry;
    mutation.region_id = region_id;

    std::vector<IndexRangeOption> options = {
        IndexRangeOption{region_id, 61, 100, 2, dingodb::wal::LogEntryType::kEntryTypeData},
    };

    std::vector<dingodb::wal::LogEntry> log_entries = GenLogEntries(options);
    mutation.log_entries.swap(log_entries);

    ASSERT_TRUE(log_storage->CommitMutation(&mutation));

    int64_t term = log_storage->GetTerm(region_id, 80);
    ASSERT_EQ(2, term);
  }

  log_storage->Close();
}

TEST_F(RocksLogStorageTest, TruncatePrefix) {
  std::string path = fmt::format("{}/{}", kLogPath, "TruncatePrefix");
  auto log_storage = dingodb::wal::RocksLogStorage::New(path);
  ASSERT_TRUE(log_storage != nullptr);

  int64_t region_id = 10000;

  log_storage->RegisterClientType(dingodb::wal::ClientType::kRaft);

  ASSERT_TRUE(log_storage->Init());

  ASSERT_TRUE(log_storage->RegisterRegion(region_id));

  dingodb::wal::Mutation mutation;
  mutation.type = dingodb::wal::Mutation::Type::kAppendLogEntry;
  mutation.region_id = region_id;

  std::vector<IndexRangeOption> options = {
      IndexRangeOption{region_id, 1, 100, 1, dingodb::wal::LogEntryType::kEntryTypeData},
  };

  std::vector<dingodb::wal::LogEntry> log_entries = GenLogEntries(options);
  mutation.log_entries.swap(log_entries);

  ASSERT_TRUE(log_storage->CommitMutation(&mutation));

  int64_t term = log_storage->GetTerm(region_id, 36);
  ASSERT_EQ(1, term);

  ASSERT_EQ(1, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncatePrefix(dingodb::wal::ClientType::kRaft, region_id, 33));
  ASSERT_EQ(33, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncatePrefix(dingodb::wal::ClientType::kRaft, region_id, 66));
  ASSERT_EQ(66, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncatePrefix(dingodb::wal::ClientType::kRaft, region_id, 99));
  ASSERT_EQ(99, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncatePrefix(dingodb::wal::ClientType::kRaft, region_id, 100));
  ASSERT_EQ(100, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncatePrefix(dingodb::wal::ClientType::kRaft, region_id, 101));
  ASSERT_EQ(101, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncatePrefix(dingodb::wal::ClientType::kRaft, region_id, 111));
  ASSERT_EQ(111, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(110, log_storage->LastLogIndex(region_id));

  log_storage->Close();
}

TEST_F(RocksLogStorageTest, TruncateSuffix) {
  std::string path = fmt::format("{}/{}", kLogPath, "TruncateSuffix");
  auto log_storage = dingodb::wal::RocksLogStorage::New(path);
  ASSERT_TRUE(log_storage != nullptr);

  int64_t region_id = 10000;

  log_storage->RegisterClientType(dingodb::wal::ClientType::kRaft);

  ASSERT_TRUE(log_storage->Init());

  ASSERT_TRUE(log_storage->RegisterRegion(region_id));

  dingodb::wal::Mutation mutation;
  mutation.type = dingodb::wal::Mutation::Type::kAppendLogEntry;
  mutation.region_id = region_id;

  std::vector<IndexRangeOption> options = {
      IndexRangeOption{region_id, 1, 100, 1, dingodb::wal::LogEntryType::kEntryTypeData},
  };

  std::vector<dingodb::wal::LogEntry> log_entries = GenLogEntries(options);
  mutation.log_entries.swap(log_entries);

  ASSERT_TRUE(log_storage->CommitMutation(&mutation));

  int64_t term = log_storage->GetTerm(region_id, 36);
  ASSERT_EQ(1, term);

  ASSERT_EQ(1, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(-1, log_storage->TruncateSuffix(dingodb::wal::ClientType::kRaft, region_id, 200));
  ASSERT_EQ(1, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncateSuffix(dingodb::wal::ClientType::kRaft, region_id, 100));
  ASSERT_EQ(1, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncateSuffix(dingodb::wal::ClientType::kRaft, region_id, 90));
  ASSERT_EQ(1, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(90, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncateSuffix(dingodb::wal::ClientType::kRaft, region_id, 50));
  ASSERT_EQ(1, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(50, log_storage->LastLogIndex(region_id));

  ASSERT_EQ(0, log_storage->TruncateSuffix(dingodb::wal::ClientType::kRaft, region_id, 1));
  ASSERT_EQ(1, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(1, log_storage->LastLogIndex(region_id));

  log_storage->Close();
}

TEST_F(RocksLogStorageTest, DestroyRegionLog) {
  std::string path = fmt::format("{}/{}", kLogPath, "DestroyRegionLog");
  auto log_storage = dingodb::wal::RocksLogStorage::New(path);
  ASSERT_TRUE(log_storage != nullptr);

  int64_t region_id = 10000;

  log_storage->RegisterClientType(dingodb::wal::ClientType::kRaft);

  ASSERT_TRUE(log_storage->Init());

  ASSERT_TRUE(log_storage->RegisterRegion(region_id));

  dingodb::wal::Mutation mutation;
  mutation.type = dingodb::wal::Mutation::Type::kAppendLogEntry;
  mutation.region_id = region_id;

  std::vector<IndexRangeOption> options = {
      IndexRangeOption{region_id, 1, 100, 1, dingodb::wal::LogEntryType::kEntryTypeData},
  };

  std::vector<dingodb::wal::LogEntry> log_entries = GenLogEntries(options);
  mutation.log_entries.swap(log_entries);

  ASSERT_TRUE(log_storage->CommitMutation(&mutation));

  int64_t term = log_storage->GetTerm(region_id, 36);
  ASSERT_EQ(1, term);

  ASSERT_EQ(1, log_storage->FirstLogIndex(region_id));
  ASSERT_EQ(100, log_storage->LastLogIndex(region_id));

  ASSERT_TRUE(log_storage->DestroyRegionLog(region_id));

  dingodb::wal::LogIndexMeta log_index_meta;
  ASSERT_FALSE(log_storage->GetLogIndexMeta(region_id, log_index_meta));
}

TEST_F(RocksLogStorageTest, SyncWal) {
  GTEST_SKIP() << "skip....";

  std::string path = fmt::format("{}/{}", kLogPath, "sync");
  auto log_storage = dingodb::wal::RocksLogStorage::New(path);
  ASSERT_TRUE(log_storage != nullptr);

  int64_t region_id = 10000;

  log_storage->RegisterClientType(dingodb::wal::ClientType::kRaft);

  ASSERT_TRUE(log_storage->Init());

  ASSERT_TRUE(log_storage->RegisterRegion(region_id));

  dingodb::wal::Mutation mutation;
  mutation.type = dingodb::wal::Mutation::Type::kAppendLogEntry;
  mutation.region_id = region_id;

  std::vector<int64_t> entry_nums = {100, 1000, 10000, 100000};

  int64_t step = 100;

  int64_t index = 1;
  int64_t end_index = 0;
  for (auto entry_num : entry_nums) {
    end_index += entry_num;
    for (; index < end_index; index += step) {
      std::vector<IndexRangeOption> options = {
          IndexRangeOption{region_id, index, index + step - 1, 1, dingodb::wal::LogEntryType::kEntryTypeData},
      };

      std::vector<dingodb::wal::LogEntry> log_entries = GenLogEntries(options);
      mutation.log_entries.swap(log_entries);

      ASSERT_TRUE(log_storage->CommitMutation(&mutation));
    }

    int64_t start_time = dingodb::Helper::TimestampUs();
    log_storage->SyncWal();
    std::cout << fmt::format("entry num({}) elapsed time: {}us", entry_num, dingodb::Helper::TimestampUs() - start_time)
              << std::endl;
  }

  log_storage->Close();
}

struct Param {
  dingodb::wal::RocksLogStoragePtr log_storage;
  int64_t region_id;
  std::vector<int64_t> region_ids;
};

void* WorkFunc(void* arg) {
  Param* param = static_cast<Param*>(arg);
  auto log_segment_adapter =
      std::make_shared<dingodb::wal::RocksLogStorageWrapper>(param->region_id, param->log_storage);

  std::cout << fmt::format("start worker {} ......", param->region_id) << std::endl;

  braft::ConfigurationManager configuration_manager;
  if (log_segment_adapter->init(&configuration_manager) != 0) {
    std::cerr << "init RocksLogStorageWrapper fail." << std::endl;
    return nullptr;
  }

  int64_t term = 1;
  int64_t step = 0;
  for (int64_t index = 1; index < 50000;) {
    std::vector<IndexRangeOption> options;
    if (index % 64 == 0) {
      options = {
          IndexRangeOption{10000, index, index, term, dingodb::wal::LogEntryType::kEntryTypeConfiguration},
      };
      ++index;
      ++term;
    } else {
      step = dingodb::Helper::GenerateRealRandomInteger(1, 32);
      options = {
          // term 1
          IndexRangeOption{10000, index, index + step, term, dingodb::wal::LogEntryType::kEntryTypeData},
      };
      index += step + 1;
    }

    std::vector<braft::LogEntry*> entries = GenBraftLogEntries(options);

    // std::cout << fmt::format("region({}) term({}) index([{}, {}]) step({})", param->region_id, term,
    //                          options[0].start_index, options[0].end_index, step)
    //           << std::endl;

    log_segment_adapter->append_entries(entries, nullptr);

    for (auto* entry : entries) {
      entry->Release();
    }

    // bthread_usleep(dingodb::Helper::GenerateRealRandomInteger(1, 10) * 1000);
  }

  std::cout << fmt::format("stop worker {} ......", param->region_id) << std::endl;

  return nullptr;
}

void* TruncateWorkFunc(void* arg) {
  Param* param = static_cast<Param*>(arg);

  auto log_storage = param->log_storage;

  std::cout << fmt::format("start truncate worker {} ......", param->region_id) << std::endl;

  for (;;) {
    for (auto region_id : param->region_ids) {
      int64_t first_index = log_storage->FirstLogIndex(region_id);
      int64_t last_index = log_storage->LastLogIndex(region_id);
      if (last_index <= first_index) continue;

      int64_t truncate_prefix = (first_index + last_index) / 2;
      if (truncate_prefix > 0) {
        log_storage->TruncatePrefix(dingodb::wal::ClientType::kRaft, region_id, truncate_prefix);
      }
    }

    bthread_usleep(dingodb::Helper::GenerateRealRandomInteger(1, 10) * 1000 * 1000);
  }

  return nullptr;
}

TEST_F(RocksLogStorageTest, AppendByMultiProducer) {
  GTEST_SKIP() << "skip...";

  std::string path = fmt::format("{}/{}", kLogPath, "multiproducer");
  auto log_storage = dingodb::wal::RocksLogStorage::New(path);
  ASSERT_TRUE(log_storage != nullptr);

  log_storage->RegisterClientType(dingodb::wal::ClientType::kRaft);

  ASSERT_TRUE(log_storage->Init());

  log_storage->PrintLogIndexMeta();

  return;

  const int bthread_num = 2;

  std::vector<bthread_t> tids;
  std::vector<Param> params(bthread_num);

  std::vector<int64_t> region_ids;
  for (int i = 0; i < bthread_num; ++i) {
    bthread_t tid;
    auto& param = params[i];
    param.log_storage = log_storage;
    param.region_id = 200001 + i;
    bthread_start_background(&tid, nullptr, WorkFunc, &param);
    tids.push_back(tid);
    region_ids.push_back(param.region_id);
  }

  bthread_t tid;
  Param truncate_param;
  truncate_param.log_storage = log_storage;
  truncate_param.region_ids = region_ids;
  bthread_start_background(&tid, nullptr, TruncateWorkFunc, &truncate_param);
  tids.push_back(tid);

  for (;;) {
    log_storage->PrintLogIndexMeta();
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  for (unsigned long tid : tids) {
    bthread_join(tid, nullptr);
  }

  log_storage->Close();
}

TEST_F(RocksLogStorageTest, Perf) {
  GTEST_SKIP() << "skip...";

  std::string path = fmt::format("{}/{}", kLogPath, "perf");
  auto log_storage = dingodb::wal::RocksLogStorage::New(path);
  ASSERT_TRUE(log_storage != nullptr);

  log_storage->RegisterClientType(dingodb::wal::ClientType::kRaft);

  ASSERT_TRUE(log_storage->Init());

  // log_storage->PrintLogIndexMeta();

  int64_t start_time = dingodb::Helper::TimestampUs();

  const int bthread_num = 32;

  std::vector<bthread_t> tids;
  std::vector<Param> params(bthread_num);

  std::vector<int64_t> region_ids;
  for (int i = 0; i < bthread_num; ++i) {
    bthread_t tid;
    auto& param = params[i];
    param.log_storage = log_storage;
    param.region_id = 200001 + i;
    bthread_start_background(&tid, nullptr, WorkFunc, &param);
    tids.push_back(tid);
    region_ids.push_back(param.region_id);
  }

  // for (;;) {
  //   // log_storage->PrintLogIndexMeta();
  //   std::this_thread::sleep_for(std::chrono::seconds(5));
  // }

  for (unsigned long tid : tids) {
    bthread_join(tid, nullptr);
  }

  std::cout << "elapsed time: " << dingodb::Helper::TimestampUs() - start_time << std::endl;

  log_storage->Close();
}

// TEST_F(RocksLogStorageTest, PerfRocksdb) {
//   std::string path = "./unit_test/log/raw_rocksdb";

//   rocksdb::Options db_options;
//   db_options.create_if_missing = true;
//   db_options.create_missing_column_families = true;
//   db_options.max_background_jobs = 8;
//   db_options.max_subcompactions = 6;
//   db_options.stats_dump_period_sec = 60;
//   db_options.use_direct_io_for_flush_and_compaction = true;
//   db_options.manual_wal_flush = true;

//   // std::shared_ptr<rocksdb::RateLimiter> rate_limiter;
//   // rate_limiter.reset(
//   //     rocksdb::NewGenericRateLimiter(1024 * 1024 * 32, 500 * 1000, 10, rocksdb::RateLimiter::Mode::kWritesOnly,
//   //     false));
//   // db_options.rate_limiter = rate_limiter;

//   rocksdb::DB* db = nullptr;
//   rocksdb::Status status = rocksdb::DB::Open(db_options, path, &db);
//   ASSERT_TRUE(status.ok()) << fmt::format("open rocksdb fail, error: {}", status.ToString());

//   db->Close();
// }
