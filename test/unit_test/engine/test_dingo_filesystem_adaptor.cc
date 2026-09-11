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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "gflags/gflags.h"
#include "mvcc/codec.h"
#include "raft/dingo_filesystem_adaptor.h"
#include "server/server.h"

DECLARE_string(role);

namespace dingodb {

DECLARE_string(raft_snapshot_policy);
DECLARE_int64(snapshot_timeout_min);

namespace {

using Records = std::vector<std::pair<std::string, std::string>>;

// Expose only the protected constructor, without replacing any reader behavior.
class SnapshotDataReader : public DingoDataReaderAdaptor {
 public:
  SnapshotDataReader(int64_t region_id, const std::string& path, DingoFileSystemAdaptor* filesystem,
                     std::shared_ptr<IteratorContext> context)
      : DingoDataReaderAdaptor(region_id, path, filesystem, std::move(context)) {}
};

testing::AssertionResult ReadChunk(braft::FileAdaptor& reader, off_t offset, size_t limit, Records& records,
                                   ssize_t& bytes_read) {
  butil::IOPortal portal;
  bytes_read = reader.read(&portal, offset, limit);
  if (bytes_read < 0 || static_cast<size_t>(bytes_read) != portal.size()) {
    return testing::AssertionFailure() << "snapshot read returned " << bytes_read << " for " << portal.size()
                                       << " bytes";
  }

  const std::string data = portal.to_string();
  size_t position = 0;
  records.clear();
  while (position < data.size()) {
    std::string fields[2];
    for (auto& field : fields) {
      if (data.size() - position < sizeof(size_t)) {
        return testing::AssertionFailure() << "truncated snapshot field length";
      }
      size_t length = 0;
      std::memcpy(&length, data.data() + position, sizeof(length));
      position += sizeof(length);
      if (length > data.size() - position) {
        return testing::AssertionFailure() << "truncated snapshot field";
      }
      field.assign(data.data() + position, length);
      position += length;
    }
    records.emplace_back(std::move(fields[0]), std::move(fields[1]));
  }
  return testing::AssertionSuccess();
}

}  // namespace

class DingoSnapshotReaderTest : public testing::Test {
 protected:
  DingoSnapshotReaderTest() : previous_meta_(Server::GetInstance().store_meta_manager_) {}

  ~DingoSnapshotReaderTest() override {
    // This also runs after a fatal SetUp/TestBody assertion, restoring other suites' singleton state.
    Server::GetInstance().store_meta_manager_ = std::move(previous_meta_);
  }

  void SetUp() override {
    FLAGS_role = "store";
    FLAGS_raft_snapshot_policy = "dingo";
    char directory[] = "/tmp/dingo-snapshot-reader-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    directory_ = directory;

    auto config = std::make_shared<YamlConfig>();
    ASSERT_EQ(config->Load("store:\n  path: " + directory_ + "\n"), 0);
    engine_ = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine_->Init(config, {Constant::kStoreDataCF, Constant::kStoreMetaCF}));

    auto metadata = std::make_shared<StoreMetaManager>(std::make_shared<MetaReader>(engine_),
                                                       std::make_shared<MetaWriter>(engine_));
    pb::common::RegionDefinition definition;
    definition.set_id(kRegionId);
    definition.set_name("snapshot-reader-regression");
    definition.mutable_range()->set_start_key("b");
    definition.mutable_range()->set_end_key("m");
    auto region = store::Region::New(definition);
    ASSERT_NE(region, nullptr);
    region->SetState(pb::common::NORMAL);
    metadata->GetStoreRegionMeta()->AddRegion(region);
    Server::GetInstance().store_meta_manager_ = std::move(metadata);

    // Include exact encoded boundaries and timestamped keys in both adjacent regions.
    records_ = {{mvcc::Codec::EncodeBytes(std::string("a")), "before"},
                {mvcc::Codec::EncodeBytes(std::string("b")), "lower"},
                {mvcc::Codec::EncodeKey(std::string("b"), 7), "lower-version"},
                {mvcc::Codec::EncodeKey(std::string("c"), 7), "inside"},
                {mvcc::Codec::EncodeBytes(std::string("m")), "upper"},
                {mvcc::Codec::EncodeKey(std::string("m"), 7), "next-region"},
                {mvcc::Codec::EncodeKey(std::string("z"), 7), "last"}};
    for (const auto& [key, value] : records_) {
      pb::common::KeyValue kv;
      kv.set_key(key);
      kv.set_value(value);
      ASSERT_TRUE(engine_->Writer()->KvPut(Constant::kStoreDataCF, kv).ok());
    }

    snapshot_ = std::make_shared<SnapshotContext>(engine_);
    snapshot_->range = definition.range();
    snapshot_path_ = directory_ + "/snapshot_00001";
    data_path_ = snapshot_path_ + "/" + Constant::kStoreDataCF + kSnapshotDataFile;
    filesystem_ = std::make_unique<DingoFileSystemAdaptor>(kRegionId);
    // Install a real engine snapshot at the boundary normally supplied by the raft state machine.
    // No raft server, election, network port, or production reader method is substituted.
    auto& environment = filesystem_->snapshot_context_env_map_[snapshot_path_];
    environment.snapshot_context = snapshot_;
    environment.count = 1;
    filesystem_->mutil_snapshot_cond_.Increase();
  }

  void TearDown() override {
    if (filesystem_ != nullptr) {
      filesystem_->close_snapshot(snapshot_path_);
      filesystem_.reset();
    }
    snapshot_.reset();
    if (engine_ != nullptr) {
      engine_->Close();
    }
    if (!directory_.empty()) {
      std::filesystem::remove_all(directory_);
    }
  }

  std::unique_ptr<braft::FileAdaptor> OpenReader() {
    butil::File::Error error = butil::File::FILE_OK;
    return std::unique_ptr<braft::FileAdaptor>(filesystem_->open(data_path_, O_RDONLY, nullptr, &error));
  }

  static constexpr int64_t kRegionId = 1486001;
  google::FlagSaver flags_;
  std::shared_ptr<StoreMetaManager> previous_meta_;
  std::string directory_;
  std::string snapshot_path_;
  std::string data_path_;
  std::shared_ptr<RocksRawEngine> engine_;
  std::shared_ptr<SnapshotContext> snapshot_;
  std::unique_ptr<DingoFileSystemAdaptor> filesystem_;
  Records records_;
};

TEST_F(DingoSnapshotReaderTest, OpenReaderIncludesLowerAndExcludesAdjacentRegion) {
  auto reader = OpenReader();
  ASSERT_NE(reader, nullptr);
  Records actual;
  ssize_t bytes_read = 0;
  ASSERT_TRUE(ReadChunk(*reader, 0, 4096, actual, bytes_read));
  const Records expected(records_.begin() + 1, records_.begin() + 4);
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(reader->size(), bytes_read);

  ssize_t eof_bytes = -1;
  ASSERT_TRUE(ReadChunk(*reader, bytes_read, 4096, actual, eof_bytes));
  EXPECT_EQ(eof_bytes, 0);
}

TEST_F(DingoSnapshotReaderTest, ExpiredRestartRetainsBothBoundsAndSnapshot) {
  FLAGS_snapshot_timeout_min = 0;
  auto reader = OpenReader();
  ASSERT_NE(reader, nullptr);
  Records actual;
  ssize_t bytes_read = 0;
  off_t offset = 0;
  // Two distinct packets ensure retrying offset zero cannot use the last-packet cache.
  ASSERT_TRUE(ReadChunk(*reader, offset, 1, actual, bytes_read));
  EXPECT_EQ(actual, Records({records_[1]}));
  offset += bytes_read;
  ASSERT_TRUE(ReadChunk(*reader, offset, 1, actual, bytes_read));
  EXPECT_EQ(actual, Records({records_[2]}));
  offset += bytes_read;
  ASSERT_TRUE(ReadChunk(*reader, offset, 4096, actual, bytes_read));
  EXPECT_EQ(actual, Records({records_[3]}));

  // A reset must reuse the snapshot, not observe writes made after it was opened.
  pb::common::KeyValue kv;
  kv.set_key(records_[3].first);
  kv.set_value("changed-after-snapshot");
  ASSERT_TRUE(engine_->Writer()->KvPut(Constant::kStoreDataCF, kv).ok());

  const auto context = snapshot_->data_iterators.at(Constant::kStoreDataCF);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (context->offset_update_time.GetTime() <= 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_GT(context->offset_update_time.GetTime(), 0);
  ASSERT_TRUE(ReadChunk(*reader, 0, 4096, actual, bytes_read));
  const Records expected(records_.begin() + 1, records_.begin() + 4);
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(reader->size(), bytes_read);
}

TEST_F(DingoSnapshotReaderTest, EmptyIteratorUpperBoundReadsToEnd) {
  // EncodeRange encodes even an empty plain key; the unlimited contract is on IteratorContext.
  auto context = std::make_shared<IteratorContext>();
  context->region_id = kRegionId;
  context->cf_name = Constant::kStoreDataCF;
  context->lower_bound = records_[1].first;
  context->snapshot_context = snapshot_.get();
  context->reading = true;
  IteratorOptions options;
  options.lower_bound = context->lower_bound;
  context->iter = engine_->Reader()->NewIterator(Constant::kStoreDataCF, snapshot_->snapshot, options);
  context->iter->Seek(context->lower_bound);
  snapshot_->data_iterators[Constant::kStoreDataCF] = context;
  SnapshotDataReader reader(kRegionId, data_path_, filesystem_.get(), context);
  reader.Open();

  Records actual;
  ssize_t bytes_read = 0;
  ASSERT_TRUE(ReadChunk(reader, 0, 4096, actual, bytes_read));
  const Records expected(records_.begin() + 1, records_.end());
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(reader.size(), bytes_read);
}

}  // namespace dingodb
