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
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "butil/endpoint.h"
#include "butil/strings/string_split.h"
#include "fmt/core.h"
#include "vector/vector_index_snapshot.h"

class VectorIndexSnapshotTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}
};

static butil::EndPoint ParseHost(const std::string& uri) {
  std::vector<std::string> strs;
  butil::SplitString(uri, '/', &strs);

  if (strs.size() < 4) {
    return {};
  }
  std::string host_and_port = strs[2];

  butil::EndPoint endpoint;
  butil::str2endpoint(host_and_port.c_str(), &endpoint);

  return endpoint;
}

static int64_t ParseReaderId(const std::string& uri) {
  std::vector<std::string> strs;
  butil::SplitString(uri, '/', &strs);

  if (strs.size() < 4) {
    return 0;
  }

  std::string& reader_id_str = strs[3];

  char* end = nullptr;
  int64_t result = std::strtoull(reader_id_str.c_str(), &end, 10);
  if ((end - reader_id_str.c_str()) + 1 <= reader_id_str.size()) {
    return 0;
  }

  return result;
}

TEST_F(VectorIndexSnapshotTest, ParseHost) {  // NOLINT
  {
    std::string uri = "remote://172.20.3.17:21002/688558464596561784";
    auto endpoint = ParseHost(uri);

    EXPECT_EQ("172.20.3.17", std::string(butil::ip2str(endpoint.ip).c_str()));
    EXPECT_EQ(21002, endpoint.port);
  }

  {
    std::string uri = "remote:/127.0.0.1:20001/12346";
    auto endpoint = ParseHost(uri);
    EXPECT_EQ(0, endpoint.port);
  }
}

TEST_F(VectorIndexSnapshotTest, ParseReaderId) {  // NOLINT
  {
    std::string uri = "remote://127.0.0.1:20001/12346";
    EXPECT_EQ(12346, ParseReaderId(uri));
  }
  {
    std::string uri = "remote://127.0.0.1:20001/abc";
    EXPECT_EQ(0, ParseReaderId(uri));
  }

  {
    std::string uri = "remote://127.0.0.1:20001/123abc";
    EXPECT_EQ(0, ParseReaderId(uri));
  }
}

TEST_F(VectorIndexSnapshotTest, AddSnapshot) {  // NOLINT
  auto snapshot_set = std::make_shared<dingodb::vector_index::SnapshotMetaSet>(100);

  EXPECT_EQ(nullptr, snapshot_set->GetLastSnapshot());

  {
    int64_t vector_index_id = 100;
    int64_t snapshot_log_id = 5;
    std::string path = fmt::format("/tmp/{}/snapshot_{:020}", vector_index_id, snapshot_log_id);
    auto snapshot = dingodb::vector_index::SnapshotMeta::New(vector_index_id, path);

    snapshot_set->AddSnapshot(snapshot);
    EXPECT_EQ(snapshot, snapshot_set->GetLastSnapshot());
  }

  {
    int64_t vector_index_id = 100;
    int64_t snapshot_log_id = 11;
    std::string path = fmt::format("/tmp/{}/snapshot_{:020}", vector_index_id, snapshot_log_id);
    auto snapshot = dingodb::vector_index::SnapshotMeta::New(vector_index_id, path);

    snapshot_set->AddSnapshot(snapshot);
    EXPECT_EQ(snapshot, snapshot_set->GetLastSnapshot());
    EXPECT_EQ(1, snapshot_set->GetSnapshots().size());
  }

  {
    int64_t vector_index_id = 101;
    int64_t snapshot_log_id = 6;
    std::string path = fmt::format("/tmp/{}/snapshot_{:020}", vector_index_id, snapshot_log_id);
    auto snapshot = dingodb::vector_index::SnapshotMeta::New(vector_index_id, path);

    snapshot_set->AddSnapshot(snapshot);
    EXPECT_EQ(snapshot, snapshot_set->GetLastSnapshot());
  }

  {
    int64_t vector_index_id = 101;
    int64_t snapshot_log_id = 16;
    std::string path = fmt::format("/tmp/{}/snapshot_{:020}", vector_index_id, snapshot_log_id);
    auto snapshot = dingodb::vector_index::SnapshotMeta::New(vector_index_id, path);

    snapshot_set->AddSnapshot(snapshot);
    EXPECT_EQ(snapshot, snapshot_set->GetLastSnapshot());
    EXPECT_EQ(1, snapshot_set->GetSnapshots().size());
  }
}
