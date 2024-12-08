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
#include <memory>
#include <string>

#include "br/sst_file_writer.h"
#include "fmt/core.h"
#include "proto/error.pb.h"
#include "rocksdb/sst_file_reader.h"

class BrSstFileWriterTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static std::string file_path = "./br_sst_file_writer.sst";
  std::shared_ptr<br::SstFileWriter> sst;
};

TEST_F(BrSstFileWriterTest, SaveFile1) {
  rocksdb::Options options;
  sst = std::make_shared<br::SstFileWriter>(options);

  std::map<std::string, std::string> kvs;

  butil::Status status;
  status = sst->SaveFile(kvs, file_path);

  EXPECT_EQ(status.error_code(), 4);

  sst.reset();

  sst = std::make_shared<br::SstFileWriter>(options);

  status = sst->SaveFile(kvs, file_path);

  EXPECT_EQ(status.error_code(), 4);
  sst.reset();
}

TEST_F(BrSstFileWriterTest, SaveFile2) {
  rocksdb::Options options;
  sst = std::make_shared<br::SstFileWriter>(options);

  std::map<std::string, std::string> kvs;

  for (int i = 0; i < 10; i++) {
    kvs.insert({fmt::format("key_{}", i), fmt::format("value_{}", i)});
  }
  butil::Status status;
  status = sst->SaveFile(kvs, file_path);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::Errno::OK);
  int64_t size = sst->GetSize();
  EXPECT_EQ(size, 1131);
  sst.reset();

  sst = std::make_shared<br::SstFileWriter>(options);
  status = sst->SaveFile(kvs, file_path);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::Errno::OK);
  size = sst->GetSize();
  EXPECT_EQ(size, 1131);
  sst.reset();
}

TEST_F(BrSstFileWriterTest, Read) {
  rocksdb::Options options;
  rocksdb::SstFileReader reader(options);
  auto status = reader.Open(file_path);
  EXPECT_EQ(status.code(), status.OK().code());

  int i = 0;
  auto *iter = reader.NewIterator(rocksdb::ReadOptions());
  iter->SeekToFirst();
  while (iter->Valid()) {
    EXPECT_EQ(iter->key(), fmt::format("key_{}", i));
    EXPECT_EQ(iter->value(), fmt::format("value_{}", i));
    iter->Next();
    i++;
  }

  EXPECT_EQ(i, 10);

  std::filesystem::remove(file_path);
}