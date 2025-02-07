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

#include "br/sst_file_reader.h"
#include "br/sst_file_writer.h"
#include "fmt/core.h"
#include "proto/error.pb.h"

class BrSstFileReaderTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static std::string file_path = "./br_sst_file_reader.sst";
  std::shared_ptr<br::SstFileWriter> writer_sst;
  std::shared_ptr<br::SstFileReader> reader_sst;
};

TEST_F(BrSstFileReaderTest, SaveFile) {
  rocksdb::Options options;
  writer_sst = std::make_shared<br::SstFileWriter>(options);

  std::map<std::string, std::string> kvs;

  for (int i = 0; i < 10; i++) {
    kvs.insert({fmt::format("key_{}", i), fmt::format("value_{}", i)});
  }
  butil::Status status;
  status = writer_sst->SaveFile(kvs, file_path);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::Errno::OK);
  int64_t size = writer_sst->GetSize();
  EXPECT_EQ(size, 1152);
  writer_sst.reset();

  writer_sst = std::make_shared<br::SstFileWriter>(options);
  status = writer_sst->SaveFile(kvs, file_path);
  EXPECT_EQ(status.error_code(), dingodb::pb::error::Errno::OK);
  size = writer_sst->GetSize();
  EXPECT_EQ(size, 1152);
  writer_sst.reset();
}

TEST_F(BrSstFileReaderTest, Read) {
  reader_sst = std::make_shared<br::SstFileReader>();

  int i = 0;
  std::map<std::string, std::string> kvs;
  auto status = reader_sst->ReadFile(file_path, kvs);

  for (const auto &[key, value] : kvs) {
    EXPECT_EQ(key, fmt::format("key_{}", i));
    EXPECT_EQ(value, fmt::format("value_{}", i));
    i++;
  }

  EXPECT_EQ(i, 10);

  std::filesystem::remove(file_path);
}