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

#include <filesystem>
#include <string>

#include "br/utils.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/meta.pb.h"

class BrUtilsTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  inline static std::string file_path = "./br_utils_test_file";
  inline static std::string dir_path = "./br_utils_test_dir";
  inline static std::string dir_recursion_path = "./br_utils_test_dir/a/b";
};

TEST_F(BrUtilsTest, FormatStatusError) {
  butil::Status status;
  auto s = br::Utils::FormatStatusError(status);
  EXPECT_NE(s, "");
  DINGO_LOG(INFO) << s;

  status.set_error(dingodb::pb::error::EILLEGAL_PARAMTETERS, "invalid parameters");
  s = br::Utils::FormatStatusError(status);
  EXPECT_NE(s, "");
  DINGO_LOG(INFO) << s;
}

TEST_F(BrUtilsTest, FormatResponseError) {
  dingodb::pb::meta::ImportMetaResponse response;
  auto s = br::Utils::FormatResponseError(response);
  EXPECT_NE(s, "");
  DINGO_LOG(INFO) << s;

  response.mutable_error()->set_errcode(dingodb::pb::error::EILLEGAL_PARAMTETERS);
  response.mutable_error()->set_errmsg("invalid parameters");
  s = br::Utils::FormatResponseError(response);
  EXPECT_NE(s, "");
  DINGO_LOG(INFO) << s;
}

TEST_F(BrUtilsTest, CreateFile) {
  std::ofstream writer;
  auto status = br::Utils::CreateFile(writer, file_path);
  ASSERT_TRUE(status.ok());
  writer.close();
}

TEST_F(BrUtilsTest, FileExistsAndRegular) {
  auto status = br::Utils::FileExistsAndRegular(file_path);
  std::filesystem::remove(file_path);
}

TEST_F(BrUtilsTest, CreateDir) {
  auto status = br::Utils::CreateDir(dir_path);
  ASSERT_TRUE(status.ok());
}

TEST_F(BrUtilsTest, DirExists) {
  auto status = br::Utils::DirExists(dir_path);
  ASSERT_TRUE(status.ok());
}

TEST_F(BrUtilsTest, CreateDirRecursion) {
  auto status = br::Utils::CreateDirRecursion(dir_recursion_path);
  ASSERT_TRUE(status.ok());
}

TEST_F(BrUtilsTest, ClearDir) {
  auto status = br::Utils::ClearDir(dir_path);
  ASSERT_TRUE(status.ok());

  br::Utils::RemoveAllDir(dir_path, true);
}

TEST_F(BrUtilsTest, ReadFile) {
  std::ofstream writer;
  auto status = br::Utils::CreateFile(writer, file_path);
  ASSERT_TRUE(status.ok());
  writer << "hello world \n";
  writer << "nin hao!! \n";
  writer.close();

  std::ifstream reader;

  status = br::Utils::ReadFile(reader, file_path);
  ASSERT_TRUE(status.ok());

  std::string line;
  reader >> line;
  EXPECT_EQ(line, "hello");
  reader >> line;
  EXPECT_EQ(line, "world");
  reader >> line;
  EXPECT_EQ(line, "nin");
  reader >> line;
  EXPECT_EQ(line, "hao!!");

  reader.close();

  status = br::Utils::ReadFile(reader, file_path);
  ASSERT_TRUE(status.ok());

  std::getline(reader, line);
  EXPECT_EQ(line, "hello world ");

  std::getline(reader, line);
  EXPECT_EQ(line, "nin hao!! ");

  std::filesystem::remove(file_path);
}

TEST_F(BrUtilsTest, CheckBackupMeta) {
  std::string storage_internal = "./backup2";
  const std::string& file_name = "backupmeta";

  const std::string& dir_name = "";
  const std::string& exec_node = "backup";
  std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta;
  backup_meta = std::make_shared<dingodb::pb::common::BackupMeta>();

  backup_meta->set_remark("remark");
  backup_meta->set_dir_name("");
  backup_meta->set_file_name("backupmeta");
  backup_meta->set_file_size(1896);
  backup_meta->set_encryption("b5b8a69893a7666fb6d7d03cc4d2007e2e7b7b63");
  backup_meta->set_exec_node("backup");

  auto status = br::Utils::CheckBackupMeta(backup_meta, storage_internal, file_name, dir_name, exec_node);
  EXPECT_TRUE(status.ok());
}