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
#include "fmt/core.h"

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
