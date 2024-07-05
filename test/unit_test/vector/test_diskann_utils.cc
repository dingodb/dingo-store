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

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "butil/status.h"
#include "diskann/diskann_utils.h"
#include "proto/error.pb.h"

// prepare for test
// sudo echo "12345" > /var/dummy.txt
// sudo mkdir /var/a
// sudo echo "12345" > /var/a/1.txt

namespace dingodb {

class DiskANNUtilsTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(DiskANNUtilsTest, FileExistsAndRegular) {
  butil::Status status;

  // emtpy file path
  {
    std::string file_path;
    status = DiskANNUtils::FileExistsAndRegular(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
  }

  // file path is dir
  {
    std::string file_path = "./test_dir";
    std::filesystem::create_directory("test_dir");
    status = DiskANNUtils::FileExistsAndRegular(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_REGULAR);
    std::filesystem::remove(file_path);
  }

  // file path is link
  {
    std::string file_path = "./1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "11111111111111";
    output_file.close();
    std::string link_path = "./link_1.txt";
    std::filesystem::create_symlink(file_path, link_path);
    status = DiskANNUtils::FileExistsAndRegular(link_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove(file_path);
    std::filesystem::remove(link_path);
  }

  // file path is no exist
  {
    std::string file_path = "./1.txt";
    status = DiskANNUtils::FileExistsAndRegular(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
  }

  // file path is no permission
  {
    std::string file_path = "/var/dummy.txt";
    status = DiskANNUtils::FileExistsAndRegular(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    file_path = "/var/dummy2.txt";
    status = DiskANNUtils::FileExistsAndRegular(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);

    file_path = "/var/a";
    status = DiskANNUtils::FileExistsAndRegular(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_REGULAR);
  }

  // file path is regular
  {
    std::string file_path = "./1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "11111111111111";
    output_file.close();

    status = DiskANNUtils::FileExistsAndRegular(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove(file_path);
  }
}

TEST_F(DiskANNUtilsTest, DirExists) {
  butil::Status status;

  // emtpy dir path
  {
    std::string dir_path;
    status = DiskANNUtils::DirExists(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
  }

  // dir path is file
  {
    std::string dir_path = "./1.txt";
    std::ofstream output_file(dir_path, std::ios::out | std::ios::trunc);
    output_file << "11";
    output_file.close();
    status = DiskANNUtils::DirExists(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_DIRECTORY);
    std::filesystem::remove(dir_path);
  }

  // dir path is link
  {
    std::string file_path = "./a";
    std::filesystem::create_directory(file_path);
    std::string link_path = "./link_a";
    std::filesystem::create_symlink(file_path, link_path);
    status = DiskANNUtils::DirExists(link_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove(file_path);
    std::filesystem::remove(link_path);
  }

  // dir path is no permission
  {
    std::string file_path = "/var/a";
    status = DiskANNUtils::DirExists(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
  }

  // dir path is ok
  {
    std::string dir_path = "./a";
    std::filesystem::create_directory(dir_path);
    std::string file_path = dir_path + "/1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "11111111111111";
    output_file.close();
    status = DiskANNUtils::DirExists(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove_all(dir_path);
  }
}

TEST_F(DiskANNUtilsTest, ClearDir) {
  butil::Status status;

  // emtpy dir path
  {
    std::string dir_path;
    status = DiskANNUtils::ClearDir(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
  }

  // dir path is file
  {
    std::string dir_path = "./1.txt";
    std::ofstream output_file(dir_path, std::ios::out | std::ios::trunc);
    output_file << "11";
    output_file.close();
    status = DiskANNUtils::ClearDir(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_DIRECTORY);
    std::filesystem::remove(dir_path);
  }

  // dir path is link
  {
    std::string dir_path = "./a";
    std::filesystem::create_directory(dir_path);
    std::string file_path = dir_path + "/1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "1";
    output_file.close();
    std::string link_path = "./link_a";
    std::filesystem::create_directory_symlink(dir_path, link_path);
    status = DiskANNUtils::ClearDir(link_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove(file_path);
    std::filesystem::remove(link_path);
  }

  // dir path is no permission
  {
    std::string file_path = "/var/a";
    status = DiskANNUtils::ClearDir(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_PERMISSION_DENIED);
  }

  // dir path is ok
  {
    std::string dir_path = "./a";
    std::filesystem::create_directory(dir_path);
    std::string file_path = dir_path + "/1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "1";
    output_file.close();
    status = DiskANNUtils::ClearDir(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove_all(dir_path);
  }

  // dir path is ok
  {
    std::string dir_path = "./a";
    std::filesystem::create_directory(dir_path);
    status = DiskANNUtils::ClearDir(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove_all(dir_path);
  }
}

TEST_F(DiskANNUtilsTest, RemoveFile) {
  butil::Status status;

  // emtpy file path
  {
    std::string file_path;
    status = DiskANNUtils::RemoveFile(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
  }

  // file path is dir
  {
    std::string file_path = "./test_dir";
    std::filesystem::create_directory("test_dir");
    status = DiskANNUtils::RemoveFile(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_REGULAR);
    std::filesystem::remove(file_path);
  }

  // file path is link
  {
    std::string file_path = "./1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "11111111111111";
    output_file.close();
    std::string link_path = "./link_1.txt";
    std::filesystem::create_symlink(file_path, link_path);
    status = DiskANNUtils::RemoveFile(link_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove(file_path);
    std::filesystem::remove(link_path);
  }

  // file path is no exist
  {
    std::string file_path = "./1.txt";
    status = DiskANNUtils::RemoveFile(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
  }

  // file path is no permission
  {
    std::string file_path = "/var/dummy.txt";
    status = DiskANNUtils::RemoveFile(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_PERMISSION_DENIED);
  }

  // file path is regular
  {
    std::string file_path = "./1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "11111111111111";
    output_file.close();
    status = DiskANNUtils::RemoveFile(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove(file_path);
  }
}

TEST_F(DiskANNUtilsTest, RemoveDir) {
  butil::Status status;

  // emtpy dir path
  {
    std::string dir_path;
    status = DiskANNUtils::RemoveDir(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
  }

  // dir path is file
  {
    std::string dir_path = "./1.txt";
    std::ofstream output_file(dir_path, std::ios::out | std::ios::trunc);
    output_file << "11";
    output_file.close();
    status = DiskANNUtils::RemoveDir(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_DIRECTORY);
    std::filesystem::remove(dir_path);
  }

  // dir path is link
  {
    std::string dir_path = "./a";
    std::filesystem::create_directory(dir_path);
    std::string file_path = dir_path + "/1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "1";
    output_file.close();
    std::string link_path = "./link_a";
    std::filesystem::create_directory_symlink(dir_path, link_path);
    status = DiskANNUtils::RemoveDir(link_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove(file_path);
    std::filesystem::remove(link_path);
  }

  // dir path is no permission
  {
    std::string file_path = "/var/a";
    status = DiskANNUtils::RemoveDir(file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_PERMISSION_DENIED);
  }

  // dir path is ok
  {
    std::string dir_path = "./a";
    std::filesystem::create_directory(dir_path);
    std::string file_path = dir_path + "/1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "1";
    output_file.close();
    status = DiskANNUtils::RemoveDir(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove_all(dir_path);
  }

  // dir path is ok
  {
    std::string dir_path = "./a";
    std::filesystem::create_directory(dir_path);
    status = DiskANNUtils::RemoveDir(dir_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove_all(dir_path);
  }
}

TEST_F(DiskANNUtilsTest, RenameDir) {
  butil::Status status;

  // emtpy file path
  {
    std::string old_file_path, new_file_path;
    status = DiskANNUtils::Rename(old_file_path, new_file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
  }

  // dir path is file
  {
    std::string old_file_path = "./1.txt";
    std::string new_file_path = "./2.txt";
    std::ofstream output_file2(new_file_path, std::ios::out | std::ios::trunc);
    output_file2 << "11";
    output_file2.close();
    status = DiskANNUtils::Rename(old_file_path, new_file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    std::filesystem::remove(old_file_path);
    std::filesystem::remove(new_file_path);
  }

  // dir path is link
  {
    std::string old_file_path = "./a";
    std::string new_file_path = "./b";
    std::filesystem::create_directory(new_file_path);
    status = DiskANNUtils::Rename(old_file_path, new_file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    std::filesystem::remove_all(new_file_path);
  }

  {
    // dir path is file
    {
      std::string old_file_path = "./1.txt";
      std::string new_file_path = "./2.txt";
      std::ofstream output_file(old_file_path, std::ios::out | std::ios::trunc);
      output_file << "11";
      output_file.close();
      std::ofstream output_file2(new_file_path, std::ios::out | std::ios::trunc);
      output_file2 << "11";
      output_file2.close();
      status = DiskANNUtils::Rename(old_file_path, new_file_path);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      std::filesystem::remove(old_file_path);
      std::filesystem::remove(new_file_path);
    }
  }

  // dir path is link
  {
    std::string old_file_path = "./a";
    std::string new_file_path = "./b";
    std::filesystem::create_directory(old_file_path);
    std::filesystem::create_directory(new_file_path);
    std::string file_path = old_file_path + "/1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "1";
    output_file.close();
    std::string link_path = "./link_a";
    std::filesystem::create_directory_symlink(old_file_path, link_path);
    status = DiskANNUtils::Rename(old_file_path, new_file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove_all(new_file_path);
    std::filesystem::remove(link_path);
  }

  {
    // dir path is file
    {
      std::string old_file_path = "./1.txt";
      std::string new_file_path = "./2.txt";
      std::ofstream output_file(old_file_path, std::ios::out | std::ios::trunc);
      output_file << "11";
      output_file.close();
      status = DiskANNUtils::Rename(old_file_path, new_file_path);
      EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
      std::filesystem::remove(old_file_path);
      std::filesystem::remove(new_file_path);
    }
  }

  // dir path is link
  {
    std::string old_file_path = "./a";
    std::string new_file_path = "./b";
    std::filesystem::create_directory(old_file_path);
    std::string file_path = old_file_path + "/1.txt";
    std::ofstream output_file(file_path, std::ios::out | std::ios::trunc);
    output_file << "1";
    output_file.close();
    std::string link_path = "./link_a";
    std::filesystem::create_directory_symlink(old_file_path, link_path);
    status = DiskANNUtils::Rename(old_file_path, new_file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    std::filesystem::remove_all(new_file_path);
    std::filesystem::remove(link_path);
  }

  // file path is no permission
  {
    std::string old_file_path = "/var/dummy.txt";
    std::string new_file_path = "./dummy.txt";
    status = DiskANNUtils::Rename(old_file_path, new_file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_CROSS_DEVICE);
  }

  // file path is no permission
  {
    std::string old_file_path = "./1.txt";
    std::string new_file_path = "/var/2.txt";
    std::ofstream output_file(old_file_path, std::ios::out | std::ios::trunc);
    output_file << "11";
    output_file.close();

    status = DiskANNUtils::Rename(old_file_path, new_file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_CROSS_DEVICE);
  }

  // file path is file
  {
    std::string old_file_path = "./1.txt";
    std::string new_file_path = "./x/2.txt";
    std::ofstream output_file(old_file_path, std::ios::out | std::ios::trunc);
    output_file << "11";
    output_file.close();
    status = DiskANNUtils::Rename(old_file_path, new_file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    std::filesystem::remove(old_file_path);
    std::filesystem::remove(new_file_path);
  }

  // dir path is link
  {
    std::string old_file_path = "./a";
    std::string new_file_path = "./y/b";
    std::filesystem::create_directory(old_file_path);
    status = DiskANNUtils::Rename(old_file_path, new_file_path);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    std::filesystem::remove_all(new_file_path);
    std::filesystem::remove_all(old_file_path);
  }
}

TEST_F(DiskANNUtilsTest, DiskANNIndexPathPrefixExists) {
  butil::Status status;
  bool build_with_mem_index = false;

  // emtpy dir path
  {
    std::string dir_path;
    status = DiskANNUtils::DiskANNIndexPathPrefixExists(dir_path, build_with_mem_index,
                                                        pb::common::MetricType::METRIC_TYPE_L2);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
  }

  // dir path is not exist
  {
    std::string dir_path = "./test_dir";
    // std::filesystem::create_directory("test_dir");
    status = DiskANNUtils::DiskANNIndexPathPrefixExists(dir_path, build_with_mem_index,
                                                        pb::common::MetricType::METRIC_TYPE_L2);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    std::filesystem::remove(dir_path);
  }

  // dir path is file
  {
    std::string dir_path = "./1.txt";
    std::ofstream output_file(dir_path, std::ios::out | std::ios::trunc);
    output_file << "11";
    output_file.close();
    status = DiskANNUtils::DiskANNIndexPathPrefixExists(dir_path, build_with_mem_index,
                                                        pb::common::MetricType::METRIC_TYPE_L2);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_DIRECTORY);
    std::filesystem::remove(dir_path);
  }

  // dir path is exist
  {
    std::string dir_path = "./test_dir";
    std::filesystem::create_directory("test_dir");
    status = DiskANNUtils::DiskANNIndexPathPrefixExists(dir_path, build_with_mem_index,
                                                        pb::common::MetricType::METRIC_TYPE_L2);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    std::filesystem::remove(dir_path);
  }

  // dir path is exist
  {
    std::string dir_path = "./test_dir";
    std::filesystem::create_directory(dir_path);

    std::vector<std::string> common_files;
    common_files.push_back(dir_path + "/_disk.index");
    common_files.push_back(dir_path + "/_pq_compressed.bin");
    common_files.push_back(dir_path + "/_pq_pivots.bin");
    common_files.push_back(dir_path + "/_sample_data.bin");
    common_files.push_back(dir_path + "/_sample_ids.bin");

    for (const auto& file_name : common_files) {
      std::ofstream output_file(file_name, std::ios::out | std::ios::trunc);
      output_file << "11";
      output_file.close();
    }

    status = DiskANNUtils::DiskANNIndexPathPrefixExists(dir_path, build_with_mem_index,
                                                        pb::common::MetricType::METRIC_TYPE_L2);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EFILE_NOT_EXIST);
    std::filesystem::remove_all(dir_path);
  }

  // dir path is exist
  {
    std::string dir_path = "./test_dir";
    std::filesystem::create_directory(dir_path);

    std::vector<std::string> common_files;
    common_files.push_back(dir_path + "/_disk.index");
    common_files.push_back(dir_path + "/_pq_compressed.bin");
    common_files.push_back(dir_path + "/_pq_pivots.bin");
    common_files.push_back(dir_path + "/_sample_data.bin");
    common_files.push_back(dir_path + "/_sample_ids.bin");
    common_files.push_back(dir_path + "/_mem.index.data");

    for (const auto& file_name : common_files) {
      std::ofstream output_file(file_name, std::ios::out | std::ios::trunc);
      output_file << "11";
      output_file.close();
    }

    status = DiskANNUtils::DiskANNIndexPathPrefixExists(dir_path, build_with_mem_index,
                                                        pb::common::MetricType::METRIC_TYPE_L2);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_TRUE(build_with_mem_index);
    std::filesystem::remove_all(dir_path);
  }

  // dir path is exist
  {
    std::string dir_path = "./test_dir";
    std::filesystem::create_directory(dir_path);

    std::vector<std::string> common_files;
    common_files.push_back(dir_path + "/_disk.index");
    common_files.push_back(dir_path + "/_pq_compressed.bin");
    common_files.push_back(dir_path + "/_pq_pivots.bin");
    common_files.push_back(dir_path + "/_sample_data.bin");
    common_files.push_back(dir_path + "/_sample_ids.bin");
    // common_files.push_back(dir_path + "/_mem.index");

    common_files.push_back(dir_path + "/_disk.index_centroids.bin");
    common_files.push_back(dir_path + "/_disk.index_medoids.bin");

    for (const auto& file_name : common_files) {
      std::ofstream output_file(file_name, std::ios::out | std::ios::trunc);
      output_file << "11";
      output_file.close();
    }

    status = DiskANNUtils::DiskANNIndexPathPrefixExists(dir_path, build_with_mem_index,
                                                        pb::common::MetricType::METRIC_TYPE_L2);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_FALSE(build_with_mem_index);
    std::filesystem::remove_all(dir_path);
  }

  // dir path is exist
  {
    std::string dir_path = "./test_dir";
    std::filesystem::create_directory(dir_path);

    std::vector<std::string> common_files;
    common_files.push_back(dir_path + "/_disk.index");
    common_files.push_back(dir_path + "/_pq_compressed.bin");
    common_files.push_back(dir_path + "/_pq_pivots.bin");
    common_files.push_back(dir_path + "/_sample_data.bin");
    common_files.push_back(dir_path + "/_sample_ids.bin");
    common_files.push_back(dir_path + "/_mem.index.data");

    common_files.push_back(dir_path + "/_disk.index_centroids.bin");
    common_files.push_back(dir_path + "/_disk.index_medoids.bin");

    for (const auto& file_name : common_files) {
      std::ofstream output_file(file_name, std::ios::out | std::ios::trunc);
      output_file << "11";
      output_file.close();
    }

    status = DiskANNUtils::DiskANNIndexPathPrefixExists(dir_path, build_with_mem_index,
                                                        pb::common::MetricType::METRIC_TYPE_L2);
    EXPECT_EQ(status.error_code(), pb::error::Errno::EINTERNAL);
    std::filesystem::remove_all(dir_path);
  }
}

TEST_F(DiskANNUtilsTest, DiskANNCoreStateToString) {
  DiskANNCoreState state;
  std::atomic<DiskANNCoreState> state2;

  {
    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kUnknown");
    EXPECT_EQ(s2, "DiskANNCoreState::kUnknown");
  }

  {
    state = DiskANNCoreState::kImporting;
    state2 = DiskANNCoreState::kImporting;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kImporting");
    EXPECT_EQ(s2, "DiskANNCoreState::kImporting");
  }

  {
    state = DiskANNCoreState::kImported;
    state2 = DiskANNCoreState::kImported;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kImported");
    EXPECT_EQ(s2, "DiskANNCoreState::kImported");
  }

  {
    state = DiskANNCoreState::kInitialized;
    state2 = DiskANNCoreState::kInitialized;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kInitialized");
    EXPECT_EQ(s2, "DiskANNCoreState::kInitialized");
  }

  {
    state = DiskANNCoreState::kBuilding;
    state2 = DiskANNCoreState::kBuilding;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kBuilding");
    EXPECT_EQ(s2, "DiskANNCoreState::kBuilding");
  }

  {
    state = DiskANNCoreState::kBuilded;
    state2 = DiskANNCoreState::kBuilded;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kBuilded");
    EXPECT_EQ(s2, "DiskANNCoreState::kBuilded");
  }

  {
    state = DiskANNCoreState::kUpdatingPath;
    state2 = DiskANNCoreState::kUpdatingPath;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kUpdatingPath");
    EXPECT_EQ(s2, "DiskANNCoreState::kUpdatingPath");
  }

  {
    state = DiskANNCoreState::kUpdatedPath;
    state2 = DiskANNCoreState::kUpdatedPath;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);
  }

  {
    state = DiskANNCoreState::kLoading;
    state2 = DiskANNCoreState::kLoading;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kLoading");
    EXPECT_EQ(s2, "DiskANNCoreState::kLoading");
  }

  {
    state = DiskANNCoreState::kLoaded;
    state2 = DiskANNCoreState::kLoaded;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kLoaded");
    EXPECT_EQ(s2, "DiskANNCoreState::kLoaded");
  }

  {
    state = DiskANNCoreState::kSearing;
    state2 = DiskANNCoreState::kSearing;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kSearing");
    EXPECT_EQ(s2, "DiskANNCoreState::kSearing");
  }

  {
    state = DiskANNCoreState::kSearched;
    state2 = DiskANNCoreState::kSearched;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kSearched");
    EXPECT_EQ(s2, "DiskANNCoreState::kSearched");
  }

  {
    state = DiskANNCoreState::kReseting;
    state2 = DiskANNCoreState::kReseting;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kReseting");
    EXPECT_EQ(s2, "DiskANNCoreState::kReseting");
  }

  {
    state = DiskANNCoreState::kReset;
    state2 = DiskANNCoreState::kReset;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kReset");
    EXPECT_EQ(s2, "DiskANNCoreState::kReset");
  }

  {
    state = DiskANNCoreState::kDestroying;
    state2 = DiskANNCoreState::kDestroying;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kDestroying");
    EXPECT_EQ(s2, "DiskANNCoreState::kDestroying");
  }

  {
    state = DiskANNCoreState::kDestroyed;
    state2 = DiskANNCoreState::kDestroyed;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kDestroyed");
    EXPECT_EQ(s2, "DiskANNCoreState::kDestroyed");
  }

  {
    state = DiskANNCoreState::kIdle;
    state2 = DiskANNCoreState::kIdle;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kIdle");
    EXPECT_EQ(s2, "DiskANNCoreState::kIdle");
  }

  {
    state = DiskANNCoreState::kFailed;
    state2 = DiskANNCoreState::kFailed;

    std::string s = DiskANNUtils::DiskANNCoreStateToString(state);
    std::string s2 = DiskANNUtils::DiskANNCoreStateToString(state2);

    EXPECT_EQ(s, "DiskANNCoreState::kFailed");
    EXPECT_EQ(s2, "DiskANNCoreState::kFailed");
  }
}

TEST_F(DiskANNUtilsTest, DiskANNCoreStateToPb) {
  EXPECT_EQ(pb::common::DiskANNCoreState::UNKNOWN, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kUnknown));
  EXPECT_EQ(pb::common::DiskANNCoreState::IMPORTING, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kImporting));
  EXPECT_EQ(pb::common::DiskANNCoreState::IMPORTED, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kImported));
  EXPECT_EQ(pb::common::DiskANNCoreState::UNINITIALIZED,
            DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kUninitialized));
  EXPECT_EQ(pb::common::DiskANNCoreState::INITIALIZED,
            DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kInitialized));
  EXPECT_EQ(pb::common::DiskANNCoreState::BUILDING, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kBuilding));
  EXPECT_EQ(pb::common::DiskANNCoreState::BUILDED, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kBuilded));
  EXPECT_EQ(pb::common::DiskANNCoreState::UPDATINGPATH,
            DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kUpdatingPath));
  EXPECT_EQ(pb::common::DiskANNCoreState::UPDATEDPATH,
            DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kUpdatedPath));
  EXPECT_EQ(pb::common::DiskANNCoreState::LOADING, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kLoading));
  EXPECT_EQ(pb::common::DiskANNCoreState::LOADED, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kLoaded));
  EXPECT_EQ(pb::common::DiskANNCoreState::SEARING, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kSearing));
  EXPECT_EQ(pb::common::DiskANNCoreState::SEARCHED, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kSearched));
  EXPECT_EQ(pb::common::DiskANNCoreState::RESETING, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kReseting));
  EXPECT_EQ(pb::common::DiskANNCoreState::RESET, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kReset));
  EXPECT_EQ(pb::common::DiskANNCoreState::DESTROYING,
            DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kDestroying));
  EXPECT_EQ(pb::common::DiskANNCoreState::DESTROYED, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kDestroyed));
  EXPECT_EQ(pb::common::DiskANNCoreState::IDLE, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kIdle));
  EXPECT_EQ(pb::common::DiskANNCoreState::FAILED, DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState::kFailed));
}

TEST_F(DiskANNUtilsTest, DiskANNCoreStateFromPb) {
  EXPECT_EQ(DiskANNCoreState::kUnknown, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::UNKNOWN));
  EXPECT_EQ(DiskANNCoreState::kImporting,
            DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::IMPORTING));

  EXPECT_EQ(DiskANNCoreState::kImported, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::IMPORTED));

  EXPECT_EQ(DiskANNCoreState::kUninitialized,
            DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::UNINITIALIZED));

  EXPECT_EQ(DiskANNCoreState::kInitialized,
            DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::INITIALIZED));

  EXPECT_EQ(DiskANNCoreState::kBuilding, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::BUILDING));

  EXPECT_EQ(DiskANNCoreState::kBuilded, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::BUILDED));

  EXPECT_EQ(DiskANNCoreState::kUpdatingPath,
            DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::UPDATINGPATH));

  EXPECT_EQ(DiskANNCoreState::kUpdatedPath,
            DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::UPDATEDPATH));

  EXPECT_EQ(DiskANNCoreState::kLoading, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::LOADING));

  EXPECT_EQ(DiskANNCoreState::kLoaded, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::LOADED));

  EXPECT_EQ(DiskANNCoreState::kSearing, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::SEARING));

  EXPECT_EQ(DiskANNCoreState::kSearched, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::SEARCHED));

  EXPECT_EQ(DiskANNCoreState::kReseting, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::RESETING));

  EXPECT_EQ(DiskANNCoreState::kReset, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::RESET));

  EXPECT_EQ(DiskANNCoreState::kDestroying,
            DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::DESTROYING));

  EXPECT_EQ(DiskANNCoreState::kDestroyed,
            DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::DESTROYED));

  EXPECT_EQ(DiskANNCoreState::kIdle, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::IDLE));

  EXPECT_EQ(DiskANNCoreState::kFailed, DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState::FAILED));
}

}  // namespace dingodb
