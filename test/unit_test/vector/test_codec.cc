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
#include <string>

#include "common/helper.h"
#include "mvcc/codec.h"

namespace dingodb {

const char kPrefix = 'r';

class VectorCodecTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

 private:
};

TEST_F(VectorCodecTest, packageAndUnPackage) {
  int64_t partition_id = 12345;

  {
    std::string plain_key = VectorCodec::PackageVectorKey(kPrefix, partition_id);
    ASSERT_EQ(9, plain_key.size());
    ASSERT_EQ(partition_id, VectorCodec::UnPackagePartitionId(plain_key));
  }

  {
    int64_t document_id = 789654;
    std::string plain_key = VectorCodec::PackageVectorKey(kPrefix, partition_id, document_id);
    ASSERT_EQ(17, plain_key.size());
    ASSERT_EQ(partition_id, VectorCodec::UnPackagePartitionId(plain_key));
    ASSERT_EQ(document_id, VectorCodec::UnPackageVectorId(plain_key));
  }

  {
    int64_t document_id = 789654;
    const std::string scalar_key = "hello world";
    std::string plain_key = VectorCodec::PackageVectorKey(kPrefix, partition_id, document_id, scalar_key);
    ASSERT_EQ(28, plain_key.size());
    ASSERT_EQ(partition_id, VectorCodec::UnPackagePartitionId(plain_key));
    ASSERT_EQ(document_id, VectorCodec::UnPackageVectorId(plain_key));
    ASSERT_EQ(scalar_key, VectorCodec::UnPackageScalarKey(plain_key));
  }
}

TEST_F(VectorCodecTest, encodeAndDecode) {
  int64_t partition_id = 12345;

  {
    std::string encode_key = VectorCodec::EncodeVectorKey(kPrefix, partition_id);

    ASSERT_EQ(partition_id, VectorCodec::DecodePartitionIdFromEncodeKey(encode_key));
  }

  {
    int64_t vector_id = 9876543;
    std::string encode_key = VectorCodec::EncodeVectorKey(kPrefix, partition_id, vector_id);

    ASSERT_EQ(partition_id, VectorCodec::DecodePartitionIdFromEncodeKey(encode_key));
    ASSERT_EQ(vector_id, VectorCodec::DecodeVectorIdFromEncodeKey(encode_key));
  }

  {
    int64_t vector_id = 9876543;
    std::string encode_key = VectorCodec::EncodeVectorKey(kPrefix, partition_id, vector_id);

    int64_t actual_partition_id;
    int64_t actual_vector_id;
    VectorCodec::DecodeFromEncodeKey(encode_key, actual_partition_id, actual_vector_id);
    ASSERT_EQ(partition_id, actual_partition_id);
    ASSERT_EQ(vector_id, actual_vector_id);
  }

  {
    int64_t vector_id = 9876543;
    std::string scalar_key = "hello";
    std::string encode_key = VectorCodec::EncodeVectorKey(kPrefix, partition_id, vector_id, scalar_key);

    int64_t actual_partition_id;
    int64_t actual_vector_id;
    std::string expect_scalar_key;
    VectorCodec::DecodeFromEncodeKey(encode_key, actual_partition_id, actual_vector_id, expect_scalar_key);
    ASSERT_EQ(partition_id, actual_partition_id);
    ASSERT_EQ(vector_id, actual_vector_id);
    ASSERT_EQ(scalar_key, expect_scalar_key);
  }

  {
    int64_t vector_id = 9876543;
    std::string scalar_key = "hello";
    std::string encode_key = VectorCodec::EncodeVectorKey(kPrefix, partition_id, vector_id, scalar_key);

    ASSERT_EQ(partition_id, VectorCodec::DecodePartitionIdFromEncodeKey(encode_key));
    ASSERT_EQ(vector_id, VectorCodec::DecodeVectorIdFromEncodeKey(encode_key));
    ASSERT_EQ(scalar_key, VectorCodec::DecodeScalarKeyFromEncodeKey(encode_key));
  }
}

}  // namespace dingodb