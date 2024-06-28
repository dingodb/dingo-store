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

TEST_F(VectorCodecTest, encodeAndDecode) {
  int64_t partition_id = 12345;

  {
    std::string encode_key;
    VectorCodec::EncodeVectorKey(kPrefix, partition_id, encode_key);

    ASSERT_EQ(partition_id, VectorCodec::DecodePartitionIdFromEncodeKey(encode_key));
  }

  {
    std::string encode_key;
    int64_t vector_id = 9876543;
    VectorCodec::EncodeVectorKey(kPrefix, partition_id, vector_id, encode_key);

    ASSERT_EQ(partition_id, VectorCodec::DecodePartitionIdFromEncodeKey(encode_key));
    ASSERT_EQ(vector_id, VectorCodec::DecodeVectorIdFromEncodeKey(encode_key));
  }

  {
    std::string encode_key;
    int64_t vector_id = 9876543;
    VectorCodec::EncodeVectorKey(kPrefix, partition_id, vector_id, encode_key);

    int64_t actual_partition_id;
    int64_t actual_vector_id;
    VectorCodec::DecodeFromEncodeKey(encode_key, actual_partition_id, actual_vector_id);
    ASSERT_EQ(partition_id, actual_partition_id);
    ASSERT_EQ(vector_id, actual_vector_id);
  }

  {
    std::string encode_key;
    int64_t vector_id = 9876543;
    std::string scalar_key = "hello";
    VectorCodec::EncodeVectorKey(kPrefix, partition_id, vector_id, scalar_key, encode_key);

    int64_t actual_partition_id;
    int64_t actual_vector_id;
    std::string expect_scalar_key;
    VectorCodec::DecodeFromEncodeKey(encode_key, actual_partition_id, actual_vector_id, expect_scalar_key);
    ASSERT_EQ(partition_id, actual_partition_id);
    ASSERT_EQ(vector_id, actual_vector_id);
    ASSERT_EQ(scalar_key, expect_scalar_key);
  }

  {
    std::string encode_key;
    int64_t vector_id = 9876543;
    std::string scalar_key = "hello";
    VectorCodec::EncodeVectorKey(kPrefix, partition_id, vector_id, scalar_key, encode_key);

    ASSERT_EQ(partition_id, VectorCodec::DecodePartitionIdFromEncodeKey(encode_key));
    ASSERT_EQ(vector_id, VectorCodec::DecodeVectorIdFromEncodeKey(encode_key));
    ASSERT_EQ(scalar_key, VectorCodec::DecodeScalarKeyFromEncodeKey(encode_key));
  }
}

}  // namespace dingodb