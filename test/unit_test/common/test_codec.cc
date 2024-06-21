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
#include "vector/codec.h"

namespace dingodb {

class CodecTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

 private:
};

TEST_F(CodecTest, EncodeVectorKey) {
  char prefix = 'c';
  int64_t partition_id = 0x1122334455667788;
  int64_t vector_id = 0x1199AABBCCDDEEFF;
  std::string scalar_key = "001122334455667788";
  std::string result;

  VectorCodec::EncodeVectorKey(prefix, partition_id, result);
  LOG(INFO) << "result : " << Helper::StringToHex(result);

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, result);
  LOG(INFO) << "result : " << Helper::StringToHex(result);

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, scalar_key, result);
  LOG(INFO) << "result : " << Helper::StringToHex(result);
}

TEST_F(CodecTest, DecodeVectorId) {
  char prefix = 'c';
  int64_t partition_id = 0x1122334455667788;
  int64_t vector_id = 0x1199AABBCCDDEEFF;
  std::string scalar_key = "001122334455667788";
  std::string result1;

  VectorCodec::EncodeVectorKey(prefix, partition_id, result1);
  LOG(INFO) << "result : " << Helper::StringToHex(result1);

  std::string result2;

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, result2);
  LOG(INFO) << "result : " << Helper::StringToHex(result2);

  std::string result3;

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, scalar_key, result3);
  LOG(INFO) << "result : " << Helper::StringToHex(result3);

  int64_t vector_id_1 = VectorCodec::DecodeVectorId(result1);
  EXPECT_EQ(0, vector_id_1);
  int64_t vector_id_2 = VectorCodec::DecodeVectorId(result2);
  EXPECT_EQ(vector_id, vector_id_2);
  int64_t vector_id_3 = VectorCodec::DecodeVectorId(result3);
  EXPECT_EQ(vector_id, vector_id_3);
}

TEST_F(CodecTest, DecodePartitionId) {
  char prefix = 'c';
  int64_t partition_id = 0x1122334455667788;
  int64_t vector_id = 0x1199AABBCCDDEEFF;
  std::string scalar_key = "001122334455667788";
  std::string result1;

  VectorCodec::EncodeVectorKey(prefix, partition_id, result1);
  LOG(INFO) << "result : " << Helper::StringToHex(result1);

  std::string result2;

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, result2);
  LOG(INFO) << "result : " << Helper::StringToHex(result2);

  std::string result3;

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, scalar_key, result3);
  LOG(INFO) << "result : " << Helper::StringToHex(result3);

  int64_t partition_id_1 = VectorCodec::DecodePartitionId(result1);
  EXPECT_EQ(partition_id, partition_id_1);
  int64_t partition_id_2 = VectorCodec::DecodePartitionId(result2);
  EXPECT_EQ(partition_id, partition_id_2);
  int64_t partition_id_3 = VectorCodec::DecodePartitionId(result3);
  EXPECT_EQ(partition_id, partition_id_3);
}

TEST_F(CodecTest, DecodeScalarKey) {
  char prefix = 'c';
  int64_t partition_id = 0x1122334455667788;
  int64_t vector_id = 0x1199AABBCCDDEEFF;
  std::string scalar_key = "001122334455667788";
  std::string result1;

  VectorCodec::EncodeVectorKey(prefix, partition_id, result1);
  LOG(INFO) << "result : " << Helper::StringToHex(result1);

  std::string result2;

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, result2);
  LOG(INFO) << "result : " << Helper::StringToHex(result2);

  std::string result3;

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, scalar_key, result3);
  LOG(INFO) << "result : " << Helper::StringToHex(result3);

  EXPECT_DEATH(VectorCodec::DecodeScalarKey(result1), "");

  EXPECT_DEATH(VectorCodec::DecodeScalarKey(result2), "");

  std::string scalar_key_3 = VectorCodec::DecodeScalarKey(result3);
  EXPECT_EQ(scalar_key, scalar_key_3);
}

TEST_F(CodecTest, IsValidKey) {
  char prefix = 'c';
  int64_t partition_id = 0x1122334455667788;
  int64_t vector_id = 0x1199AABBCCDDEEFF;
  std::string scalar_key = "001122334455667788";
  std::string result1;

  VectorCodec::EncodeVectorKey(prefix, partition_id, result1);
  LOG(INFO) << "result : " << Helper::StringToHex(result1);

  std::string result2;

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, result2);
  LOG(INFO) << "result : " << Helper::StringToHex(result2);

  std::string result3;

  VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, scalar_key, result3);
  LOG(INFO) << "result : " << Helper::StringToHex(result3);

  EXPECT_TRUE(VectorCodec::IsValidKey(result1));
  EXPECT_TRUE(VectorCodec::IsValidKey(result2));
  EXPECT_TRUE(VectorCodec::IsValidKey(result3));
}

}  // namespace dingodb
