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

#include "gtest/gtest.h"
#include "vector/vector_common.h"

namespace dingodb {
namespace sdk {

TEST(VectorIndexCacheKeyTest, EncodeDecodeVectorIndexCacheKey) {
  int64_t schema_id = 123;
  std::string index_name = "test_index";

  VectorIndexCacheKey key = GetVectorIndexCacheKey(schema_id, index_name);

  int64_t decoded_schema_id;
  std::string decoded_index_name;
  DecodeVectorIndexCacheKey(key, decoded_schema_id, decoded_index_name);

  EXPECT_EQ(decoded_schema_id, schema_id);
  EXPECT_EQ(decoded_index_name, index_name);
}

}  // namespace sdk
}  // namespace dingodb