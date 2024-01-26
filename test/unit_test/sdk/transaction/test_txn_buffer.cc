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

#include <memory>
#include <set>

#include "gtest/gtest.h"
#include "sdk/transaction/txn_buffer.h"

namespace dingodb {
namespace sdk {

class TxnBufferTest : public testing::Test {
 protected:
  void SetUp() override { txn_buffer.reset(new TxnBuffer()); }

  std::shared_ptr<TxnBuffer> txn_buffer;
};

TEST_F(TxnBufferTest, TestSingleOp) {
  TxnMutation mutation;
  Status tmp;
  {
    tmp = txn_buffer->Get("a", mutation);
    EXPECT_TRUE(tmp.IsNotFound());
    EXPECT_TRUE(txn_buffer->IsEmpty());
  }

  {
    // put
    tmp = txn_buffer->Put("a", "ra");
    EXPECT_TRUE(tmp.ok());
    EXPECT_FALSE(txn_buffer->IsEmpty());

    tmp = txn_buffer->Get("a", mutation);
    EXPECT_TRUE(tmp.ok());
    EXPECT_EQ(mutation.type, kPut);
    EXPECT_EQ(mutation.value, "ra");

    std::string pk = txn_buffer->GetPrimaryKey();
    EXPECT_EQ(pk, "a");
  }

  {
    // put if absent
    tmp = txn_buffer->PutIfAbsent("a", "ra");
    EXPECT_TRUE(tmp.ok());
    tmp = txn_buffer->Get("a", mutation);
    EXPECT_TRUE(tmp.ok());
    EXPECT_EQ(mutation.type, kPut);
    EXPECT_EQ(mutation.value, "ra");
  }

  {
    // delete
    tmp = txn_buffer->Delete("a");
    EXPECT_TRUE(tmp.ok());
    tmp = txn_buffer->Get("a", mutation);
    EXPECT_TRUE(tmp.ok());
    EXPECT_EQ(mutation.type, kDelete);
    EXPECT_TRUE(mutation.value.empty());
    EXPECT_FALSE(txn_buffer->IsEmpty());

    std::string pk = txn_buffer->GetPrimaryKey();
    EXPECT_EQ(pk, "a");
  }

  {
    // put if absent
    tmp = txn_buffer->PutIfAbsent("a", "ra");
    EXPECT_TRUE(tmp.ok());
    tmp = txn_buffer->Get("a", mutation);
    EXPECT_TRUE(tmp.ok());
    EXPECT_EQ(mutation.type, kPutIfAbsent);
    EXPECT_EQ(mutation.value, "ra");
    std::string pk = txn_buffer->GetPrimaryKey();
    EXPECT_EQ(pk, "a");
  }
}

TEST_F(TxnBufferTest, Range) {
  Status tmp;
  // put
  tmp = txn_buffer->Put("a", "ra");
  EXPECT_TRUE(tmp.ok());

  // put if absent
  tmp = txn_buffer->PutIfAbsent("b", "rb");
  EXPECT_TRUE(tmp.ok());

  // delete
  tmp = txn_buffer->Delete("c");
  EXPECT_TRUE(tmp.ok());

  std::vector<TxnMutation> mutations;
  tmp = txn_buffer->Range("a", "c", mutations);
  EXPECT_TRUE(tmp.ok());

  EXPECT_EQ(mutations.size(), 2);

  std::set<std::string> to_check = {"a", "b", "c"};
  for(const auto& mutation : mutations) {
    to_check.erase(mutation.key);
  }
  EXPECT_EQ(to_check.size(), 1);
  EXPECT_TRUE(to_check.find("c") != to_check.cend());
}

}  // namespace sdk

}  // namespace dingodb