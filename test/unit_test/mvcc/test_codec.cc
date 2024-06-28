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

class MvccCodecTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

 private:
};

TEST_F(MvccCodecTest, ValueFlagDelete) {
  {
    std::string expect(1, static_cast<char>(mvcc::ValueFlag::kDelete));
    ASSERT_EQ(expect, mvcc::Codec::ValueFlagDelete());
  }

  {
    std::string expect(1, static_cast<char>(mvcc::ValueFlag::kPut));
    ASSERT_NE(expect, mvcc::Codec::ValueFlagDelete());
  }
}

TEST_F(MvccCodecTest, EncodeBytes) {
  // empty
  {
    std::string user_key;
    std::string expect;
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\xf7');

    {
      std::string encode_key = mvcc::Codec::EncodeBytes(user_key);
      EXPECT_EQ(expect, encode_key);
    }

    {
      std::string encode_key;
      mvcc::Codec::EncodeBytes(user_key, encode_key);
      EXPECT_EQ(expect, encode_key);
    }

    {
      std::string encode_key;
      mvcc::Codec::EncodeBytes(std::string_view(user_key), encode_key);
      EXPECT_EQ(expect, encode_key);
    }
  }

  // length lt 8
  {
    std::string user_key = "h";
    std::string expect = "h";
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\xf8');

    {
      std::string encode_key = mvcc::Codec::EncodeBytes(user_key);
      EXPECT_EQ(expect, encode_key);
    }

    {
      std::string encode_key;
      mvcc::Codec::EncodeBytes(user_key, encode_key);
      EXPECT_EQ(expect, encode_key);
    }

    {
      std::string encode_key;
      mvcc::Codec::EncodeBytes(std::string_view(user_key), encode_key);
      EXPECT_EQ(expect, encode_key);
    }
  }

  // length eq 8
  {
    std::string user_key = "hello wo";
    std::string expect = "hello wo\xff";
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\xf7');

    {
      std::string encode_key = mvcc::Codec::EncodeBytes(user_key);
      EXPECT_EQ(expect, encode_key);
    }

    {
      std::string encode_key;
      mvcc::Codec::EncodeBytes(user_key, encode_key);
      EXPECT_EQ(expect, encode_key);
    }

    {
      std::string encode_key;
      mvcc::Codec::EncodeBytes(std::string_view(user_key), encode_key);
      EXPECT_EQ(expect, encode_key);
    }
  }

  // length gt 8
  {
    std::string user_key = "hello world 123456";
    std::string expect = std::string("hello wo\xff") + "rld 1234\xff" + "56";
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\x00');
    expect.push_back('\xf9');

    {
      std::string encode_key = mvcc::Codec::EncodeBytes(user_key);
      EXPECT_EQ(expect, encode_key);
    }

    {
      std::string encode_key;
      mvcc::Codec::EncodeBytes(user_key, encode_key);
      EXPECT_EQ(expect, encode_key);
    }

    {
      std::string encode_key;
      mvcc::Codec::EncodeBytes(std::string_view(user_key), encode_key);
      EXPECT_EQ(expect, encode_key);
    }
  }
}

TEST_F(MvccCodecTest, DecodeBytes) {
  // empty
  {
    std::string encode_key;
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\xf7');
    std::string output;
    std::string expect;

    ASSERT_TRUE(mvcc::Codec::DecodeBytes(encode_key, output));

    EXPECT_EQ(expect, output);
  }

  // length lt 8
  {
    std::string encode_key = "h";
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\xf8');
    std::string output;
    std::string expect = "h";

    ASSERT_TRUE(mvcc::Codec::DecodeBytes(encode_key, output));

    EXPECT_EQ(expect, output);
  }

  // length eq 8
  {
    std::string encode_key = "hello wo\xff";
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\xf7');
    std::string output;
    std::string expect = "hello wo";

    ASSERT_TRUE(mvcc::Codec::DecodeBytes(encode_key, output));

    EXPECT_EQ(expect, output);
  }
  // length gt 8
  {
    std::string encode_key = std::string("hello wo\xff") + "rld 1234\xff" + "56";
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\xf9');
    std::string output;
    std::string expect = "hello world 123456";

    ASSERT_TRUE(mvcc::Codec::DecodeBytes(encode_key, output));

    EXPECT_EQ(expect, output);
  }

  // length invalid
  {
    std::string encode_key = "h";
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\xf8');
    std::string output;

    ASSERT_FALSE(mvcc::Codec::DecodeBytes(encode_key, output));
  }

  // tail char is 0xff
  {
    std::string encode_key = "h";
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\xff');
    std::string output;

    ASSERT_FALSE(mvcc::Codec::DecodeBytes(encode_key, output));
  }

  // pading not 0x00
  {
    std::string encode_key = "h";
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x02');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\x00');
    encode_key.push_back('\xf8');
    std::string output;

    ASSERT_FALSE(mvcc::Codec::DecodeBytes(encode_key, output));
  }
}

TEST_F(MvccCodecTest, EncodeDecodeKey) {
  // encode and decode
  {
    int64_t expect_ts = 1;
    std::string expect_user_key = "hello world 123456";

    std::string encode_key = mvcc::Codec::EncodeKey(expect_user_key, expect_ts);

    std::string user_key;
    int64_t ts;
    ASSERT_TRUE(mvcc::Codec::DecodeKey(encode_key, user_key, ts));
    EXPECT_EQ(expect_ts, ts);
    EXPECT_EQ(expect_user_key, user_key);
  }

  // encode and decode
  {
    int64_t expect_ts = 123456789;
    std::string expect_user_key = "hello world 123456";

    std::string encode_key = mvcc::Codec::EncodeKey(expect_user_key, expect_ts);

    std::string user_key;
    int64_t ts;
    ASSERT_TRUE(mvcc::Codec::DecodeKey(encode_key, user_key, ts));
    EXPECT_EQ(expect_ts, ts);
    EXPECT_EQ(expect_user_key, user_key);
  }

  // diff sequence
  {
    std::string expect_user_key = "hello world 123456";

    std::string encode_key1 = mvcc::Codec::EncodeKey(expect_user_key, 0);
    std::string encode_key2 = mvcc::Codec::EncodeKey(expect_user_key, 1);
    EXPECT_LT(encode_key2, encode_key1);
  }

  // diff sequence
  {
    std::string expect_user_key = "hello world 123456";

    std::string encode_key1 = mvcc::Codec::EncodeKey(expect_user_key, 111);
    std::string encode_key2 = mvcc::Codec::EncodeKey(expect_user_key, 222);
    EXPECT_LT(encode_key2, encode_key1);
  }
}

TEST_F(MvccCodecTest, TruncateTsForKey) {
  std::string key = mvcc::Codec::EncodeKey(std::string("hello"), 100001);
  std::cout << "key: " << Helper::StringToHex(key) << std::endl;

  auto truc_key = mvcc::Codec::TruncateTsForKey(key);
  EXPECT_EQ(9, truc_key.size());

  int64_t ts = mvcc::Codec::TruncateKeyForTs(key);
  EXPECT_EQ(100001, ts);
}

TEST_F(MvccCodecTest, PackageValue) {
  {
    std::string value = "hello";
    std::string output;
    mvcc::Codec::PackageValue(mvcc::ValueFlag::kPut, value, output);

    mvcc::ValueFlag actual_flag;
    int64_t actual_ttl;
    auto actual_value = mvcc::Codec::UnPackageValue(output, actual_flag, actual_ttl);
    ASSERT_EQ(mvcc::ValueFlag::kPut, actual_flag);
    ASSERT_EQ(0, actual_ttl);
    ASSERT_EQ(value, actual_value);
  }

  {
    std::string value = "hello";
    std::string output;
    mvcc::Codec::PackageValue(mvcc::ValueFlag::kDelete, value, output);

    mvcc::ValueFlag actual_flag;
    int64_t actual_ttl;
    auto actual_value = mvcc::Codec::UnPackageValue(output, actual_flag, actual_ttl);
    ASSERT_EQ(mvcc::ValueFlag::kDelete, actual_flag);
    ASSERT_EQ(0, actual_ttl);
    ASSERT_EQ("", actual_value);
  }

  {
    std::string value = "hello";
    std::string output;
    int64_t ttl = dingodb::Helper::TimestampMs();
    mvcc::Codec::PackageValue(mvcc::ValueFlag::kPutTTL, ttl, value, output);

    mvcc::ValueFlag actual_flag;
    int64_t actual_ttl;
    auto actual_value = mvcc::Codec::UnPackageValue(output, actual_flag, actual_ttl);
    ASSERT_EQ(mvcc::ValueFlag::kPutTTL, actual_flag);
    ASSERT_EQ(ttl, actual_ttl);
    ASSERT_EQ(value, actual_value);
  }
}

}  // namespace dingodb