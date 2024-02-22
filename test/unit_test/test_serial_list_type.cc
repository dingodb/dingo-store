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

#include <byteswap.h>
#include <gtest/gtest.h>
#include <proto/meta.pb.h>
#include <serial/record_decoder.h>
#include <serial/record_encoder.h>
#include <serial/utils.h>

#include <algorithm>
#include <bitset>
#include <memory>
#include <optional>
#include <string>

#include "glog/logging.h"

// #include "serial/keyvalue_codec.h"
#include "serial/schema/base_schema.h"

using namespace dingodb;
using namespace std;

class DingoSerialListTypeTest : public testing::Test {
 private:
  std::shared_ptr<vector<std::shared_ptr<BaseSchema>>> schemas_;
  vector<any>* record_;

 public:
  void InitVector() {
    schemas_ = std::make_shared<vector<std::shared_ptr<BaseSchema>>>(25);

    auto id = std::make_shared<DingoSchema<optional<int32_t>>>();
    id->SetIndex(0);
    id->SetAllowNull(false);
    id->SetIsKey(true);
    schemas_->at(0) = id;

    auto name = std::make_shared<DingoSchema<optional<shared_ptr<string>>>>();
    name->SetIndex(1);
    name->SetAllowNull(false);
    name->SetIsKey(true);
    schemas_->at(1) = name;

    auto gender = std::make_shared<DingoSchema<optional<shared_ptr<string>>>>();
    gender->SetIndex(2);
    gender->SetAllowNull(false);
    gender->SetIsKey(true);
    schemas_->at(2) = gender;

    auto score = std::make_shared<DingoSchema<optional<int64_t>>>();
    score->SetIndex(3);
    score->SetAllowNull(false);
    score->SetIsKey(true);
    schemas_->at(3) = score;

    auto addr = std::make_shared<DingoSchema<optional<shared_ptr<string>>>>();
    addr->SetIndex(4);
    addr->SetAllowNull(true);
    addr->SetIsKey(false);
    schemas_->at(4) = addr;

    auto exist = std::make_shared<DingoSchema<optional<bool>>>();
    exist->SetIndex(5);
    exist->SetAllowNull(false);
    exist->SetIsKey(false);
    schemas_->at(5) = exist;

    auto pic = std::make_shared<DingoSchema<optional<shared_ptr<string>>>>();
    pic->SetIndex(6);
    pic->SetAllowNull(true);
    pic->SetIsKey(false);
    schemas_->at(6) = pic;

    auto test_null = std::make_shared<DingoSchema<optional<int32_t>>>();
    test_null->SetIndex(7);
    test_null->SetAllowNull(true);
    test_null->SetIsKey(false);
    schemas_->at(7) = test_null;

    auto age = std::make_shared<DingoSchema<optional<int32_t>>>();
    age->SetIndex(8);
    age->SetAllowNull(false);
    age->SetIsKey(false);
    schemas_->at(8) = age;

    auto prev = std::make_shared<DingoSchema<optional<int64_t>>>();
    prev->SetIndex(9);
    prev->SetAllowNull(false);
    prev->SetIsKey(false);
    schemas_->at(9) = prev;

    auto salary = std::make_shared<DingoSchema<optional<double>>>();
    salary->SetIndex(10);
    salary->SetAllowNull(true);
    salary->SetIsKey(false);
    schemas_->at(10) = salary;

    auto boollist_null = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<bool>>>>>();
    boollist_null->SetIndex(11);
    boollist_null->SetAllowNull(true);
    boollist_null->SetIsKey(false);
    schemas_->at(11) = boollist_null;

    auto boollist = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<bool>>>>>();
    boollist->SetIndex(12);
    boollist->SetAllowNull(false);
    boollist->SetIsKey(false);
    schemas_->at(12) = boollist;

    auto stringlist_null = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<std::string>>>>>();
    stringlist_null->SetIndex(13);
    stringlist_null->SetAllowNull(true);
    stringlist_null->SetIsKey(false);
    schemas_->at(13) = stringlist_null;

    auto stringlist = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<std::string>>>>>();
    stringlist->SetIndex(14);
    stringlist->SetAllowNull(false);
    stringlist->SetIsKey(false);
    schemas_->at(14) = stringlist;

    auto doublelist_null = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<double>>>>>();
    doublelist_null->SetIndex(15);
    doublelist_null->SetAllowNull(true);
    doublelist_null->SetIsKey(false);
    schemas_->at(15) = doublelist_null;

    auto doublelist = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<double>>>>>();
    doublelist->SetIndex(16);
    doublelist->SetAllowNull(false);
    doublelist->SetIsKey(false);
    schemas_->at(16) = doublelist;

    auto floatlist_null = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<float>>>>>();
    floatlist_null->SetIndex(17);
    floatlist_null->SetAllowNull(true);
    floatlist_null->SetIsKey(false);
    schemas_->at(17) = floatlist_null;

    auto floatlist = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<float>>>>>();
    floatlist->SetIndex(18);
    floatlist->SetAllowNull(false);
    floatlist->SetIsKey(false);
    schemas_->at(18) = floatlist;

    auto integerlist_null = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<int32_t>>>>>();
    integerlist_null->SetIndex(19);
    integerlist_null->SetAllowNull(true);
    integerlist_null->SetIsKey(false);
    schemas_->at(19) = integerlist_null;

    auto integerlist = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<int32_t>>>>>();
    integerlist->SetIndex(20);
    integerlist->SetAllowNull(false);
    integerlist->SetIsKey(false);
    schemas_->at(20) = integerlist;

    auto longlist_null = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<int64_t>>>>>();
    longlist_null->SetIndex(21);
    longlist_null->SetAllowNull(true);
    longlist_null->SetIsKey(false);
    schemas_->at(21) = longlist_null;

    auto longlist = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<int64_t>>>>>();
    longlist->SetIndex(22);
    longlist->SetAllowNull(false);
    longlist->SetIsKey(false);
    schemas_->at(22) = longlist;

    auto stringlist_null1 = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<std::string>>>>>();
    stringlist_null1->SetIndex(23);
    stringlist_null1->SetAllowNull(true);
    stringlist_null1->SetIsKey(false);
    schemas_->at(23) = stringlist_null1;

    auto stringlist1 = std::make_shared<DingoSchema<optional<std::shared_ptr<std::vector<std::string>>>>>();
    stringlist1->SetIndex(24);
    stringlist1->SetAllowNull(false);
    stringlist1->SetIsKey(false);
    schemas_->at(24) = stringlist1;
  }

  void DeleteSchemas() {
    schemas_->clear();
    schemas_->shrink_to_fit();
  }

  void InitRecord() {
    record_ = new vector<any>(25);
    optional<int32_t> id = 0;
    std::shared_ptr<std::string> name = std::make_shared<std::string>("tn");
    std::shared_ptr<std::string> gender = std::make_shared<std::string>("f");
    optional<int64_t> score = 214748364700L;
    std::shared_ptr<std::string> addr = std::make_shared<std::string>(
        "test address test 中文 表情😊🏷️👌 test "
        "测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试"
        "测"
        "试伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫");
    optional<bool> exist = false;
    optional<shared_ptr<string>> pic = nullopt;
    optional<int32_t> test_null = nullopt;
    optional<int32_t> age = -20;
    optional<int64_t> prev = -214748364700L;
    optional<double> salary = 873485.4234;

    optional<std::shared_ptr<std::vector<bool>>> bool1 = std::make_shared<std::vector<bool>>(std::vector<bool>{});
    optional<std::shared_ptr<std::vector<bool>>> bool2 =
        std::make_shared<std::vector<bool>>(std::vector<bool>{true, false, false, false, true});

    optional<std::shared_ptr<std::vector<std::string>>> string1 =
        std::make_shared<std::vector<std::string>>(std::vector<std::string>{});
    optional<std::shared_ptr<std::vector<std::string>>> string2 =
        std::make_shared<std::vector<std::string>>(std::vector<std::string>{"qwe", "中文"});

    optional<std::shared_ptr<std::vector<double>>> double1 =
        std::make_shared<std::vector<double>>(std::vector<double>{});
    optional<std::shared_ptr<std::vector<double>>> double2 = std::make_shared<std::vector<double>>(
        std::vector<double>{3323232333.21221, 23232.2111111, 222334455566.23, 2222, 67889.246});

    optional<std::shared_ptr<std::vector<float>>> float1 = std::make_shared<std::vector<float>>(std::vector<float>{});
    optional<std::shared_ptr<std::vector<float>>> float2 =
        std::make_shared<std::vector<float>>(std::vector<float>{33232.21221, 23232.2111111, 2222, 67889.246});

    optional<std::shared_ptr<std::vector<int32_t>>> int1 =
        std::make_shared<std::vector<int32_t>>(std::vector<int32_t>{});
    optional<std::shared_ptr<std::vector<int32_t>>> int2 =
        std::make_shared<std::vector<int32_t>>(std::vector<int32_t>{33232, 2111111, 2222, 246});

    optional<std::shared_ptr<std::vector<int64_t>>> long1 =
        std::make_shared<std::vector<int64_t>>(std::vector<int64_t>{});
    optional<std::shared_ptr<std::vector<int64_t>>> long2 =
        std::make_shared<std::vector<int64_t>>(std::vector<int64_t>{33232998776555l, 2111111l, 2222l, 2469999883732l});

    optional<std::shared_ptr<std::vector<std::string>>> string3 = std::make_shared<
        std::vector<std::string>>(std::vector<std::string>{
        "测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试",
        "试伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫"});
    optional<std::shared_ptr<std::vector<std::string>>> string4 = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"test address test 中文 表情😊🏷️👌 test ", "中文"});

    record_->at(0) = id;
    record_->at(1) = optional<shared_ptr<string>>{name};
    record_->at(2) = optional<shared_ptr<string>>{gender};
    record_->at(3) = score;
    record_->at(4) = optional<shared_ptr<string>>{addr};
    record_->at(5) = exist;
    record_->at(6) = pic;
    record_->at(7) = test_null;
    record_->at(8) = age;
    record_->at(9) = prev;
    record_->at(10) = salary;
    record_->at(11) = bool1;
    record_->at(12) = bool2;
    record_->at(13) = string1;
    record_->at(14) = string2;
    record_->at(15) = double1;
    record_->at(16) = double2;
    record_->at(17) = float1;
    record_->at(18) = float2;
    record_->at(19) = int1;
    record_->at(20) = int2;
    record_->at(21) = long1;
    record_->at(22) = long2;
    record_->at(23) = string3;
    record_->at(24) = string4;
  }
  void DeleteRecords() {
    optional<shared_ptr<string>> name = any_cast<optional<shared_ptr<string>>>(record_->at(1));
    if (name.has_value()) {
    }
    optional<shared_ptr<string>> gender = any_cast<optional<shared_ptr<string>>>(record_->at(2));
    if (gender.has_value()) {
    }
    optional<shared_ptr<string>> addr = any_cast<optional<shared_ptr<string>>>(record_->at(4));
    if (addr.has_value()) {
    }
    record_->clear();
    record_->shrink_to_fit();
  }
  std::shared_ptr<vector<std::shared_ptr<BaseSchema>>> GetSchemas() { return schemas_; }
  vector<any>* GetRecord() { return record_; }

 protected:
  bool le = IsLE();
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DingoSerialListTypeTest, boolListSchema) {
  DingoSchema<optional<std::shared_ptr<::vector<bool>>>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(false);
  std::shared_ptr<::vector<bool>> data1 = std::make_shared<std::vector<bool>>(std::vector<bool>{false, true});
  Buf* bf1 = new Buf(2 + 4, this->le);
  b1.EncodeValue(bf1, data1);
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1, this->le);
  delete bs1;
  optional<std::shared_ptr<::vector<bool>>> data2 = b1.DecodeValue(bf2);
  delete bf1;
  delete bf2;
  if (data2.has_value()) {
    const auto& vector2 = *data2.value();
    EXPECT_EQ(data1->size(), vector2.size());
    for (size_t j = 0; j < data1->size(); ++j) {
      LOG(INFO) << "(*data1)[j]:" << (*data1)[j] << ",vector2[j]:" << vector2[j];
      EXPECT_EQ((*data1)[j], vector2[j]);
    }
  } else {
    EXPECT_TRUE(0);
  }

  DingoSchema<optional<bool>> b2;
  b2.SetIndex(0);
  b2.SetAllowNull(true);
  b2.SetIsKey(false);
  optional<bool> data3 = true;
  Buf* bf3 = new Buf(1, this->le);
  b2.EncodeValue(bf3, data3);
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2, this->le);
  delete bs2;
  optional<bool> data4 = b2.DecodeValue(bf4);
  delete bf3;
  delete bf4;
  if (data4.has_value()) {
    EXPECT_EQ(data3, data4.value());
  } else {
    EXPECT_TRUE(0);
  }

  optional<bool> data5 = nullopt;
  Buf* bf5 = new Buf(1, this->le);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3, this->le);
  delete bs3;
  optional<bool> data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value());

  DingoSchema<optional<bool>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100, this->le);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4, this->le);
  delete bs4;
  EXPECT_FALSE(b3.DecodeKey(bf8).has_value());
  delete bf7;
  delete bf8;
}

TEST_F(DingoSerialListTypeTest, integerSchema) {
  DingoSchema<optional<int32_t>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  optional<int32_t> data1 = 1543234;
  Buf* bf1 = new Buf(1, this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1, this->le);
  delete bs1;
  optional<int32_t> data2 = b1.DecodeKey(bf2);
  delete bf1;
  delete bf2;
  if (data2.has_value()) {
    EXPECT_EQ(data1, data2.value());
  } else {
    EXPECT_TRUE(0);
  }

  DingoSchema<optional<int32_t>> b2;
  b2.SetIndex(0);
  b2.SetAllowNull(true);
  b2.SetIsKey(false);
  optional<int32_t> data3 = 532142;
  Buf* bf3 = new Buf(1, this->le);
  b2.EncodeValue(bf3, data3);
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2, this->le);
  delete bs2;
  optional<int32_t> data4 = b2.DecodeValue(bf4);
  delete bf3;
  delete bf4;
  if (data4.has_value()) {
    EXPECT_EQ(data3, data4.value());
  } else {
    EXPECT_TRUE(0);
  }

  optional<int32_t> data5 = nullopt;
  Buf* bf5 = new Buf(1, this->le);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3, this->le);
  delete bs3;
  optional<int32_t> data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value());

  DingoSchema<optional<int32_t>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100, this->le);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4, this->le);
  delete bs4;
  EXPECT_FALSE(b3.DecodeKey(bf8).has_value());
  delete bf7;
  delete bf8;
}

TEST_F(DingoSerialListTypeTest, integerSchemaLeBe) {
  uint32_t data = 1543234;
  // bitset<32> key_data("10000000000101111000110001000010");
  bitset<8> key_data_0("10000000");
  bitset<8> key_data_1("00010111");
  bitset<8> key_data_2("10001100");
  bitset<8> key_data_3("01000010");
  // bitset<32> value_data("00000000000101111000110001000010");
  bitset<8> value_data_0("00000000");
  bitset<8> value_data_1("00010111");
  bitset<8> value_data_2("10001100");
  bitset<8> value_data_3("01000010");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<int32_t>> b1;
  b1.SetIndex(0);
  optional<int32_t> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (this->le) {
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  Buf* bf11 = new Buf(bs1, this->le);
  optional<int32_t> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  Buf* bf21 = new Buf(bs2, this->le);
  optional<int32_t> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, integerSchemaFakeLeBe) {
  uint32_t data = bswap_32(1543234);
  // bitset<32> key_data("10000000000101111000110001000010");
  bitset<8> key_data_0("10000000");
  bitset<8> key_data_1("00010111");
  bitset<8> key_data_2("10001100");
  bitset<8> key_data_3("01000010");
  // bitset<32> value_data("00000000000101111000110001000010");
  bitset<8> value_data_0("00000000");
  bitset<8> value_data_1("00010111");
  bitset<8> value_data_2("10001100");
  bitset<8> value_data_3("01000010");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<int32_t>> b1;
  b1.SetIndex(0);
  optional<int32_t> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (!this->le) {
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, !this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  Buf* bf11 = new Buf(bs1, !this->le);
  optional<int32_t> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, !this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  Buf* bf21 = new Buf(bs2, !this->le);
  optional<int32_t> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, floatSchemaLeBe) {
  float data = -43225.23;
  // bitset<32> key_data("00111000110101110010011011000100");
  bitset<8> key_data_0("00111000");
  bitset<8> key_data_1("11010111");
  bitset<8> key_data_2("00100110");
  bitset<8> key_data_3("11000100");
  // bitset<32> value_data("11000111001010001101100100111011");
  bitset<8> value_data_0("11000111");
  bitset<8> value_data_1("00101000");
  bitset<8> value_data_2("11011001");
  bitset<8> value_data_3("00111011");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<float>> b1;
  b1.SetIndex(0);
  optional<float> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (this->le) {
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  Buf* bf11 = new Buf(bs1, this->le);
  optional<float> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  Buf* bf21 = new Buf(bs2, this->le);
  optional<float> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, floatSchemaFakeLeBe) {
  float ori_data = -43225.53;
  uint32_t ori_data_bits;
  memcpy(&ori_data_bits, &ori_data, 4);
  uint32_t data_bits = bswap_32(ori_data_bits);
  float data;
  memcpy(&data, &data_bits, 4);
  // bitset<32> key_data("00111000110101110010011011000100");
  bitset<8> key_data_0("00111000");
  bitset<8> key_data_1("11010111");
  bitset<8> key_data_2("00100110");
  bitset<8> key_data_3("01110111");
  // bitset<32> value_data("11000111001010001101100110001000");
  bitset<8> value_data_0("11000111");
  bitset<8> value_data_1("00101000");
  bitset<8> value_data_2("11011001");
  bitset<8> value_data_3("10001000");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<float>> b1;
  b1.SetIndex(0);
  optional<float> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (!this->le) {
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, !this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  Buf* bf11 = new Buf(bs1, !this->le);
  optional<float> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, !this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  Buf* bf21 = new Buf(bs2, !this->le);
  optional<float> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, longSchemaLeBe) {
  uint64_t data = 8237583920453957801;
  // bitset<64> key_data("1111001001010001110001101110111001011010001000001011100010101001");
  bitset<8> key_data_0("11110010");
  bitset<8> key_data_1("01010001");
  bitset<8> key_data_2("11000110");
  bitset<8> key_data_3("11101110");
  bitset<8> key_data_4("01011010");
  bitset<8> key_data_5("00100000");
  bitset<8> key_data_6("10111000");
  bitset<8> key_data_7("10101001");
  // bitset<64> value_data("0111001001010001110001101110111001011010001000001011100010101001");
  bitset<8> value_data_0("01110010");
  bitset<8> value_data_1("01010001");
  bitset<8> value_data_2("11000110");
  bitset<8> value_data_3("11101110");
  bitset<8> value_data_4("01011010");
  bitset<8> value_data_5("00100000");
  bitset<8> value_data_6("10111000");
  bitset<8> value_data_7("10101001");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<int64_t>> b1;
  b1.SetIndex(0);
  optional<int64_t> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (this->le) {
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  bitset<8> bs15(bs1->at(5));
  EXPECT_EQ(bs15, key_data_4);
  bitset<8> bs16(bs1->at(6));
  EXPECT_EQ(bs16, key_data_5);
  bitset<8> bs17(bs1->at(7));
  EXPECT_EQ(bs17, key_data_6);
  bitset<8> bs18(bs1->at(8));
  EXPECT_EQ(bs18, key_data_7);
  Buf* bf11 = new Buf(bs1, this->le);
  optional<int64_t> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  bitset<8> bs25(bs2->at(5));
  EXPECT_EQ(bs25, value_data_4);
  bitset<8> bs26(bs2->at(6));
  EXPECT_EQ(bs26, value_data_5);
  bitset<8> bs27(bs2->at(7));
  EXPECT_EQ(bs27, value_data_6);
  bitset<8> bs28(bs2->at(8));
  EXPECT_EQ(bs28, value_data_7);
  Buf* bf21 = new Buf(bs2, this->le);
  optional<int64_t> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, longSchemaFakeLeBe) {
  uint64_t data = bswap_64(8237583920453957801);
  // bitset<64> key_data("1111001001010001110001101110111001011010001000001011100010101001");
  bitset<8> key_data_0("11110010");
  bitset<8> key_data_1("01010001");
  bitset<8> key_data_2("11000110");
  bitset<8> key_data_3("11101110");
  bitset<8> key_data_4("01011010");
  bitset<8> key_data_5("00100000");
  bitset<8> key_data_6("10111000");
  bitset<8> key_data_7("10101001");
  // bitset<64> value_data("0111001001010001110001101110111001011010001000001011100010101001");
  bitset<8> value_data_0("01110010");
  bitset<8> value_data_1("01010001");
  bitset<8> value_data_2("11000110");
  bitset<8> value_data_3("11101110");
  bitset<8> value_data_4("01011010");
  bitset<8> value_data_5("00100000");
  bitset<8> value_data_6("10111000");
  bitset<8> value_data_7("10101001");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<int64_t>> b1;
  b1.SetIndex(0);
  optional<int64_t> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (!this->le) {
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, !this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  bitset<8> bs15(bs1->at(5));
  EXPECT_EQ(bs15, key_data_4);
  bitset<8> bs16(bs1->at(6));
  EXPECT_EQ(bs16, key_data_5);
  bitset<8> bs17(bs1->at(7));
  EXPECT_EQ(bs17, key_data_6);
  bitset<8> bs18(bs1->at(8));
  EXPECT_EQ(bs18, key_data_7);
  Buf* bf11 = new Buf(bs1, !this->le);
  optional<int64_t> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, !this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  bitset<8> bs25(bs2->at(5));
  EXPECT_EQ(bs25, value_data_4);
  bitset<8> bs26(bs2->at(6));
  EXPECT_EQ(bs26, value_data_5);
  bitset<8> bs27(bs2->at(7));
  EXPECT_EQ(bs27, value_data_6);
  bitset<8> bs28(bs2->at(8));
  EXPECT_EQ(bs28, value_data_7);
  Buf* bf21 = new Buf(bs2, !this->le);
  optional<int64_t> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, doubleSchemaPosLeBe) {
  double data = 345235.32656;
  // bitset<64> key_data("1100000100010101000100100100110101001110011001011011111010100001");
  bitset<8> key_data_0("11000001");
  bitset<8> key_data_1("00010101");
  bitset<8> key_data_2("00010010");
  bitset<8> key_data_3("01001101");
  bitset<8> key_data_4("01001110");
  bitset<8> key_data_5("01100101");
  bitset<8> key_data_6("10111110");
  bitset<8> key_data_7("10100001");
  // bitset<64> value_data("0100000100010101000100100100110101001110011001011011111010100001");
  bitset<8> value_data_0("01000001");
  bitset<8> value_data_1("00010101");
  bitset<8> value_data_2("00010010");
  bitset<8> value_data_3("01001101");
  bitset<8> value_data_4("01001110");
  bitset<8> value_data_5("01100101");
  bitset<8> value_data_6("10111110");
  bitset<8> value_data_7("10100001");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<double>> b1;
  b1.SetIndex(0);
  optional<double> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (IsLE()) {
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  bitset<8> bs15(bs1->at(5));
  EXPECT_EQ(bs15, key_data_4);
  bitset<8> bs16(bs1->at(6));
  EXPECT_EQ(bs16, key_data_5);
  bitset<8> bs17(bs1->at(7));
  EXPECT_EQ(bs17, key_data_6);
  bitset<8> bs18(bs1->at(8));
  EXPECT_EQ(bs18, key_data_7);
  Buf* bf11 = new Buf(bs1, this->le);
  optional<double> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  bitset<8> bs25(bs2->at(5));
  EXPECT_EQ(bs25, value_data_4);
  bitset<8> bs26(bs2->at(6));
  EXPECT_EQ(bs26, value_data_5);
  bitset<8> bs27(bs2->at(7));
  EXPECT_EQ(bs27, value_data_6);
  bitset<8> bs28(bs2->at(8));
  EXPECT_EQ(bs28, value_data_7);
  Buf* bf21 = new Buf(bs2, this->le);
  optional<double> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, doubleSchemaPosFakeLeBe) {
  double ori_data = 345235.3265;
  uint64_t ori_data_bits;
  memcpy(&ori_data_bits, &ori_data, 8);
  uint64_t data_bits = bswap_64(ori_data_bits);
  double data;
  memcpy(&data, &data_bits, 8);
  // bitset<64> key_data("1100000100010101000100100100110101001110010101100000010000011001");
  bitset<8> key_data_0("11000001");
  bitset<8> key_data_1("00010101");
  bitset<8> key_data_2("00010010");
  bitset<8> key_data_3("01001101");
  bitset<8> key_data_4("01001110");
  bitset<8> key_data_5("01010110");
  bitset<8> key_data_6("00000100");
  bitset<8> key_data_7("00011001");
  // bitset<64> value_data("0100000100010101000100100100110101001110010101100000010000011001");
  bitset<8> value_data_0("01000001");
  bitset<8> value_data_1("00010101");
  bitset<8> value_data_2("00010010");
  bitset<8> value_data_3("01001101");
  bitset<8> value_data_4("01001110");
  bitset<8> value_data_5("01010110");
  bitset<8> value_data_6("00000100");
  bitset<8> value_data_7("00011001");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<double>> b1;
  b1.SetIndex(0);
  optional<double> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (!this->le) {
    cout << "LE";
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, !this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  bitset<8> bs15(bs1->at(5));
  EXPECT_EQ(bs15, key_data_4);
  bitset<8> bs16(bs1->at(6));
  EXPECT_EQ(bs16, key_data_5);
  bitset<8> bs17(bs1->at(7));
  EXPECT_EQ(bs17, key_data_6);
  bitset<8> bs18(bs1->at(8));
  EXPECT_EQ(bs18, key_data_7);
  Buf* bf11 = new Buf(bs1, !this->le);
  optional<double> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, !this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  bitset<8> bs25(bs2->at(5));
  EXPECT_EQ(bs25, value_data_4);
  bitset<8> bs26(bs2->at(6));
  EXPECT_EQ(bs26, value_data_5);
  bitset<8> bs27(bs2->at(7));
  EXPECT_EQ(bs27, value_data_6);
  bitset<8> bs28(bs2->at(8));
  EXPECT_EQ(bs28, value_data_7);
  Buf* bf21 = new Buf(bs2, !this->le);
  optional<double> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, doubleSchemaNegLeBe) {
  double data = -345235.32656;
  // bitset<64> key_data("0011111011101010111011011011001010110001100110100100000101011110");
  bitset<8> key_data_0("00111110");
  bitset<8> key_data_1("11101010");
  bitset<8> key_data_2("11101101");
  bitset<8> key_data_3("10110010");
  bitset<8> key_data_4("10110001");
  bitset<8> key_data_5("10011010");
  bitset<8> key_data_6("01000001");
  bitset<8> key_data_7("01011110");
  // bitset<64> value_data("1100000100010101000100100100110101001110011001011011111010100001");
  bitset<8> value_data_0("11000001");
  bitset<8> value_data_1("00010101");
  bitset<8> value_data_2("00010010");
  bitset<8> value_data_3("01001101");
  bitset<8> value_data_4("01001110");
  bitset<8> value_data_5("01100101");
  bitset<8> value_data_6("10111110");
  bitset<8> value_data_7("10100001");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<double>> b1;
  b1.SetIndex(0);
  optional<double> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (this->le) {
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  bitset<8> bs15(bs1->at(5));
  EXPECT_EQ(bs15, key_data_4);
  bitset<8> bs16(bs1->at(6));
  EXPECT_EQ(bs16, key_data_5);
  bitset<8> bs17(bs1->at(7));
  EXPECT_EQ(bs17, key_data_6);
  bitset<8> bs18(bs1->at(8));
  EXPECT_EQ(bs18, key_data_7);
  Buf* bf11 = new Buf(bs1, this->le);
  optional<double> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  bitset<8> bs25(bs2->at(5));
  EXPECT_EQ(bs25, value_data_4);
  bitset<8> bs26(bs2->at(6));
  EXPECT_EQ(bs26, value_data_5);
  bitset<8> bs27(bs2->at(7));
  EXPECT_EQ(bs27, value_data_6);
  bitset<8> bs28(bs2->at(8));
  EXPECT_EQ(bs28, value_data_7);
  Buf* bf21 = new Buf(bs2, this->le);
  optional<double> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, doubleSchemaNegFakeLeBe) {
  double ori_data = -345235.32656;
  uint64_t ori_data_bits;
  memcpy(&ori_data_bits, &ori_data, 8);
  uint64_t data_bits = bswap_64(ori_data_bits);
  double data;
  memcpy(&data, &data_bits, 8);
  // bitset<64> key_data("0011111011101010111011011011001010110001100110100100000101011110");
  bitset<8> key_data_0("00111110");
  bitset<8> key_data_1("11101010");
  bitset<8> key_data_2("11101101");
  bitset<8> key_data_3("10110010");
  bitset<8> key_data_4("10110001");
  bitset<8> key_data_5("10011010");
  bitset<8> key_data_6("01000001");
  bitset<8> key_data_7("01011110");
  // bitset<64> value_data("1100000100010101000100100100110101001110011001011011111010100001");
  bitset<8> value_data_0("11000001");
  bitset<8> value_data_1("00010101");
  bitset<8> value_data_2("00010010");
  bitset<8> value_data_3("01001101");
  bitset<8> value_data_4("01001110");
  bitset<8> value_data_5("01100101");
  bitset<8> value_data_6("10111110");
  bitset<8> value_data_7("10100001");
  bitset<8> not_null_tag("00000001");

  DingoSchema<optional<double>> b1;
  b1.SetIndex(0);
  optional<double> data1 = data;

  b1.SetAllowNull(true);
  b1.SetIsKey(true);
  if (!this->le) {
    b1.SetIsLe(true);
  } else {
    b1.SetIsLe(false);
  }
  Buf* bf1 = new Buf(1, !this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, not_null_tag);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, key_data_0);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, key_data_1);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, key_data_2);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, key_data_3);
  bitset<8> bs15(bs1->at(5));
  EXPECT_EQ(bs15, key_data_4);
  bitset<8> bs16(bs1->at(6));
  EXPECT_EQ(bs16, key_data_5);
  bitset<8> bs17(bs1->at(7));
  EXPECT_EQ(bs17, key_data_6);
  bitset<8> bs18(bs1->at(8));
  EXPECT_EQ(bs18, key_data_7);
  Buf* bf11 = new Buf(bs1, !this->le);
  optional<double> data2 = b1.DecodeKey(bf11);
  EXPECT_EQ(data1, data2);
  delete bs1;
  delete bf1;
  delete bf11;

  b1.SetIsKey(false);
  Buf* bf2 = new Buf(1, !this->le);
  b1.EncodeValue(bf2, data1);
  string* bs2 = bf2->GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, not_null_tag);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, value_data_0);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, value_data_1);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, value_data_2);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, value_data_3);
  bitset<8> bs25(bs2->at(5));
  EXPECT_EQ(bs25, value_data_4);
  bitset<8> bs26(bs2->at(6));
  EXPECT_EQ(bs26, value_data_5);
  bitset<8> bs27(bs2->at(7));
  EXPECT_EQ(bs27, value_data_6);
  bitset<8> bs28(bs2->at(8));
  EXPECT_EQ(bs28, value_data_7);
  Buf* bf21 = new Buf(bs2, !this->le);
  optional<double> data3 = b1.DecodeValue(bf21);
  EXPECT_EQ(data1, data3);
  delete bs2;
  delete bf2;
  delete bf21;
}

TEST_F(DingoSerialListTypeTest, longSchema) {
  DingoSchema<optional<int64_t>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  optional<int64_t> data1 = 1543234;
  Buf* bf1 = new Buf(1, this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1, this->le);
  delete bs1;
  optional<int64_t> data2 = b1.DecodeKey(bf2);
  delete bf1;
  delete bf2;
  if (data2.has_value()) {
    EXPECT_EQ(data1, data2.value());
  } else {
    EXPECT_TRUE(0);
  }

  DingoSchema<optional<int64_t>> b2;
  b2.SetIndex(0);
  b2.SetAllowNull(true);
  b2.SetIsKey(false);
  optional<int64_t> data3 = 532142;
  Buf* bf3 = new Buf(1, this->le);
  b2.EncodeValue(bf3, data3);
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2, this->le);
  delete bs2;
  optional<int64_t> data4 = b2.DecodeValue(bf4);
  delete bf3;
  delete bf4;
  if (data4.has_value()) {
    EXPECT_EQ(data3, data4.value());
  } else {
    EXPECT_TRUE(0);
  }

  optional<int64_t> data5 = nullopt;
  Buf* bf5 = new Buf(1, this->le);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3, this->le);
  delete bs3;
  optional<int64_t> data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value());

  DingoSchema<optional<int64_t>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100, this->le);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4, this->le);
  delete bs4;
  EXPECT_FALSE(b3.DecodeKey(bf8).has_value());
  delete bf7;
  delete bf8;
}

TEST_F(DingoSerialListTypeTest, doubleSchema) {
  DingoSchema<optional<double>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  optional<double> data1 = 154.3234;
  Buf* bf1 = new Buf(1, this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1, this->le);
  delete bs1;
  optional<double> data2 = b1.DecodeKey(bf2);
  delete bf1;
  delete bf2;
  if (data2.has_value()) {
    EXPECT_EQ(data1, data2.value());
  } else {
    EXPECT_TRUE(0);
  }

  DingoSchema<optional<double>> b2;
  b2.SetIndex(0);
  b2.SetAllowNull(true);
  b2.SetIsKey(false);
  optional<double> data3 = 5321.42;
  Buf* bf3 = new Buf(1, this->le);
  b2.EncodeValue(bf3, data3);
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2, this->le);
  delete bs2;
  optional<double> data4 = b2.DecodeValue(bf4);
  delete bf3;
  delete bf4;
  if (data4.has_value()) {
    EXPECT_EQ(data3, data4.value());
  } else {
    EXPECT_TRUE(0);
  }

  optional<double> data5 = nullopt;
  Buf* bf5 = new Buf(1, this->le);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3, this->le);
  delete bs3;
  optional<double> data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value());

  DingoSchema<optional<double>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100, this->le);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4, this->le);
  delete bs4;
  EXPECT_FALSE(b3.DecodeKey(bf8).has_value());
  delete bf7;
  delete bf8;
}

TEST_F(DingoSerialListTypeTest, stringSchema) {
  DingoSchema<optional<shared_ptr<string>>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  Buf* bf1 = new Buf(1, this->le);

  std::shared_ptr<std::string> s_data1 = std::make_shared<std::string>(
      "test address test 中文 表情😊🏷️👌 test "
      "测试测试测试三🤣😂😁🐱‍🐉👏");
  std::optional<std::shared_ptr<std::string>> data1{s_data1};

  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1, this->le);
  delete bs1;
  auto data2 = b1.DecodeKey(bf2);
  delete bf1;
  delete bf2;
  if (data2.has_value()) {
    EXPECT_EQ(*data1.value(), *data2.value());
  } else {
    EXPECT_TRUE(0);
  }

  DingoSchema<optional<shared_ptr<string>>> b2;
  b2.SetIndex(0);
  b2.SetAllowNull(true);
  b2.SetIsKey(false);
  std::shared_ptr<std::string> s_data3 = std::make_shared<std::string>(
      "test address test 中文 表情😊🏷️👌 test "
      "测试测试测试三🤣😂😁🐱‍🐉👏");
  std::optional<std::shared_ptr<std::string>> data3{s_data3};

  Buf* bf3 = new Buf(1, this->le);
  b2.EncodeValue(bf3, data3);
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2, this->le);
  delete bs2;
  auto data4 = b2.DecodeValue(bf4);
  delete bf3;
  delete bf4;
  if (data4.has_value()) {
    EXPECT_EQ(*data3.value(), *data4.value()) << "Line: " << __LINE__;
  } else {
    EXPECT_TRUE(0) << "Line: " << __LINE__;
  }

  std::optional<std::shared_ptr<std::string>> data5 = nullopt;
  Buf* bf5 = new Buf(1, this->le);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3, this->le);
  delete bs3;
  auto data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value()) << "Line: " << __LINE__;

  DingoSchema<optional<shared_ptr<string>>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100, this->le);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4, this->le);
  delete bs4;
  auto data8 = b3.DecodeKey(bf8);
  delete bf7;
  delete bf8;
  EXPECT_FALSE(data8.has_value()) << "Line: " << __LINE__;
}

TEST_F(DingoSerialListTypeTest, stringPrefixSchema) {
  DingoSchema<std::optional<std::shared_ptr<std::string>>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  std::shared_ptr<std::string> s_data1 = std::make_shared<std::string>(
      "test address test 中文 表情😊🏷️👌 test "
      "测试测试测试三🤣😂😁🐱‍🐉👏");
  std::optional<std::shared_ptr<std::string>> data1{s_data1};

  Buf* bf1 = new Buf(1, this->le);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();

  Buf* bf2 = new Buf(1, this->le);
  b1.EncodeKeyPrefix(bf2, data1);
  string* bs2 = bf2->GetBytes();
  string bs3(*bs1, 0, bs2->length());

  delete bf1;
  delete bf2;

  EXPECT_EQ(*bs2, bs3) << "Line: " << __LINE__;

  delete bs1;
  delete bs2;
}

TEST_F(DingoSerialListTypeTest, bufLeBe) {
  uint32_t int_data = 1543234;
  uint64_t long_data = -8237583920453957801;
  uint32_t int_atad = bswap_32(1543234);
  uint64_t long_atad = bswap_64(-8237583920453957801);

  bitset<8> bit0("00000000");
  bitset<8> bit1("00010111");
  bitset<8> bit2("10001100");
  bitset<8> bit3("01000010");
  bitset<8> bit4("10001101");
  bitset<8> bit5("10101110");
  bitset<8> bit6("00111001");
  bitset<8> bit7("00010001");
  bitset<8> bit8("10100101");
  bitset<8> bit9("11011111");
  bitset<8> bit10("01000111");
  bitset<8> bit11("01010111");
  bitset<8> bit12("01000010");
  bitset<8> bit13("10001100");
  bitset<8> bit14("00010111");
  bitset<8> bit15("00000000");

  Buf bf1(16, this->le);
  bf1.WriteInt(int_data);
  bf1.WriteLong(long_data);
  bf1.ReverseWriteInt(int_data);
  string* bs1 = bf1.GetBytes();
  bitset<8> bs10(bs1->at(0));
  EXPECT_EQ(bs10, bit0);
  bitset<8> bs11(bs1->at(1));
  EXPECT_EQ(bs11, bit1);
  bitset<8> bs12(bs1->at(2));
  EXPECT_EQ(bs12, bit2);
  bitset<8> bs13(bs1->at(3));
  EXPECT_EQ(bs13, bit3);
  bitset<8> bs14(bs1->at(4));
  EXPECT_EQ(bs14, bit4);
  bitset<8> bs15(bs1->at(5));
  EXPECT_EQ(bs15, bit5);
  bitset<8> bs16(bs1->at(6));
  EXPECT_EQ(bs16, bit6);
  bitset<8> bs17(bs1->at(7));
  EXPECT_EQ(bs17, bit7);
  bitset<8> bs18(bs1->at(8));
  EXPECT_EQ(bs18, bit8);
  bitset<8> bs19(bs1->at(9));
  EXPECT_EQ(bs19, bit9);
  bitset<8> bs110(bs1->at(10));
  EXPECT_EQ(bs110, bit10);
  bitset<8> bs111(bs1->at(11));
  EXPECT_EQ(bs111, bit11);
  bitset<8> bs112(bs1->at(12));
  EXPECT_EQ(bs112, bit12);
  bitset<8> bs113(bs1->at(13));
  EXPECT_EQ(bs113, bit13);
  bitset<8> bs114(bs1->at(14));
  EXPECT_EQ(bs114, bit14);
  bitset<8> bs115(bs1->at(15));
  EXPECT_EQ(bs115, bit15);

  Buf bf2(bs1, this->le);
  EXPECT_EQ(int_data, bf2.ReverseReadInt());
  EXPECT_EQ(int_data, bf2.ReadInt());
  EXPECT_EQ(long_data, bf2.ReadLong());
  delete bs1;

  Buf bf3(16, !this->le);
  bf3.WriteInt(int_atad);
  bf3.WriteLong(long_atad);
  bf3.ReverseWriteInt(int_atad);
  string* bs2 = bf3.GetBytes();
  bitset<8> bs20(bs2->at(0));
  EXPECT_EQ(bs20, bit0);
  bitset<8> bs21(bs2->at(1));
  EXPECT_EQ(bs21, bit1);
  bitset<8> bs22(bs2->at(2));
  EXPECT_EQ(bs22, bit2);
  bitset<8> bs23(bs2->at(3));
  EXPECT_EQ(bs23, bit3);
  bitset<8> bs24(bs2->at(4));
  EXPECT_EQ(bs24, bit4);
  bitset<8> bs25(bs2->at(5));
  EXPECT_EQ(bs25, bit5);
  bitset<8> bs26(bs2->at(6));
  EXPECT_EQ(bs26, bit6);
  bitset<8> bs27(bs2->at(7));
  EXPECT_EQ(bs27, bit7);
  bitset<8> bs28(bs2->at(8));
  EXPECT_EQ(bs28, bit8);
  bitset<8> bs29(bs2->at(9));
  EXPECT_EQ(bs29, bit9);
  bitset<8> bs210(bs2->at(10));
  EXPECT_EQ(bs210, bit10);
  bitset<8> bs211(bs2->at(11));
  EXPECT_EQ(bs211, bit11);
  bitset<8> bs212(bs2->at(12));
  EXPECT_EQ(bs212, bit12);
  bitset<8> bs213(bs2->at(13));
  EXPECT_EQ(bs213, bit13);
  bitset<8> bs214(bs2->at(14));
  EXPECT_EQ(bs214, bit14);
  bitset<8> bs215(bs2->at(15));
  EXPECT_EQ(bs215, bit15);

  Buf bf4(bs2, !this->le);
  EXPECT_EQ(int_atad, bf4.ReverseReadInt());
  EXPECT_EQ(int_atad, bf4.ReadInt());
  EXPECT_EQ(long_atad, bf4.ReadLong());
  delete bs2;
}

TEST_F(DingoSerialListTypeTest, recordTest) {
  InitVector();
  auto schemas = GetSchemas();
  RecordEncoder* re = new RecordEncoder(0, schemas, 0L, this->le);
  InitRecord();

  vector<any>* record1 = GetRecord();
  LOG(INFO) << "record1 size:" << record1->size();
  LOG(INFO) << "schemas size:" << schemas->size();
  pb::common::KeyValue kv;
  (void)re->Encode(*record1, kv);
  delete re;

  RecordDecoder* rd = new RecordDecoder(0, schemas, 0L, this->le);
  vector<any> record2;
  (void)rd->Decode(kv, record2);
  LOG(INFO) << "record2 size:" << record2.size();
  int i = 0;
  for (const auto& bs : *schemas) {
    BaseSchema::Type type = bs->GetType();
    switch (type) {
      case BaseSchema::kBool: {
        optional<bool> r1 = any_cast<optional<bool>>(record1->at(i));
        optional<bool> r2 = any_cast<optional<bool>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(r1.value(), r2.value());
        } else {
          EXPECT_FALSE(r1.has_value());
          EXPECT_FALSE(r2.has_value());
        }
        break;
      }
      case BaseSchema::kInteger: {
        optional<int32_t> r1 = any_cast<optional<int32_t>>(record1->at(i));
        optional<int32_t> r2 = any_cast<optional<int32_t>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(r1.value(), r2.value());
        } else {
          EXPECT_FALSE(r1.has_value());
          EXPECT_FALSE(r2.has_value());
        }
        break;
      }
      case BaseSchema::kLong: {
        optional<int64_t> r1 = any_cast<optional<int64_t>>(record1->at(i));
        optional<int64_t> r2 = any_cast<optional<int64_t>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(r1.value(), r2.value());
        } else {
          EXPECT_FALSE(r1.has_value());
          EXPECT_FALSE(r2.has_value());
        }
        break;
      }
      case BaseSchema::kDouble: {
        optional<double> r1 = any_cast<optional<double>>(record1->at(i));
        optional<double> r2 = any_cast<optional<double>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(r1.value(), r2.value());
        } else {
          EXPECT_FALSE(r1.has_value());
          EXPECT_FALSE(r2.has_value());
        }
        break;
      }
      case BaseSchema::kString: {
        optional<shared_ptr<string>> r1 = any_cast<optional<shared_ptr<string>>>(record1->at(i));
        optional<shared_ptr<string>> r2 = any_cast<optional<shared_ptr<string>>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(*r1.value(), *r2.value());
        } else if (r2.has_value()) {
          EXPECT_TRUE(0);
        } else {
          EXPECT_FALSE(r1.has_value());
        }
        break;
      }
      case BaseSchema::kBoolList: {
        optional<shared_ptr<std::vector<bool>>> r1 = any_cast<optional<shared_ptr<std::vector<bool>>>>(record1->at(i));
        optional<shared_ptr<std::vector<bool>>> r2 = any_cast<optional<shared_ptr<std::vector<bool>>>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          const auto& vector1 = *r1.value();
          const auto& vector2 = *r2.value();
          EXPECT_EQ(vector1.size(), vector2.size());
          for (size_t j = 0; j < vector1.size(); ++j) {
            // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
            EXPECT_EQ(vector1[j], vector2[j]);
          }
        } else if (r2.has_value()) {
          EXPECT_TRUE(0);
        } else {
          EXPECT_FALSE(r1.has_value());
        }
        break;
      }
      case BaseSchema::kStringList: {
        optional<shared_ptr<std::vector<std::string>>> r1 =
            any_cast<optional<shared_ptr<std::vector<std::string>>>>(record1->at(i));
        optional<shared_ptr<std::vector<std::string>>> r2 =
            any_cast<optional<shared_ptr<std::vector<std::string>>>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          const auto& vector1 = *r1.value();
          const auto& vector2 = *r2.value();
          EXPECT_EQ(vector1.size(), vector2.size());
          for (size_t j = 0; j < vector1.size(); ++j) {
            // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
            EXPECT_EQ(vector1[j], vector2[j]);
          }
        } else if (r2.has_value()) {
          EXPECT_TRUE(0);
        } else {
          EXPECT_FALSE(r1.has_value());
        }
        break;
      }
      case BaseSchema::kDoubleList: {
        optional<shared_ptr<std::vector<double>>> r1 =
            any_cast<optional<shared_ptr<std::vector<double>>>>(record1->at(i));
        optional<shared_ptr<std::vector<double>>> r2 =
            any_cast<optional<shared_ptr<std::vector<double>>>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          const auto& vector1 = *r1.value();
          const auto& vector2 = *r2.value();
          EXPECT_EQ(vector1.size(), vector2.size());
          for (size_t j = 0; j < vector1.size(); ++j) {
            // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
            EXPECT_EQ(vector1[j], vector2[j]);
          }
        } else if (r2.has_value()) {
          EXPECT_TRUE(0);
        } else {
          EXPECT_FALSE(r1.has_value());
        }
        break;
      }
      case BaseSchema::kFloatList: {
        optional<shared_ptr<std::vector<float>>> r1 =
            any_cast<optional<shared_ptr<std::vector<float>>>>(record1->at(i));
        optional<shared_ptr<std::vector<float>>> r2 = any_cast<optional<shared_ptr<std::vector<float>>>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          const auto& vector1 = *r1.value();
          const auto& vector2 = *r2.value();
          EXPECT_EQ(vector1.size(), vector2.size());
          for (size_t j = 0; j < vector1.size(); ++j) {
            // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
            EXPECT_EQ(vector1[j], vector2[j]);
          }
        } else if (r2.has_value()) {
          EXPECT_TRUE(0);
        } else {
          EXPECT_FALSE(r1.has_value());
        }
        break;
      }
      case BaseSchema::kIntegerList: {
        optional<shared_ptr<std::vector<int32_t>>> r1 =
            any_cast<optional<shared_ptr<std::vector<int32_t>>>>(record1->at(i));
        optional<shared_ptr<std::vector<int32_t>>> r2 =
            any_cast<optional<shared_ptr<std::vector<int32_t>>>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          const auto& vector1 = *r1.value();
          const auto& vector2 = *r2.value();
          EXPECT_EQ(vector1.size(), vector2.size());
          for (size_t j = 0; j < vector1.size(); ++j) {
            // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
            EXPECT_EQ(vector1[j], vector2[j]);
          }
        } else if (r2.has_value()) {
          EXPECT_TRUE(0);
        } else {
          EXPECT_FALSE(r1.has_value());
        }
        break;
      }
      case BaseSchema::kLongList: {
        optional<shared_ptr<std::vector<int64_t>>> r1 =
            any_cast<optional<shared_ptr<std::vector<int64_t>>>>(record1->at(i));
        optional<shared_ptr<std::vector<int64_t>>> r2 =
            any_cast<optional<shared_ptr<std::vector<int64_t>>>>(record2.at(i));
        if (r1.has_value() && r2.has_value()) {
          const auto& vector1 = *r1.value();
          const auto& vector2 = *r2.value();
          EXPECT_EQ(vector1.size(), vector2.size());
          for (size_t j = 0; j < vector1.size(); ++j) {
            // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
            EXPECT_EQ(vector1[j], vector2[j]);
          }
        } else if (r2.has_value()) {
          EXPECT_TRUE(0);
        } else {
          EXPECT_FALSE(r1.has_value());
        }
        break;
      }
      default: {
        break;
      }
    }
    i++;
  }
  // delete record2;

  vector<int> index{0, 1, 3, 5, 12, 13, 15, 17, 19, 22, 24};
  vector<int> index_temp{0, 1, 3, 5, 12, 13, 15, 17, 19, 22, 24};
  vector<any> record3;
  (void)rd->Decode(kv, index, record3);
  i = 0;
  for (const auto& bs : *schemas) {
    BaseSchema::Type type = bs->GetType();
    switch (type) {
      case BaseSchema::kBool: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<bool> r1 = any_cast<optional<bool>>(record1->at(bs->GetIndex()));
          optional<bool> r2 = any_cast<optional<bool>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kInteger: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<int32_t> r1 = any_cast<optional<int32_t>>(record1->at(bs->GetIndex()));
          optional<int32_t> r2 = any_cast<optional<int32_t>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kLong: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<int64_t> r1 = any_cast<optional<int64_t>>(record1->at(bs->GetIndex()));
          optional<int64_t> r2 = any_cast<optional<int64_t>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kDouble: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<double> r1 = any_cast<optional<double>>(record1->at(bs->GetIndex()));
          optional<double> r2 = any_cast<optional<double>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kString: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<shared_ptr<string>> r1 = any_cast<optional<shared_ptr<string>>>(record1->at(bs->GetIndex()));
          optional<shared_ptr<string>> r2 = any_cast<optional<shared_ptr<string>>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(*r1.value(), *r2.value());
          } else if (r2.has_value()) {
            EXPECT_TRUE(0);
          } else {
            EXPECT_FALSE(r1.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kBoolList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<shared_ptr<std::vector<bool>>> r1 =
              any_cast<optional<shared_ptr<std::vector<bool>>>>(record1->at(bs->GetIndex()));
          optional<shared_ptr<std::vector<bool>>> r2 = any_cast<optional<shared_ptr<std::vector<bool>>>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            const auto& vector1 = *r1.value();
            const auto& vector2 = *r2.value();
            EXPECT_EQ(vector1.size(), vector2.size());
            for (size_t j = 0; j < vector1.size(); ++j) {
              // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
              EXPECT_EQ(vector1[j], vector2[j]);
            }
          } else if (r2.has_value()) {
            EXPECT_TRUE(0);
          } else {
            EXPECT_FALSE(r1.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kStringList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<shared_ptr<std::vector<std::string>>> r1 =
              any_cast<optional<shared_ptr<std::vector<std::string>>>>(record1->at(bs->GetIndex()));
          optional<shared_ptr<std::vector<std::string>>> r2 =
              any_cast<optional<shared_ptr<std::vector<std::string>>>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            const auto& vector1 = *r1.value();
            const auto& vector2 = *r2.value();
            EXPECT_EQ(vector1.size(), vector2.size());
            for (size_t j = 0; j < vector1.size(); ++j) {
              // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
              EXPECT_EQ(vector1[j], vector2[j]);
            }
          } else if (r2.has_value()) {
            EXPECT_TRUE(0);
          } else {
            EXPECT_FALSE(r1.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kDoubleList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<shared_ptr<std::vector<double>>> r1 =
              any_cast<optional<shared_ptr<std::vector<double>>>>(record1->at(bs->GetIndex()));
          optional<shared_ptr<std::vector<double>>> r2 =
              any_cast<optional<shared_ptr<std::vector<double>>>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            const auto& vector1 = *r1.value();
            const auto& vector2 = *r2.value();
            EXPECT_EQ(vector1.size(), vector2.size());
            for (size_t j = 0; j < vector1.size(); ++j) {
              // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
              EXPECT_EQ(vector1[j], vector2[j]);
            }
          } else if (r2.has_value()) {
            EXPECT_TRUE(0);
          } else {
            EXPECT_FALSE(r1.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kFloatList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<shared_ptr<std::vector<float>>> r1 =
              any_cast<optional<shared_ptr<std::vector<float>>>>(record1->at(bs->GetIndex()));
          optional<shared_ptr<std::vector<float>>> r2 =
              any_cast<optional<shared_ptr<std::vector<float>>>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            const auto& vector1 = *r1.value();
            const auto& vector2 = *r2.value();
            EXPECT_EQ(vector1.size(), vector2.size());
            for (size_t j = 0; j < vector1.size(); ++j) {
              // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
              EXPECT_EQ(vector1[j], vector2[j]);
            }
          } else if (r2.has_value()) {
            EXPECT_TRUE(0);
          } else {
            EXPECT_FALSE(r1.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kIntegerList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<shared_ptr<std::vector<int32_t>>> r1 =
              any_cast<optional<shared_ptr<std::vector<int32_t>>>>(record1->at(bs->GetIndex()));
          optional<shared_ptr<std::vector<int32_t>>> r2 =
              any_cast<optional<shared_ptr<std::vector<int32_t>>>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            const auto& vector1 = *r1.value();
            const auto& vector2 = *r2.value();
            EXPECT_EQ(vector1.size(), vector2.size());
            for (size_t j = 0; j < vector1.size(); ++j) {
              // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
              EXPECT_EQ(vector1[j], vector2[j]);
            }
          } else if (r2.has_value()) {
            EXPECT_TRUE(0);
          } else {
            EXPECT_FALSE(r1.has_value());
          }
          i++;
        }
        break;
      }
      case BaseSchema::kLongList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          optional<shared_ptr<std::vector<int64_t>>> r1 =
              any_cast<optional<shared_ptr<std::vector<int64_t>>>>(record1->at(bs->GetIndex()));
          optional<shared_ptr<std::vector<int64_t>>> r2 =
              any_cast<optional<shared_ptr<std::vector<int64_t>>>>(record3.at(i));
          if (r1.has_value() && r2.has_value()) {
            const auto& vector1 = *r1.value();
            const auto& vector2 = *r2.value();
            EXPECT_EQ(vector1.size(), vector2.size());
            for (size_t j = 0; j < vector1.size(); ++j) {
              // LOG(INFO) << "vector1[j]:" << vector1[j] << ",vector2[j]:" << vector2[j];
              EXPECT_EQ(vector1[j], vector2[j]);
            }
          } else if (r2.has_value()) {
            EXPECT_TRUE(0);
          } else {
            EXPECT_FALSE(r1.has_value());
          }
          i++;
        }
        break;
      }
      default: {
        break;
      }
    }
  }

  DeleteSchemas();
  DeleteRecords();
  // delete record3;
  // delete kv;
  delete rd;
}
