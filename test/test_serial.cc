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
#include <algorithm>
#include <bitset>
#include <new>
#include <optional>
#include <functional>
#include "serial/schema/base_schema.h"

#include <serial/record_decoder.h>
#include <serial/record_encoder.h>
#include <serial/utils.h>
#include <proto/meta.pb.h>

using namespace dingodb;
using namespace std;

class DingoSerialTest : public testing::Test {
 private:
  vector<BaseSchema*>* schemas_;
  vector<any>* record_;

 public:
  void InitVector() {
    schemas_ = new vector<BaseSchema*>(11);
    DingoSchema<optional<int32_t>>* id = new DingoSchema<optional<int32_t>>();
    id->SetIndex(0);
    id->SetAllowNull(false);
    id->SetIsKey(true);
    schemas_->at(0) = id;

    DingoSchema<optional<reference_wrapper<string>>>* name = new DingoSchema<optional<reference_wrapper<string>>>();
    name->SetIndex(1);
    name->SetAllowNull(false);
    name->SetIsKey(true);
    schemas_->at(1) = name;

    DingoSchema<optional<reference_wrapper<string>>>* gender = new DingoSchema<optional<reference_wrapper<string>>>();
    gender->SetIndex(2);
    gender->SetAllowNull(false);
    gender->SetIsKey(true);
    schemas_->at(2) = gender;

    DingoSchema<optional<int64_t>>* score = new DingoSchema<optional<int64_t>>();
    score->SetIndex(3);
    score->SetAllowNull(false);
    score->SetIsKey(true);
    schemas_->at(3) = score;

    DingoSchema<optional<reference_wrapper<string>>>* addr = new DingoSchema<optional<reference_wrapper<string>>>();
    addr->SetIndex(4);
    addr->SetAllowNull(true);
    addr->SetIsKey(false);
    schemas_->at(4) = addr;

    DingoSchema<optional<bool>>* exist = new DingoSchema<optional<bool>>();
    exist->SetIndex(5);
    exist->SetAllowNull(false);
    exist->SetIsKey(false);
    schemas_->at(5) = exist;

    DingoSchema<optional<reference_wrapper<string>>>* pic = new DingoSchema<optional<reference_wrapper<string>>>();
    pic->SetIndex(6);
    pic->SetAllowNull(true);
    pic->SetIsKey(false);
    schemas_->at(6) = pic;

    DingoSchema<optional<int32_t>>* test_null = new DingoSchema<optional<int32_t>>();
    test_null->SetIndex(7);
    test_null->SetAllowNull(true);
    test_null->SetIsKey(false);
    schemas_->at(7) = test_null;

    DingoSchema<optional<int32_t>>* age = new DingoSchema<optional<int32_t>>();
    age->SetIndex(8);
    age->SetAllowNull(false);
    age->SetIsKey(false);
    schemas_->at(8) = age;

    DingoSchema<optional<int64_t>>* prev = new DingoSchema<optional<int64_t>>();
    prev->SetIndex(9);
    prev->SetAllowNull(false);
    prev->SetIsKey(false);
    schemas_->at(9) = prev;

    DingoSchema<optional<double>>* salary = new DingoSchema<optional<double>>();
    salary->SetIndex(10);
    salary->SetAllowNull(true);
    salary->SetIsKey(false);
    schemas_->at(10) = salary;
  }
  void DeleteSchemas() {
    for (BaseSchema *bs : *schemas_) {
      delete bs;
    }
    schemas_->clear();
    schemas_->shrink_to_fit();
  }
  void InitRecord() {
    record_ = new vector<any>(11);
    optional<int32_t> id = 0;
    string *name = new string("tn");
    string *gender = new string("f");
    optional<int64_t> score = 214748364700L;
    string *addr = new string(
        "test address test 中文 表情😊🏷️👌 test "
        "测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试测"
        "试伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫");
    optional<bool> exist = false;
    optional<reference_wrapper<string>> pic = nullopt;
    optional<int32_t> test_null = nullopt;
    optional<int32_t> age = -20;
    optional<int64_t> prev = -214748364700L;
    optional<double> salary = 873485.4234;

    record_->at(0) = id;
    record_->at(1) = optional<reference_wrapper<string>>{*name};
    record_->at(2) = optional<reference_wrapper<string>>{*gender};
    record_->at(3) = score;
    record_->at(4) = optional<reference_wrapper<string>>{*addr};
    record_->at(5) = exist;
    record_->at(6) = pic;
    record_->at(7) = test_null;
    record_->at(8) = age;
    record_->at(9) = prev;
    record_->at(10) = salary;
  }
  void DeleteRecords() {
    optional<reference_wrapper<string>> name = any_cast<optional<reference_wrapper<string>>>(record_->at(1));
    if (name.has_value()) {
      delete &name->get();
    }
    optional<reference_wrapper<string>> gender = any_cast<optional<reference_wrapper<string>>>(record_->at(2));
    if (gender.has_value()) {
      delete &gender->get();
    }
    optional<reference_wrapper<string>> addr = any_cast<optional<reference_wrapper<string>>>(record_->at(4));
    if (addr.has_value()) {
      delete &addr->get();
    }
    record_->clear();
    record_->shrink_to_fit();
  }
  vector<BaseSchema*>* GetSchemas() { return schemas_; }
  vector<any>* GetRecord() { return record_; }

 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DingoSerialTest, boolSchema) {
  DingoSchema<optional<bool>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  optional<bool> data1 = false;
  Buf* bf1 = new Buf(1);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1);
  delete bs1;
  optional<bool> data2 = b1.DecodeKey(bf2);
  delete bf1;
  delete bf2;
  if (data2.has_value()) {
    EXPECT_EQ(data1, data2.value());
  } else {
    EXPECT_TRUE(0);
  }

  DingoSchema<optional<bool>> b2;
  b2.SetIndex(0);
  b2.SetAllowNull(true);
  b2.SetIsKey(false);
  optional<bool> data3 = true;
  Buf* bf3 = new Buf(1);
  b2.EncodeValue(bf3, data3);
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2);
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
  Buf* bf5 = new Buf(1);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3);
  delete bs3;
  optional<bool> data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value());

  DingoSchema<optional<bool>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4);
  delete bs4;
  EXPECT_FALSE(b3.DecodeKey(bf8).has_value());
  delete bf7;
  delete bf8;
}

TEST_F(DingoSerialTest, integerSchema) {
  DingoSchema<optional<int32_t>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  optional<int32_t> data1 = 1543234;
  Buf* bf1 = new Buf(1);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1);
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
  Buf* bf3 = new Buf(1);
  b2.EncodeValue(bf3, data3);
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2);
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
  Buf* bf5 = new Buf(1);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3);
  delete bs3;
  optional<int32_t> data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value());

  DingoSchema<optional<int32_t>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4);
  delete bs4;
  EXPECT_FALSE(b3.DecodeKey(bf8).has_value());
  delete bf7;
  delete bf8;
}

TEST_F(DingoSerialTest, longSchema) {
  DingoSchema<optional<int64_t>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  optional<int64_t> data1 = 1543234;
  Buf* bf1 = new Buf(1);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1);
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
  Buf* bf3 = new Buf(1);
  b2.EncodeValue(bf3, data3);
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2);
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
  Buf* bf5 = new Buf(1);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3);
  delete bs3;
  optional<int64_t> data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value());

  DingoSchema<optional<int64_t>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4);
  delete bs4;
  EXPECT_FALSE(b3.DecodeKey(bf8).has_value());
  delete bf7;
  delete bf8;
}

TEST_F(DingoSerialTest, doubleSchema) {
  DingoSchema<optional<double>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  optional<double> data1 = 154.3234;
  Buf* bf1 = new Buf(1);
  b1.EncodeKey(bf1, data1);
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1);
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
  Buf* bf3 = new Buf(1);
  b2.EncodeValue(bf3, data3);
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2);
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
  Buf* bf5 = new Buf(1);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3);
  delete bs3;
  optional<double> data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value());

  DingoSchema<optional<double>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4);
  delete bs4;
  EXPECT_FALSE(b3.DecodeKey(bf8).has_value());
  delete bf7;
  delete bf8;
}

TEST_F(DingoSerialTest, stringSchema) {
  DingoSchema<optional<reference_wrapper<string>>> b1;
  b1.SetIndex(0);
  b1.SetAllowNull(false);
  b1.SetIsKey(true);
  string data1 =
      "test address test 中文 表情😊🏷️👌 test "
      "测试测试测试三🤣😂😁🐱‍🐉👏";
  Buf* bf1 = new Buf(1);
  b1.EncodeKey(bf1, optional<reference_wrapper<string>>{data1});
  string* bs1 = bf1->GetBytes();
  Buf* bf2 = new Buf(bs1);
  delete bs1;
  optional<reference_wrapper<string>> data2 = b1.DecodeKey(bf2);
  delete bf1;
  delete bf2;
  if (data2.has_value()) {
    EXPECT_EQ(data1, data2->get());
    delete &data2->get();
  } else {
    EXPECT_TRUE(0);
  }

  DingoSchema<optional<reference_wrapper<string>>> b2;
  b2.SetIndex(0);
  b2.SetAllowNull(true);
  b2.SetIsKey(false);
  string data3 =
      "test address test 中文 表情😊🏷️👌 test "
      "测试测试测试三🤣😂😁🐱‍🐉👏";
  Buf* bf3 = new Buf(1);
  b2.EncodeValue(bf3, optional<reference_wrapper<string>>{data3});
  string* bs2 = bf3->GetBytes();
  Buf* bf4 = new Buf(bs2);
  delete bs2;
  optional<reference_wrapper<string>> data4 = b2.DecodeValue(bf4);
  delete bf3;
  delete bf4;
  if (data4.has_value()) {
    EXPECT_EQ(data3, data4->get());
    delete &data4->get();
  } else {
    EXPECT_TRUE(0);
  }

  optional<reference_wrapper<string>>data5 = nullopt;
  Buf* bf5 = new Buf(1);
  b2.EncodeValue(bf5, data5);
  string* bs3 = bf5->GetBytes();
  Buf* bf6 = new Buf(bs3);
  delete bs3;
  optional<reference_wrapper<string>> data6 = b2.DecodeValue(bf6);
  delete bf5;
  delete bf6;
  EXPECT_FALSE(data6.has_value());

  DingoSchema<optional<reference_wrapper<string>>> b3;
  b3.SetIndex(0);
  b3.SetAllowNull(true);
  b3.SetIsKey(true);
  Buf* bf7 = new Buf(100);
  b3.EncodeValue(bf7, nullopt);
  string* bs4 = bf7->GetBytes();
  Buf* bf8 = new Buf(bs4);
  delete bs4;
  optional<reference_wrapper<string>> data8 = b3.DecodeKey(bf8);
  delete bf7;
  delete bf8;
  EXPECT_FALSE(data8.has_value());
}

TEST_F(DingoSerialTest, recordTest) {
  InitVector();
  vector<BaseSchema*>* schemas = GetSchemas();
  RecordEncoder* re = new RecordEncoder(0, schemas, 0L);
  InitRecord();

  vector<any>* record1 = GetRecord();
  KeyValue* kv = re->Encode(record1);
  delete re;

  RecordDecoder* rd = new RecordDecoder(0, schemas, 0L);
  vector<any>* record2 = rd->Decode(kv);

  for (BaseSchema *bs : *schemas) {
    BaseSchema::Type type = bs->GetType();
    switch (type) {
      case BaseSchema::kBool: {
        optional<bool> r1 = any_cast<optional<bool>>(record1->at(bs->GetIndex()));
        optional<bool> r2 = any_cast<optional<bool>>(record2->at(bs->GetIndex()));
        if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
        break;
      }
      case BaseSchema::kInteger: {
        optional<int32_t> r1 = any_cast<optional<int32_t>>(record1->at(bs->GetIndex()));
        optional<int32_t> r2 = any_cast<optional<int32_t>>(record2->at(bs->GetIndex()));
        if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
        break;
      }
      case BaseSchema::kLong: {
        optional<int64_t> r1 = any_cast<optional<int64_t>>(record1->at(bs->GetIndex()));
        optional<int64_t> r2 = any_cast<optional<int64_t>>(record2->at(bs->GetIndex()));
        if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
        break;
      }
      case BaseSchema::kDouble: {
        optional<double> r1 = any_cast<optional<double>>(record1->at(bs->GetIndex()));
        optional<double> r2 = any_cast<optional<double>>(record2->at(bs->GetIndex()));
        if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
        break;
      }
      case BaseSchema::kString: {
        optional<reference_wrapper<string>> r1 = any_cast<optional<reference_wrapper<string>>>(record1->at(bs->GetIndex()));
        optional<reference_wrapper<string>> r2 = any_cast<optional<reference_wrapper<string>>>(record2->at(bs->GetIndex()));
        if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1->get(), r2->get());
            delete &r2->get();
          } else if (r2.has_value()) {
            delete &r2->get();
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
  }
  delete record2;

  vector<int> index{0, 1, 3, 5};
  vector<int> index_temp{0, 1, 3, 5};
  vector<any>* record3 = rd->Decode(kv, &index);

  for (BaseSchema *bs : *schemas) {
    BaseSchema::Type type = bs->GetType();
    switch (type) {
      case BaseSchema::kBool: {
        if (binary_search(index_temp.begin(), index_temp.end(),
                          bs->GetIndex())) {
          optional<bool> r1 = any_cast<optional<bool>>(record1->at(bs->GetIndex()));
          optional<bool> r2 = any_cast<optional<bool>>(record3->at(bs->GetIndex()));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
        } else {
          optional<bool> r2 = any_cast<optional<bool>>(record3->at(bs->GetIndex()));
          EXPECT_FALSE(r2.has_value());
        }
        break;
      }
      case BaseSchema::kInteger: {
        if (binary_search(index_temp.begin(), index_temp.end(),
                          bs->GetIndex())) {
          optional<int32_t> r1 = any_cast<optional<int32_t>>(record1->at(bs->GetIndex()));
          optional<int32_t> r2 = any_cast<optional<int32_t>>(record3->at(bs->GetIndex()));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
        } else {
           optional<int32_t> r2 = any_cast< optional<int32_t>>(record3->at(bs->GetIndex()));
          EXPECT_FALSE(r2.has_value());
        }
        break;
      }
      case BaseSchema::kLong: {
        if (binary_search(index_temp.begin(), index_temp.end(),
                          bs->GetIndex())) {
          optional<int64_t> r1 = any_cast<optional<int64_t>>(record1->at(bs->GetIndex()));
          optional<int64_t> r2 = any_cast<optional<int64_t>>(record3->at(bs->GetIndex()));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
        } else {
          optional<int64_t> r2 = any_cast<optional<int64_t>>(record3->at(bs->GetIndex()));
          EXPECT_FALSE(r2.has_value());
        }
        break;
      }
      case BaseSchema::kDouble: {
        if (binary_search(index_temp.begin(), index_temp.end(),
                          bs->GetIndex())) {
          optional<double> r1 = any_cast<optional<double>>(record1->at(bs->GetIndex()));
          optional<double> r2 = any_cast<optional<double>>(record3->at(bs->GetIndex()));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1.value(), r2.value());
          } else {
            EXPECT_FALSE(r1.has_value());
            EXPECT_FALSE(r2.has_value());
          }
        } else {
          optional<double> r2 = any_cast<optional<double>>(record3->at(bs->GetIndex()));
          EXPECT_FALSE(r2.has_value());
        }
        break;
      }
      case BaseSchema::kString: {
        if (binary_search(index_temp.begin(), index_temp.end(),
                          bs->GetIndex())) {
          optional<reference_wrapper<string>> r1 = any_cast<optional<reference_wrapper<string>>>(record1->at(bs->GetIndex()));
          optional<reference_wrapper<string>> r2 = any_cast<optional<reference_wrapper<string>>>(record3->at(bs->GetIndex()));
          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(r1->get(), r2->get());
            delete &r2->get();
          } else if (r2.has_value()) {
            delete &r2->get();
            EXPECT_TRUE(0);
          } else {
            EXPECT_FALSE(r1.has_value());
          }
        } else {
          optional<reference_wrapper<string>> r2 = any_cast<optional<reference_wrapper<string>>>(record3->at(bs->GetIndex()));
          EXPECT_FALSE(r2.has_value());
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
  delete record3;
  delete kv;
  delete rd;
}


TEST_F(DingoSerialTest, tabledefinitionTest) {
  pb::meta::TableDefinition td;
  td.set_name("test");

  pb::meta::ColumnDefinition *cd1 = td.add_columns();
  cd1->set_name("id");
  cd1->set_element_type(pb::meta::ELEM_TYPE_INT32);
  cd1->set_nullable(false);
  cd1->set_indexofkey(0);

  pb::meta::ColumnDefinition *cd2 = td.add_columns();
  cd2->set_name("name");
  cd2->set_element_type(pb::meta::ELEM_TYPE_STRING);
  cd2->set_nullable(false);
  cd2->set_indexofkey(0);

  pb::meta::ColumnDefinition *cd3 = td.add_columns();
  cd3->set_name("gender");
  cd3->set_element_type(pb::meta::ELEM_TYPE_STRING);
  cd3->set_nullable(false);
  cd3->set_indexofkey(0);

  pb::meta::ColumnDefinition *cd4 = td.add_columns();
  cd4->set_name("score");
  cd4->set_element_type(pb::meta::ELEM_TYPE_INT64);
  cd4->set_nullable(false);
  cd4->set_indexofkey(0);

  pb::meta::ColumnDefinition *cd5 = td.add_columns();
  cd5->set_name("addr");
  cd5->set_element_type(pb::meta::ELEM_TYPE_STRING);
  cd5->set_nullable(true);
  cd5->set_indexofkey(-1);

  pb::meta::ColumnDefinition *cd6 = td.add_columns();
  cd6->set_name("exist");
  cd6->set_element_type(pb::meta::ELEM_TYPE_BOOLEAN);
  cd6->set_nullable(false);
  cd6->set_indexofkey(-1);

  pb::meta::ColumnDefinition *cd7 = td.add_columns();
  cd7->set_name("pic");
  cd7->set_element_type(pb::meta::ELEM_TYPE_BYTES);
  cd7->set_nullable(true);
  cd7->set_indexofkey(-1);

  pb::meta::ColumnDefinition *cd8 = td.add_columns();
  cd8->set_name("testNull");
  cd8->set_element_type(pb::meta::ELEM_TYPE_INT32);
  cd8->set_nullable(true);
  cd8->set_indexofkey(-1);

  pb::meta::ColumnDefinition *cd9 = td.add_columns();
  cd9->set_name("age");
  cd9->set_element_type(pb::meta::ELEM_TYPE_INT32);
  cd9->set_nullable(false);
  cd9->set_indexofkey(-1);

  pb::meta::ColumnDefinition *cd10 = td.add_columns();
  cd10->set_name("prev");
  cd10->set_element_type(pb::meta::ELEM_TYPE_INT64);
  cd10->set_nullable(false);
  cd10->set_indexofkey(-1);

  pb::meta::ColumnDefinition *cd11 = td.add_columns();
  cd11->set_name("salary");
  cd11->set_element_type(pb::meta::ELEM_TYPE_DOUBLE);
  cd11->set_nullable(true);
  cd11->set_indexofkey(-1);

  vector<BaseSchema*>* schemas = TableDefinitionToDingoSchema(td);
  BaseSchema* id = schemas->at(0);
  EXPECT_EQ(id->GetIndex(), 0);
  EXPECT_EQ(id->GetType(), BaseSchema::Type::kInteger);
  EXPECT_FALSE(id->AllowNull());
  EXPECT_TRUE(id->IsKey());

  BaseSchema* name = schemas->at(1);
  EXPECT_EQ(name->GetIndex(), 1);
  EXPECT_EQ(name->GetType(), BaseSchema::Type::kString);
  EXPECT_FALSE(name->AllowNull());
  EXPECT_TRUE(name->IsKey());

  BaseSchema* gender = schemas->at(2);
  EXPECT_EQ(gender->GetIndex(), 2);
  EXPECT_EQ(gender->GetType(), BaseSchema::Type::kString);
  EXPECT_FALSE(gender->AllowNull());
  EXPECT_TRUE(gender->IsKey());

  BaseSchema* score = schemas->at(3);
  EXPECT_EQ(score->GetIndex(), 3);
  EXPECT_EQ(score->GetType(), BaseSchema::Type::kLong);
  EXPECT_FALSE(score->AllowNull());
  EXPECT_TRUE(score->IsKey());

  BaseSchema* addr = schemas->at(4);
  EXPECT_EQ(addr->GetIndex(), 4);
  EXPECT_EQ(addr->GetType(), BaseSchema::Type::kString);
  EXPECT_TRUE(addr->AllowNull());
  EXPECT_FALSE(addr->IsKey());

  BaseSchema* exist = schemas->at(5);
  EXPECT_EQ(exist->GetIndex(), 5);
  EXPECT_EQ(exist->GetType(), BaseSchema::Type::kBool);
  EXPECT_FALSE(exist->AllowNull());
  EXPECT_FALSE(exist->IsKey());

  BaseSchema* pic = schemas->at(6);
  EXPECT_EQ(pic->GetIndex(), 6);
  EXPECT_EQ(pic->GetType(), BaseSchema::Type::kString);
  EXPECT_TRUE(pic->AllowNull());
  EXPECT_FALSE(pic->IsKey());

  BaseSchema* test_null = schemas->at(7);
  EXPECT_EQ(test_null->GetIndex(), 7);
  EXPECT_EQ(test_null->GetType(), BaseSchema::Type::kInteger);
  EXPECT_TRUE(test_null->AllowNull());
  EXPECT_FALSE(test_null->IsKey());

  BaseSchema* age = schemas->at(8);
  EXPECT_EQ(age->GetIndex(), 8);
  EXPECT_EQ(age->GetType(), BaseSchema::Type::kInteger);
  EXPECT_FALSE(age->AllowNull());
  EXPECT_FALSE(age->IsKey());

  BaseSchema* prev = schemas->at(9);
  EXPECT_EQ(prev->GetIndex(), 9);
  EXPECT_EQ(prev->GetType(), BaseSchema::Type::kLong);
  EXPECT_FALSE(prev->AllowNull());
  EXPECT_FALSE(prev->IsKey()); 

  BaseSchema* salary = schemas->at(10);
  EXPECT_EQ(salary->GetIndex(), 10);
  EXPECT_EQ(salary->GetType(), BaseSchema::Type::kDouble);
  EXPECT_TRUE(salary->AllowNull());
  EXPECT_FALSE(salary->IsKey()); 

  delete schemas;
}