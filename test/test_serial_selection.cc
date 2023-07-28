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
#include <chrono>
#include <functional>
#include <memory>
#include <new>
#include <optional>
#include <string>

#include "serial/keyvalue_codec.h"
#include "serial/schema/base_schema.h"
#include "serial/counter.h"

using namespace dingodb;
using namespace std;

class DingoSerialTest : public testing::Test {
 private:
  std::shared_ptr<vector<std::shared_ptr<BaseSchema>>> schemas_;
  vector<any>* record_;

 public:
  void InitVector() {
    schemas_ = std::make_shared<vector<std::shared_ptr<BaseSchema>>>(11);

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
  }

  void DeleteSchemas() {
    schemas_->clear();
    schemas_->shrink_to_fit();
  }

  void InitRecord() {
    record_ = new vector<any>(11);
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

TEST_F(DingoSerialTest, keyvaluecodeStringLoopTest) {
  auto td = std::make_shared<pb::meta::TableDefinition>();
  td->set_name("test");

  int32_t n = 10000;
  // Define column definitions and records
  vector<any> record1(n);
  for (int i = 0; i < n; i++) {
    std::string column_name = "column_" + std::to_string(i);
    std::shared_ptr<std::string> column_value = std::make_shared<std::string>("value_" + std::to_string(i));
    pb::meta::ColumnDefinition* cd = td->add_columns();
    cd->set_name(column_name);
    cd->set_element_type(pb::meta::ELEM_TYPE_STRING);
    cd->set_nullable(false);
    cd->set_indexofkey(0);
    record1.at(i) = optional<shared_ptr<string>>{column_value};
  }
  ASSERT_EQ(n, record1.size());
  // Create KeyValueCodec and encode record
  KeyValueCodec* codec = new KeyValueCodec(td, 0);
  pb::common::KeyValue kv;
  Counter LoadCnter1;
	LoadCnter1.reStart();
  (void)codec->Encode(record1, kv);
  int64_t timeDBFetch1 = LoadCnter1.mtimeElapsed();
  std::cout << "Encode Time : " << timeDBFetch1 << " milliseconds" << std::endl;
  // Decode record and verify values
  std::vector<std::any> decoded_records;
  Counter LoadCnter2;
	LoadCnter2.reStart();
  (void)codec->Decode(kv, decoded_records);
  std::cout << "Decode Time : " << LoadCnter2.mtimeElapsed() << " milliseconds" << std::endl;
  std::cout << "Decode output records size:" << decoded_records.size() << std::endl;

  // Decode record selection columns
  int selectionColumnsSize = n-3;
  {
    std::vector<int> indexes;
    for(int i =0; i < selectionColumnsSize; i++) {
      indexes.push_back(i);
    }
    std::vector<int>& column_indexes = indexes;
    std::vector<std::any> decoded_s_records;
    Counter LoadCnter3;
    LoadCnter3.reStart();
    // std::sort(column_indexes.begin(), column_indexes.end());
    (void)codec->Decode(kv, column_indexes, decoded_s_records);
    std::cout << "Decode selection columns size:" << selectionColumnsSize << ", need Time : " << LoadCnter3.mtimeElapsed() << " milliseconds" << std::endl;
    std::cout << "Decode selection output records size:" << decoded_s_records.size() << std::endl;
  }
  {
    std::vector<int> indexes;
    selectionColumnsSize = n-selectionColumnsSize;
    for(int i =0; i < selectionColumnsSize; i++) {
      indexes.push_back(i);
    }
    std::vector<int>& column_indexes = indexes;
    std::vector<std::any> decoded_s_records;
    Counter LoadCnter3;
    LoadCnter3.reStart();
    // std::sort(column_indexes.begin(), column_indexes.end());
    (void)codec->Decode(kv, column_indexes, decoded_s_records);
    std::cout << "Decode selection columns size:" << selectionColumnsSize << ", need Time : " << LoadCnter3.mtimeElapsed() << " milliseconds" << std::endl;
    std::cout << "Decode selection output records size:" << decoded_s_records.size() << std::endl;
  }
}

// TEST_F(DingoSerialTest, keyvaluecodeDoubleLoopTest) {
//   auto td = std::make_shared<pb::meta::TableDefinition>();
//   td->set_name("test");

//   int32_t n = 10000;
//   // Define column definitions and records
//   vector<any> record1(n);
//   for (int i = 0; i < n; i++) {
//     std::string column_name = "column_" + std::to_string(i);
//     optional<double> column_value = i;
//     pb::meta::ColumnDefinition* cd = td->add_columns();
//     cd->set_name(column_name);
//     cd->set_element_type(pb::meta::ELEM_TYPE_DOUBLE);
//     cd->set_nullable(false);
//     cd->set_indexofkey(0);
//     record1.at(i) = column_value;
//   }
//   ASSERT_EQ(n, record1.size());
//   // Create KeyValueCodec and encode record
//   KeyValueCodec* codec = new KeyValueCodec(td, 0);
//   pb::common::KeyValue kv;
//   auto start_time = std::chrono::high_resolution_clock::now();
//   (void)codec->Encode(record1, kv);
//   auto end_time = std::chrono::high_resolution_clock::now();
//   auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
//   std::cout << "Encode Time taken: " << duration.count() << " milliseconds" << std::endl;

//   // Decode record and verify values
//   std::vector<std::any> decoded_records;
//   (void)codec->Decode(kv, decoded_records);
//   auto decode_end_time = std::chrono::high_resolution_clock::now();
//   auto decode_duration = std::chrono::duration_cast<std::chrono::milliseconds>(decode_end_time - end_time);
//   std::cout << "Decode Time taken: " << decode_duration.count() << " milliseconds" << std::endl;
//   std::cout << "Decode output records size:" << decoded_records.size() << std::endl;

//   std::vector<int> indexes = {1, 3, 5};
//   std::vector<int>& column_indexes = indexes;
//   std::vector<std::any> decoded_s_records;
//   auto decode_s_s_end_time = std::chrono::high_resolution_clock::now();
//   (void)codec->Decode(kv, column_indexes, decoded_s_records);
//   auto decode_s_end_time = std::chrono::high_resolution_clock::now();
//   auto decode_s_duration = std::chrono::duration_cast<std::chrono::milliseconds>(decode_s_end_time - decode_s_s_end_time);
//   std::cout << "Decode selection Time taken: " << decode_s_duration.count() << " milliseconds" << std::endl;
//   std::cout << "Decode selection output records size:" << decoded_s_records.size() << std::endl;
// }