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

#ifndef DINGODB_ENGINE_WRITE_DATA_H_
#define DINGODB_ENGINE_WRITE_DATA_H_

#include <cstdarg>
#include <cstdint>
#include <vector>

#include "butil/status.h"
#include "proto/common.pb.h"
#include "proto/raft.pb.h"

namespace dingodb {

enum class DatumType {
  kPut = 0,
  kPutIfabsent = 1,
  kCreateSchema = 2,
  kDeleteRange = 3,
  kDeleteBatch = 4,
  kSplit = 5,
  kCompareAndSet = 6,
};

class DatumAble {
 public:
  virtual ~DatumAble() = default;
  virtual DatumType GetType() = 0;
  virtual pb::raft::Request* TransformToRaft() = 0;
  virtual void TransformFromRaft(pb::raft::Response& resonse) = 0;
};

struct PutDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kPut; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::PUT);
    pb::raft::PutRequest* put_request = request->mutable_put();
    for (auto& kv : kvs) {
      put_request->set_cf_name(cf_name);
      put_request->add_kvs()->CopyFrom(kv);
    }

    return request;
  };

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  std::string cf_name;
  std::vector<pb::common::KeyValue> kvs;
};

struct VectorAddDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kPut; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::VECTOR_ADD);
    pb::raft::VectorAddRequest* vector_add_request = request->mutable_vector_add();
    for (auto& vector : vectors) {
      vector_add_request->set_cf_name(cf_name);
      vector_add_request->add_vectors()->CopyFrom(vector);
    }

    return request;
  };

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  std::string cf_name;
  std::vector<pb::common::VectorWithId> vectors;
};

struct VectorDeleteDatum : public DatumAble {
  ~VectorDeleteDatum() override = default;
  DatumType GetType() override { return DatumType::kDeleteBatch; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::VECTOR_DELETE);
    pb::raft::VectorDeleteRequest* vector_delete_request = request->mutable_vector_delete();
    vector_delete_request->set_cf_name(cf_name);
    for (const auto& id : ids) {
      vector_delete_request->add_ids(id);
    }

    return request;
  }

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  std::string cf_name;
  std::vector<uint64_t> ids;
};

struct PutIfAbsentDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kPutIfabsent; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::PUTIFABSENT);
    pb::raft::PutIfAbsentRequest* put_if_absent_request = request->mutable_put_if_absent();
    put_if_absent_request->set_cf_name(cf_name);
    put_if_absent_request->set_is_atomic(is_atomic);
    for (auto& kv : kvs) {
      put_if_absent_request->add_kvs()->CopyFrom(kv);
    }

    return request;
  }

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  std::string cf_name;
  std::vector<pb::common::KeyValue> kvs;
  bool is_atomic;
};

struct CompareAndSetDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kCompareAndSet; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::COMPAREANDSET);
    pb::raft::CompareAndSetRequest* compare_and_set_request = request->mutable_compare_and_set();
    compare_and_set_request->set_cf_name(cf_name);

    for (auto& kv : kvs) {
      compare_and_set_request->add_kvs()->CopyFrom(kv);
    }

    for (auto& expect_value : expect_values) {
      compare_and_set_request->add_expect_values(expect_value);
    }

    compare_and_set_request->set_is_atomic(is_atomic);

    return request;
  }

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  std::string cf_name;
  std::vector<pb::common::KeyValue> kvs;
  std::vector<std::string> expect_values;
  bool is_atomic;
};

struct DeleteBatchDatum : public DatumAble {
  ~DeleteBatchDatum() override = default;
  DatumType GetType() override { return DatumType::kDeleteBatch; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::DELETEBATCH);
    pb::raft::DeleteBatchRequest* delete_batch_request = request->mutable_delete_batch();
    delete_batch_request->set_cf_name(cf_name);
    for (const auto& key : keys) {
      delete_batch_request->add_keys()->assign(key.data(), key.size());
    }

    return request;
  }

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  std::string cf_name;
  std::vector<std::string> keys;
};

struct DeleteRangeDatum : public DatumAble {
  ~DeleteRangeDatum() override = default;
  DatumType GetType() override { return DatumType::kDeleteRange; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::DELETERANGE);
    pb::raft::DeleteRangeRequest* delete_range_request = request->mutable_delete_range();
    delete_range_request->set_cf_name(cf_name);

    for (const auto& range : ranges) {
      delete_range_request->add_ranges()->CopyFrom(range);
    }

    return request;
  }

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  std::string cf_name;
  std::vector<pb::common::Range> ranges;
};

struct CreateSchemaDatum : public DatumAble {
  ~CreateSchemaDatum() override = default;
  DatumType GetType() override { return DatumType::kCreateSchema; }

  pb::raft::Request* TransformToRaft() override { return nullptr; }
  void TransformFromRaft(pb::raft::Response& resonse) override {}
};

struct SplitDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kSplit; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::SPLIT);
    pb::raft::SplitRequest* split_request = request->mutable_split();
    split_request->set_from_region_id(from_region_id);
    split_request->set_to_region_id(to_region_id);
    split_request->set_split_key(split_key);

    return request;
  };

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  uint64_t from_region_id;
  uint64_t to_region_id;
  std::string split_key;
};

class WriteData {
 public:
  std::vector<std::shared_ptr<DatumAble>> Datums() const { return datums_; }
  void AddDatums(std::shared_ptr<DatumAble> datum) { datums_.push_back(datum); }

 private:
  std::vector<std::shared_ptr<DatumAble>> datums_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_WRITE_DATA_H_