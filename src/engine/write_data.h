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
#include <memory>
#include <string>
#include <vector>

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
  kMetaPut = 7,
  kRebuildVectorIndex = 8,
  kSaveRaftSnapshot = 9,
  kTxn = 7,
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
      put_request->add_kvs()->Swap(&kv);
    }

    return request;
  };

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  std::string cf_name;
  std::vector<pb::common::KeyValue> kvs;
};

struct MetaPutDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kMetaPut; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::META_WRITE);

    pb::raft::RaftMetaRequest* meta_request = request->mutable_meta_req();
    auto* meta_increment_request = meta_request->mutable_meta_increment();
    meta_increment_request->Swap(&meta_increment);
    return request;
  };

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  std::string cf_name;
  pb::coordinator_internal::MetaIncrement meta_increment;
};

struct VectorAddDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kPut; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::VECTOR_ADD);
    pb::raft::VectorAddRequest* vector_add_request = request->mutable_vector_add();
    for (auto& vector : vectors) {
      vector_add_request->set_cf_name(cf_name);
      vector_add_request->add_vectors()->Swap(&vector);
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
  std::vector<int64_t> ids;
};

// TxnDatum
struct TxnDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kTxn; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::TXN);

    pb::raft::TxnRaftRequest* txn_request = request->mutable_txn_raft_req();
    txn_request->Swap(&txn_request_to_raft);
    return request;
  };

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  pb::raft::TxnRaftRequest txn_request_to_raft;
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
      put_if_absent_request->add_kvs()->Swap(&kv);
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
      *(compare_and_set_request->add_kvs()) = kv;
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
    for (auto& key : keys) {
      delete_batch_request->add_keys()->swap(key);
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
      *(delete_range_request->add_ranges()) = range;
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
    *(split_request->mutable_epoch()) = epoch;
    split_request->set_split_strategy(split_strategy);

    return request;
  };

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  int64_t from_region_id;
  int64_t to_region_id;
  std::string split_key;
  pb::common::RegionEpoch epoch;
  pb::raft::SplitStrategy split_strategy;
};

struct RebuildVectorIndexDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kRebuildVectorIndex; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::REBUILD_VECTOR_INDEX);
    auto* rebuild_request = request->mutable_rebuild_vector_index();

    return request;
  };

  void TransformFromRaft(pb::raft::Response& resonse) override {}
};

struct SaveRaftSnapshotDatum : public DatumAble {
  DatumType GetType() override { return DatumType::kSaveRaftSnapshot; }

  pb::raft::Request* TransformToRaft() override {
    auto* request = new pb::raft::Request();

    request->set_cmd_type(pb::raft::CmdType::SAVE_RAFT_SNAPSHOT);
    request->mutable_save_snapshot()->set_region_id(region_id);

    return request;
  };

  void TransformFromRaft(pb::raft::Response& resonse) override {}

  int64_t region_id;
};

class WriteData {
 public:
  std::vector<std::shared_ptr<DatumAble>> Datums() const { return datums_; }
  void AddDatums(std::shared_ptr<DatumAble> datum) { datums_.push_back(datum); }

 private:
  std::vector<std::shared_ptr<DatumAble>> datums_;
};

// Build WriteData struct.
class WriteDataBuilder {
 public:
  // PutDatum
  static std::shared_ptr<WriteData> BuildWrite(const std::string& cf_name,
                                               const std::vector<pb::common::KeyValue>& kvs) {
    auto datum = std::make_shared<PutDatum>();
    datum->cf_name = cf_name;
    datum->kvs = kvs;

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // VectorAddDatum
  static std::shared_ptr<WriteData> BuildWrite(const std::string& cf_name,
                                               const std::vector<pb::common::VectorWithId>& vectors) {
    auto datum = std::make_shared<VectorAddDatum>();
    datum->cf_name = cf_name;
    datum->vectors = vectors;

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // VectorDeleteDatum
  static std::shared_ptr<WriteData> BuildWrite(const std::string& cf_name, const std::vector<int64_t>& ids) {
    auto datum = std::make_shared<VectorDeleteDatum>();
    datum->cf_name = cf_name;
    datum->ids = ids;

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // PutIfAbsentDatum
  static std::shared_ptr<WriteData> BuildWrite(const std::string& cf_name, const std::vector<pb::common::KeyValue>& kvs,
                                               bool is_atomic) {
    auto datum = std::make_shared<PutIfAbsentDatum>();
    datum->cf_name = cf_name;
    datum->kvs = kvs;
    datum->is_atomic = is_atomic;

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // DeleteBatchDatum
  static std::shared_ptr<WriteData> BuildWrite(const std::string& cf_name, const std::vector<std::string>& keys) {
    auto datum = std::make_shared<DeleteBatchDatum>();
    datum->cf_name = cf_name;
    datum->keys = std::move(const_cast<std::vector<std::string>&>(keys));  // NOLINT

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // DeleteRangeDatum
  static std::shared_ptr<WriteData> BuildWrite(const std::string& cf_name, const pb::common::Range& range) {
    auto datum = std::make_shared<DeleteRangeDatum>();
    datum->cf_name = cf_name;
    datum->ranges.emplace_back(std::move(const_cast<pb::common::Range&>(range)));

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // CompareAndSetDatum
  static std::shared_ptr<WriteData> BuildWrite(const std::string& cf_name, const std::vector<pb::common::KeyValue>& kvs,
                                               const std::vector<std::string>& expect_values, bool is_atomic) {
    auto datum = std::make_shared<CompareAndSetDatum>();
    datum->cf_name = cf_name;
    datum->kvs = kvs;
    datum->expect_values = expect_values;
    datum->is_atomic = is_atomic;

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // MetaPutDatum
  static std::shared_ptr<WriteData> BuildWrite(const std::string& cf_name,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
    auto datum = std::make_shared<MetaPutDatum>();
    datum->cf_name = cf_name;
    datum->meta_increment.Swap(&meta_increment);

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // TxnDatum
  static std::shared_ptr<WriteData> BuildWrite(pb::raft::TxnRaftRequest& txn_raft_request) {
    auto datum = std::make_shared<TxnDatum>();
    datum->txn_request_to_raft.Swap(&txn_raft_request);

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // SplitDatum
  static std::shared_ptr<WriteData> BuildWrite(const pb::coordinator::SplitRequest& split_request,
                                               const pb::common::RegionEpoch& epoch) {
    auto datum = std::make_shared<SplitDatum>();
    datum->from_region_id = split_request.split_from_region_id();
    datum->to_region_id = split_request.split_to_region_id();
    datum->split_key = split_request.split_watershed_key();
    datum->epoch = epoch;
    if (split_request.store_create_region()) {
      datum->split_strategy = pb::raft::POST_CREATE_REGION;
    } else {
      datum->split_strategy = pb::raft::PRE_CREATE_REGION;
    }

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // RebuildVectorIndexDatum
  static std::shared_ptr<WriteData> BuildWrite() {
    auto datum = std::make_shared<RebuildVectorIndexDatum>();

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }

  // SaveRaftSnapshotDatum
  static std::shared_ptr<WriteData> BuildWrite(int64_t region_id) {
    auto datum = std::make_shared<SaveRaftSnapshotDatum>();
    datum->region_id = region_id;

    auto write_data = std::make_shared<WriteData>();
    write_data->AddDatums(std::static_pointer_cast<DatumAble>(datum));

    return write_data;
  }
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_WRITE_DATA_H_