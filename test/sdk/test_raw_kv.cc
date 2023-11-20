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
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.h"
#include "common.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "proto/error.pb.h"
#include "store_rpc.h"
#include "test_base.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {
class RawKVTest : public TestBase {
 public:
  RawKVTest() = default;

  ~RawKVTest() override = default;

  void SetUp() override {
    TestBase::SetUp();
    Status kv = NewRawKV(raw_kv);
    CHECK(kv.IsOK());
  }

  void TearDown() override { raw_kv.reset(); }

  std::shared_ptr<RawKV> raw_kv;
};

TEST_F(RawKVTest, Get) {
  std::string key = "b";
  std::string value;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(key, region).IsOK());
  CHECK_NOTNULL(region.get());

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
    CHECK_NOTNULL(kv_get_rpc);

    EXPECT_TRUE(kv_get_rpc->Request()->has_context());
    auto context = kv_get_rpc->Request()->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

    kv_get_rpc->MutableResponse()->set_value("pong");
    return Status::OK();
  });

  Status got = raw_kv->Get(key, value);
  EXPECT_TRUE(got.IsOK());
  EXPECT_EQ(value, "pong");
}

TEST_F(RawKVTest, BatchGetSuccess) {
  std::vector<std::string> keys;
  keys.emplace_back("b");
  keys.emplace_back("d");
  keys.emplace_back("f");

  std::vector<KVPair> kvs;

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* batch_get_rpc = dynamic_cast<KvBatchGetRpc*>(&rpc);
    CHECK_NOTNULL(batch_get_rpc);
    CHECK(batch_get_rpc->Request()->has_context());

    EXPECT_EQ(1, batch_get_rpc->Request()->keys_size());
    const auto& key = batch_get_rpc->Request()->keys(0);
    if (key == "b") {
      auto* kv = batch_get_rpc->MutableResponse()->add_kvs();
      kv->set_key("b");
      kv->set_value("b");
    } else if (key == "d") {
      auto* kv = batch_get_rpc->MutableResponse()->add_kvs();
      kv->set_key("d");
      kv->set_value("d");
    } else if (key == "f") {
      auto* kv = batch_get_rpc->MutableResponse()->add_kvs();
      kv->set_key("f");
      kv->set_value("f");
    }
    return Status::OK();
  });

  Status got = raw_kv->BatchGet(keys, kvs);
  EXPECT_TRUE(got.IsOK());
  EXPECT_EQ(keys.size(), kvs.size());

  for (const auto& kv : kvs) {
    EXPECT_EQ(kv.key, kv.value);
  }
}

TEST_F(RawKVTest, BatchGetPartialFail) {
  std::vector<std::string> keys;
  keys.emplace_back("b");
  keys.emplace_back("d");
  keys.emplace_back("f");

  std::vector<KVPair> kvs;

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* batch_get_rpc = dynamic_cast<KvBatchGetRpc*>(&rpc);
    CHECK_NOTNULL(batch_get_rpc);
    CHECK(batch_get_rpc->Request()->has_context());

    EXPECT_EQ(1, batch_get_rpc->Request()->keys_size());
    const auto& key = batch_get_rpc->Request()->keys(0);
    if (key == "b") {
      auto* kv = batch_get_rpc->MutableResponse()->add_kvs();
      kv->set_key("b");
      kv->set_value("b");
    } else if (key == "d") {
      auto* kv = batch_get_rpc->MutableResponse()->add_kvs();
      kv->set_key("d");
      kv->set_value("d");
      auto* error = batch_get_rpc->MutableResponse()->mutable_error();
      error->set_errcode(pb::error::EINTERNAL);
    } else if (key == "f") {
      auto* kv = batch_get_rpc->MutableResponse()->add_kvs();
      kv->set_key("f");
      kv->set_value("f");
    }
    return Status::OK();
  });

  Status got = raw_kv->BatchGet(keys, kvs);
  EXPECT_FALSE(got.IsOK());
  EXPECT_EQ(2, kvs.size());

  for (const auto& kv : kvs) {
    EXPECT_EQ(kv.key, kv.value);
  }
}

TEST_F(RawKVTest, BatchGetAllFail) {
  std::vector<std::string> keys;
  keys.emplace_back("b");
  keys.emplace_back("d");
  keys.emplace_back("f");

  std::vector<KVPair> kvs;

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* batch_get_rpc = dynamic_cast<KvBatchGetRpc*>(&rpc);
    CHECK_NOTNULL(batch_get_rpc);
    CHECK(batch_get_rpc->Request()->has_context());

    EXPECT_EQ(1, batch_get_rpc->Request()->keys_size());
    const auto& key = batch_get_rpc->Request()->keys(0);
    if (key == "b") {
      auto* kv = batch_get_rpc->MutableResponse()->add_kvs();
      kv->set_key("b");
      kv->set_value("b");
      auto* error = batch_get_rpc->MutableResponse()->mutable_error();
      error->set_errcode(pb::error::EINTERNAL);
    } else if (key == "d") {
      auto* kv = batch_get_rpc->MutableResponse()->add_kvs();
      kv->set_key("d");
      kv->set_value("d");
      auto* error = batch_get_rpc->MutableResponse()->mutable_error();
      error->set_errcode(pb::error::EINTERNAL);
    } else if (key == "f") {
      auto* kv = batch_get_rpc->MutableResponse()->add_kvs();
      kv->set_key("f");
      kv->set_value("f");
      auto* error = batch_get_rpc->MutableResponse()->mutable_error();
      error->set_errcode(pb::error::EINTERNAL);
    }
    return Status::OK();
  });

  Status got = raw_kv->BatchGet(keys, kvs);
  EXPECT_FALSE(got.IsOK());
  EXPECT_EQ(0, kvs.size());

  for (const auto& kv : kvs) {
    EXPECT_EQ(kv.key, kv.value);
  }
}

TEST_F(RawKVTest, Put) {
  std::string key = "d";
  std::string value = "pong";

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(key, region).IsOK());
  CHECK_NOTNULL(region.get());

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_put_rpc = dynamic_cast<KvPutRpc*>(&rpc);
    CHECK_NOTNULL(kv_put_rpc);

    EXPECT_TRUE(kv_put_rpc->Request()->has_context());
    auto context = kv_put_rpc->Request()->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

    auto kv = kv_put_rpc->MutableRequest()->kv();
    EXPECT_EQ(kv.key(), key);
    EXPECT_EQ(kv.value(), value);
    return Status::OK();
  });

  Status put = raw_kv->Put(key, value);
  EXPECT_TRUE(put.IsOK());
}

TEST_F(RawKVTest, BatchPutSuccess) {
  std::vector<KVPair> kvs;
  kvs.push_back({"b", "b"});
  kvs.push_back({"d", "d"});
  kvs.push_back({"f", "f"});

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_batch_put_rpc = dynamic_cast<KvBatchPutRpc*>(&rpc);
    CHECK_NOTNULL(kv_batch_put_rpc);

    CHECK(kv_batch_put_rpc->Request()->has_context());
    auto context = kv_batch_put_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    EXPECT_EQ(1, kv_batch_put_rpc->Request()->kvs_size());

    for (const auto& kv : kv_batch_put_rpc->Request()->kvs()) {
      EXPECT_EQ(kv.key(), kv.value());
    }

    return Status::OK();
  });
  Status put = raw_kv->BatchPut(kvs);
  EXPECT_TRUE(put.IsOK());
}

TEST_F(RawKVTest, BatchPutPartialFail) {
  std::vector<KVPair> kvs;
  kvs.push_back({"b", "b"});
  kvs.push_back({"d", "d"});
  kvs.push_back({"f", "f"});

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_batch_put_rpc = dynamic_cast<KvBatchPutRpc*>(&rpc);
    CHECK_NOTNULL(kv_batch_put_rpc);

    CHECK(kv_batch_put_rpc->Request()->has_context());
    auto context = kv_batch_put_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    EXPECT_EQ(1, kv_batch_put_rpc->Request()->kvs_size());

    for (const auto& kv : kv_batch_put_rpc->Request()->kvs()) {
      EXPECT_EQ(kv.key(), kv.value());
      if (kv.key() == "d") {
        auto* error = kv_batch_put_rpc->MutableResponse()->mutable_error();
        error->set_errcode(pb::error::EINTERNAL);
      }
    }

    return Status::OK();
  });

  Status put = raw_kv->BatchPut(kvs);
  EXPECT_FALSE(put.IsOK());
}

TEST_F(RawKVTest, PutIfAbsent) {
  std::string key = "d";
  std::string value = "d";

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvPutIfAbsentRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    auto kv = kv_rpc->MutableRequest()->kv();
    EXPECT_EQ(kv.key(), key);
    EXPECT_EQ(kv.value(), value);

    kv_rpc->MutableResponse()->set_key_state(true);
    return Status::OK();
  });

  bool state;
  Status put = raw_kv->PutIfAbsent(key, value, state);
  EXPECT_TRUE(put.IsOK());
  EXPECT_TRUE(state);
}

TEST_F(RawKVTest, BatchPutIfAbsentSuccess) {
  std::vector<KVPair> kvs;
  kvs.push_back({"b", "b"});
  kvs.push_back({"d", "d"});
  kvs.push_back({"f", "f"});

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvBatchPutIfAbsentRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    CHECK(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    EXPECT_EQ(1, kv_rpc->Request()->kvs_size());

    for (const auto& kv : kv_rpc->Request()->kvs()) {
      EXPECT_EQ(kv.key(), kv.value());
      kv_rpc->MutableResponse()->add_key_states(true);
    }

    return Status::OK();
  });

  std::vector<KeyOpState> states;
  EXPECT_TRUE(raw_kv->BatchPutIfAbsent(kvs, states).IsOK());
  EXPECT_EQ(kvs.size(), states.size());
  for (const auto& state : states) {
    EXPECT_TRUE(state.state);
  }
}

TEST_F(RawKVTest, BatchPutIfAbsentPartialFail) {
  std::vector<KVPair> kvs;
  kvs.push_back({"b", "b"});
  kvs.push_back({"d", "d"});
  kvs.push_back({"f", "f"});

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvBatchPutIfAbsentRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    CHECK(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    EXPECT_EQ(1, kv_rpc->Request()->kvs_size());

    for (const auto& kv : kv_rpc->Request()->kvs()) {
      EXPECT_EQ(kv.key(), kv.value());
      if (kv.key() == "f") {
        kv_rpc->MutableResponse()->add_key_states(false);
        auto* error = kv_rpc->MutableResponse()->mutable_error();
        error->set_errcode(pb::error::EINTERNAL);
      } else {
        kv_rpc->MutableResponse()->add_key_states(true);
      }
    }

    return Status::OK();
  });

  std::vector<KeyOpState> states;
  EXPECT_FALSE(raw_kv->BatchPutIfAbsent(kvs, states).IsOK());
  for (const auto& state : states) {
    if (state.key == "f") {
      EXPECT_FALSE(state.state);
    } else {
      EXPECT_TRUE(state.state);
    }
  }
}

TEST_F(RawKVTest, Delete) {
  std::string key = "d";

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvBatchDeleteRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    EXPECT_EQ(1, kv_rpc->MutableRequest()->keys_size());
    EXPECT_EQ(key, kv_rpc->MutableRequest()->keys(0));
    return Status::OK();
  });

  EXPECT_TRUE(raw_kv->Delete(key).IsOK());
}

TEST_F(RawKVTest, BatchDeleteSuccess) {
  std::vector<std::string> to_delete;
  to_delete.push_back("b");
  to_delete.push_back("d");
  to_delete.push_back("f");

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvBatchDeleteRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    for (const auto& key : kv_rpc->Request()->keys()) {
      EXPECT_TRUE(key == "b" || key == "d" || key == "f");
    }

    return Status::OK();
  });

  EXPECT_TRUE(raw_kv->BatchDelete(to_delete).IsOK());
}

TEST_F(RawKVTest, BatchDeletePartialFail) {
  std::vector<std::string> to_delete;
  to_delete.push_back("b");
  to_delete.push_back("d");
  to_delete.push_back("f");

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvBatchDeleteRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    for (const auto& key : kv_rpc->Request()->keys()) {
      EXPECT_TRUE(key == "b" || key == "d" || key == "f");
      if (key == "b") {
        auto* error = kv_rpc->MutableResponse()->mutable_error();
        error->set_errcode(pb::error::EINTERNAL);
      }
    }

    return Status::OK();
  });

  EXPECT_FALSE(raw_kv->BatchDelete(to_delete).IsOK());
}

TEST_F(RawKVTest, DeleteRangeInOneRegion) {
  std::string start = "b";
  std::string end = "c";

  int64_t count = 100;

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvDeleteRangeRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    auto range_with_option = kv_rpc->Request()->range();
    const auto& range = range_with_option.range();
    EXPECT_EQ(start, range.start_key());
    EXPECT_EQ(end, range.end_key());

    EXPECT_TRUE(range_with_option.with_start());
    EXPECT_FALSE(range_with_option.with_end());

    kv_rpc->MutableResponse()->set_delete_count(count);

    return Status::OK();
  });

  int64_t delete_count;

  EXPECT_TRUE(raw_kv->DeleteRange(start, end, delete_count).IsOK());
  EXPECT_EQ(count, delete_count);
}

TEST_F(RawKVTest, DeleteRangeInOneRegionWithoutStartKey) {
  std::string start = "b";
  std::string end = "c";

  int64_t count = 100;

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvDeleteRangeRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    auto range_with_option = kv_rpc->Request()->range();
    const auto& range = range_with_option.range();

    EXPECT_EQ(start, range.start_key());
    EXPECT_EQ(end, range.end_key());

    EXPECT_FALSE(range_with_option.with_start());
    EXPECT_FALSE(range_with_option.with_end());

    kv_rpc->MutableResponse()->set_delete_count(0);

    return Status::OK();
  });

  int64_t delete_count;

  EXPECT_TRUE(raw_kv->DeleteRange(start, end, delete_count, false).IsOK());
  EXPECT_EQ(0, delete_count);
}

TEST_F(RawKVTest, DeleteRangeInTwoRegion) {
  std::string start = "b";
  std::string end = "d";

  int64_t count = 100;

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvDeleteRangeRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    auto range_with_option = kv_rpc->Request()->range();
    const auto& range = range_with_option.range();
    if (range.start_key() == start) {
      EXPECT_EQ(range.end_key(), "c");
      EXPECT_TRUE(range_with_option.with_start());
      EXPECT_FALSE(range_with_option.with_end());
    }
    if (range.end_key() == end) {
      EXPECT_EQ(range.start_key(), "c");
      EXPECT_TRUE(range_with_option.with_start());
      EXPECT_FALSE(range_with_option.with_end());
    }

    kv_rpc->MutableResponse()->set_delete_count(count);

    return Status::OK();
  });

  int64_t delete_count;

  EXPECT_TRUE(raw_kv->DeleteRange(start, end, delete_count).IsOK());
  EXPECT_EQ(2 * count, delete_count);
}

TEST_F(RawKVTest, DeleteRangeInTwoRegionWithoutStartKey) {
  std::string start = "b";
  std::string end = "d";

  int64_t count = 100;

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvDeleteRangeRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    auto range_with_option = kv_rpc->Request()->range();
    const auto& range = range_with_option.range();
    if (range.start_key() == start) {
      EXPECT_EQ(range.end_key(), "c");
      EXPECT_FALSE(range_with_option.with_start());
      EXPECT_FALSE(range_with_option.with_end());
      kv_rpc->MutableResponse()->set_delete_count(0);
    }
    if (range.end_key() == end) {
      EXPECT_EQ(range.start_key(), "c");
      EXPECT_TRUE(range_with_option.with_start());
      EXPECT_FALSE(range_with_option.with_end());
      kv_rpc->MutableResponse()->set_delete_count(count);
    }

    return Status::OK();
  });

  int64_t delete_count;

  EXPECT_TRUE(raw_kv->DeleteRange(start, end, delete_count, false).IsOK());
  EXPECT_EQ(count, delete_count);
}

TEST_F(RawKVTest, DeleteRangeInThressRegionWithEndkey) {
  std::string start = "b";
  std::string end = "e";

  int64_t count = 100;

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvDeleteRangeRpc*>(&rpc);
    if (kv_rpc == nullptr) {
      auto* kv_rpc = dynamic_cast<KvBatchDeleteRpc*>(&rpc);
      CHECK_NOTNULL(kv_rpc);

      EXPECT_TRUE(kv_rpc->Request()->has_context());
      auto context = kv_rpc->Request()->context();
      EXPECT_TRUE(context.has_region_epoch());

      EXPECT_EQ(1, kv_rpc->MutableRequest()->keys_size());
      EXPECT_EQ("e", kv_rpc->MutableRequest()->keys(0));

      return Status::OK();
    } else {
      CHECK_NOTNULL(kv_rpc);

      EXPECT_TRUE(kv_rpc->Request()->has_context());
      auto context = kv_rpc->Request()->context();
      EXPECT_TRUE(context.has_region_epoch());

      auto range_with_option = kv_rpc->Request()->range();
      const auto& range = range_with_option.range();
      EXPECT_TRUE(range.start_key() == start || range.start_key() == "c");
      if (range.start_key() == start) {
        EXPECT_EQ(range.end_key(), "c");
        EXPECT_TRUE(range_with_option.with_start());
        EXPECT_FALSE(range_with_option.with_end());
        kv_rpc->MutableResponse()->set_delete_count(count);
      }
      if (range.start_key() == "c") {
        EXPECT_EQ(range.end_key(), "e");
        EXPECT_TRUE(range_with_option.with_start());
        EXPECT_FALSE(range_with_option.with_end());
        kv_rpc->MutableResponse()->set_delete_count(count);
      }
    }

    return Status::OK();
  });

  int64_t delete_count;

  EXPECT_TRUE(raw_kv->DeleteRange(start, end, delete_count, true, true).IsOK());
  EXPECT_EQ(2 * count + 1, delete_count);
}

TEST_F(RawKVTest, DeleteRangeInThressRegionWithoutStartKeyWithEndkey) {
  std::string start = "a";
  std::string end = "e";

  int64_t count = 100;

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvDeleteRangeRpc*>(&rpc);
    if (kv_rpc == nullptr) {
      auto* kv_rpc = dynamic_cast<KvBatchDeleteRpc*>(&rpc);
      CHECK_NOTNULL(kv_rpc);

      EXPECT_TRUE(kv_rpc->Request()->has_context());
      auto context = kv_rpc->Request()->context();
      EXPECT_TRUE(context.has_region_epoch());

      EXPECT_EQ(1, kv_rpc->MutableRequest()->keys_size());
      EXPECT_EQ("e", kv_rpc->MutableRequest()->keys(0));

      return Status::OK();
    } else {
      CHECK_NOTNULL(kv_rpc);

      EXPECT_TRUE(kv_rpc->Request()->has_context());
      auto context = kv_rpc->Request()->context();
      EXPECT_TRUE(context.has_region_epoch());

      auto range_with_option = kv_rpc->Request()->range();
      const auto& range = range_with_option.range();
      EXPECT_TRUE(range.start_key() == start || range.start_key() == "c");
      if (range.start_key() == start) {
        EXPECT_EQ(range.end_key(), "c");
        EXPECT_FALSE(range_with_option.with_start());
        EXPECT_FALSE(range_with_option.with_end());
        kv_rpc->MutableResponse()->set_delete_count(count);
      }
      if (range.start_key() == "c") {
        EXPECT_EQ(range.end_key(), "e");
        EXPECT_TRUE(range_with_option.with_start());
        EXPECT_FALSE(range_with_option.with_end());
        kv_rpc->MutableResponse()->set_delete_count(count);
      }
    }

    return Status::OK();
  });

  int64_t delete_count;

  EXPECT_TRUE(raw_kv->DeleteRange(start, end, delete_count, false, true).IsOK());
  EXPECT_EQ(2 * count + 1, delete_count);
}

TEST_F(RawKVTest, CompareAndSet) {
  std::string key = "d";
  std::string value = "d";
  std::string expteced = "b";

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(key, region).IsOK());
  CHECK_NOTNULL(region.get());

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvCompareAndSetRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

    auto kv = kv_rpc->Request()->kv();
    EXPECT_EQ(kv.key(), key);
    EXPECT_EQ(kv.value(), value);

    auto expect_value = kv_rpc->Request()->expect_value();

    kv_rpc->MutableResponse()->set_key_state(true);

    EXPECT_EQ(expteced, expect_value);
    return Status::OK();
  });

  bool state = false;
  Status result = raw_kv->CompareAndSet(key, value, expteced, state);
  EXPECT_TRUE(result.IsOK());
  EXPECT_TRUE(state);
}

TEST_F(RawKVTest, BatchCompareAndSetInvalid) {
  std::vector<KVPair> kvs;
  kvs.push_back({"a", "a"});
  kvs.push_back({"d", "d"});
  kvs.push_back({"e", "e"});

  std::vector<std::string> expect_values;

  std::vector<KeyOpState> key_state;
  Status result = raw_kv->BatchCompareAndSet(kvs, expect_values, key_state);
  EXPECT_TRUE(result.IsInvalidArgument());
}

TEST_F(RawKVTest, BatchCompareAndSetSuccess) {
  std::vector<KVPair> kvs;
  kvs.push_back({"a", "a"});
  kvs.push_back({"b", "b"});
  kvs.push_back({"d", "d"});
  kvs.push_back({"e", "e"});

  std::vector<std::string> expect_values;
  expect_values.push_back("z");
  expect_values.push_back("y");
  expect_values.push_back("x");
  expect_values.push_back("w");

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvBatchCompareAndSetRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    EXPECT_EQ(kv_rpc->Request()->kvs_size(), kv_rpc->Request()->expect_values_size());

    for (auto i = 0; i < kv_rpc->Request()->kvs_size(); i++) {
      auto kv = kv_rpc->Request()->kvs(i);
      auto expect = kv_rpc->Request()->expect_values(i);
      if (kv.key() == "a") {
        EXPECT_EQ("a", kv.value());
        EXPECT_EQ("z", expect);
      } else if (kv.key() == "b") {
        EXPECT_EQ("b", kv.value());
        EXPECT_EQ("y", expect);
      } else if (kv.key() == "d") {
        EXPECT_EQ("d", kv.value());
        EXPECT_EQ("x", expect);
      } else if (kv.key() == "e") {
        EXPECT_EQ("e", kv.value());
        EXPECT_EQ("w", expect);
      } else {
        EXPECT_TRUE(false);
      }

      kv_rpc->MutableResponse()->add_key_states(true);
    }

    return Status::OK();
  });

  std::vector<KeyOpState> key_state;
  Status result = raw_kv->BatchCompareAndSet(kvs, expect_values, key_state);
  EXPECT_TRUE(result.IsOK());
  EXPECT_EQ(kvs.size(), key_state.size());
  for (const auto& state : key_state) {
    EXPECT_TRUE(state.state);
  }
}

TEST_F(RawKVTest, BatchCompareAndSetPartialFail) {
  std::vector<KVPair> kvs;
  kvs.push_back({"a", "a"});
  kvs.push_back({"b", "b"});
  kvs.push_back({"d", "d"});
  kvs.push_back({"e", "e"});

  std::vector<std::string> expect_values;
  expect_values.push_back("z");
  expect_values.push_back("y");
  expect_values.push_back("x");
  expect_values.push_back("w");

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvBatchCompareAndSetRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    EXPECT_TRUE(kv_rpc->Request()->has_context());
    auto context = kv_rpc->Request()->context();
    EXPECT_TRUE(context.has_region_epoch());

    EXPECT_EQ(kv_rpc->Request()->kvs_size(), kv_rpc->Request()->expect_values_size());

    bool fail = false;
    for (auto i = 0; i < kv_rpc->Request()->kvs_size(); i++) {
      auto kv = kv_rpc->Request()->kvs(i);
      auto expect = kv_rpc->Request()->expect_values(i);
      if (kv.key() == "a") {
        EXPECT_EQ("a", kv.value());
        EXPECT_EQ("z", expect);
        kv_rpc->MutableResponse()->add_key_states(true);
      } else if (kv.key() == "b") {
        EXPECT_EQ("b", kv.value());
        EXPECT_EQ("y", expect);
        kv_rpc->MutableResponse()->add_key_states(true);
      } else if (kv.key() == "d") {
        EXPECT_EQ("d", kv.value());
        EXPECT_EQ("x", expect);
        kv_rpc->MutableResponse()->add_key_states(false);
        fail = true;
      } else if (kv.key() == "e") {
        EXPECT_EQ("e", kv.value());
        EXPECT_EQ("w", expect);
        kv_rpc->MutableResponse()->add_key_states(true);
      } else {
        EXPECT_TRUE(false);
      }
    }

    if (fail) {
      auto* error = kv_rpc->MutableResponse()->mutable_error();
      error->set_errcode(pb::error::EINTERNAL);
    }

    return Status::OK();
  });

  std::vector<KeyOpState> key_state;
  Status result = raw_kv->BatchCompareAndSet(kvs, expect_values, key_state);
  EXPECT_FALSE(result.IsOK());

  for (const auto& state : key_state) {
    if (state.key != "d") {
      EXPECT_TRUE(state.state);
    } else {
      EXPECT_FALSE(state.state);
    }
  }
}

}  // namespace sdk
}  // namespace dingodb