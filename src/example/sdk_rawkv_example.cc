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

#include <unistd.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "common/logging.h"
#include "glog/logging.h"
#include "sdk/client.h"
#include "sdk/coordinator_proxy.h"
#include "sdk/meta_cache.h"
#include "sdk/status.h"

using dingodb::sdk::MetaCache;
using dingodb::sdk::Region;
using dingodb::sdk::Status;

DEFINE_string(coordinator_url, "", "coordinator url");

// TODO: remove
static std::shared_ptr<dingodb::CoordinatorInteraction> g_coordinator_interaction;

static std::shared_ptr<dingodb::sdk::CoordinatorProxy> g_coordinator_proxy;

static std::shared_ptr<dingodb::sdk::Client> g_client;

static std::vector<int64_t> g_region_ids;

void CreateRegion(std::string name, std::string start_key, std::string end_key, int replicas = 3,
                  dingodb::sdk::EngineType engine_type = dingodb::sdk::kLSM) {
  CHECK(!name.empty()) << "name should not empty";
  CHECK(!start_key.empty()) << "start_key should not empty";
  CHECK(!end_key.empty()) << "end_key should not empty";
  CHECK(start_key < end_key) << "start_key must < end_key";
  CHECK(replicas > 0) << "replicas must > 0";

  dingodb::sdk::RegionCreator* tmp_creator;
  Status built = g_client->NewRegionCreator(&tmp_creator);
  CHECK(built.IsOK()) << "dingo creator build fail";
  CHECK_NOTNULL(tmp_creator);
  std::shared_ptr<dingodb::sdk::RegionCreator> creator(tmp_creator);

  int64_t region_id = -1;
  Status tmp = creator->SetRegionName(name)
                   .SetRange(start_key, end_key)
                   .SetEngineType(engine_type)
                   .SetReplicaNum(replicas)
                   .Wait(true)
                   .Create(region_id);
  DINGO_LOG(INFO) << "Create region status: " << tmp.ToString() << ", region_id:" << region_id;

  if (tmp.ok()) {
    CHECK(region_id > 0);
    bool inprogress = true;
    g_client->IsCreateRegionInProgress(region_id, inprogress);
    CHECK(!inprogress);
    g_region_ids.push_back(region_id);
  }
}

void PostClean() {
  for (const auto region_id : g_region_ids) {
    Status tmp = g_client->DropRegion(region_id);
    DINGO_LOG(INFO) << "drop region status: " << tmp.ToString() << ", region_id:" << region_id;
    bool inprogress = true;
    tmp = g_client->IsCreateRegionInProgress(region_id, inprogress);
    DINGO_LOG(INFO) << "query region status: " << tmp.ToString() << ", region_id:" << region_id;
  }
  g_region_ids.clear();
}

void MetaCacheExample() {
  auto meta_cache = std::make_shared<MetaCache>(g_coordinator_proxy);

  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey("wb", region);
  DINGO_LOG(INFO) << got.ToString() << ", " << (got.IsOK() ? region->ToString() : "null");
  CHECK(got.IsOK());

  got = meta_cache->LookupRegionByKey("wc00000000", region);
  DINGO_LOG(INFO) << got.ToString();
  CHECK(got.IsOK());

  got = meta_cache->LookupRegionByKey("wz00000000", region);
  DINGO_LOG(INFO) << got.ToString();
  CHECK(got.IsNotFound());

  meta_cache->Dump();
}

void RawKVExample() {
  dingodb::sdk::RawKV* tmp;
  Status built = g_client->NewRawKV(&tmp);
  CHECK(built.IsOK()) << "dingo raw_kv build fail";
  CHECK_NOTNULL(tmp);
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv(tmp);

  {
    // put/get/delete
    std::string key = "wb01";
    std::string value = "pong";
    Status put = raw_kv->Put(key, value);
    DINGO_LOG(INFO) << "raw_kv put:" << put.ToString();

    std::string to_get;
    Status got = raw_kv->Get(key, to_get);
    DINGO_LOG(INFO) << "raw_kv get:" << got.ToString() << ", value:" << to_get;
    if (got.IsOK()) {
      CHECK_EQ(value, to_get);
    }

    Status del = raw_kv->Delete(key);
    DINGO_LOG(INFO) << "raw_kv delete:" << del.ToString();
    if (del.IsOK()) {
      std::string tmp;
      got = raw_kv->Get(key, tmp);
      DINGO_LOG(INFO) << "raw_kv get after delete:" << got.ToString() << ", value:" << tmp;
      CHECK(tmp.empty());
    }
  }

  std::vector<std::string> keys;
  keys.push_back("wb01");
  keys.push_back("wc01");
  keys.push_back("wd01");
  keys.push_back("wf01");
  keys.push_back("wl01");
  keys.push_back("wm01");

  std::vector<std::string> values;
  values.push_back("rwb01");
  values.push_back("rwc01");
  values.push_back("rwd01");
  values.push_back("rwf01");
  values.push_back("rl01");
  values.push_back("rm01");

  {
    // batch put/batch get/batch delete
    std::vector<dingodb::sdk::KVPair> kvs;
    kvs.reserve(keys.size());
    for (auto i = 0; i < keys.size(); i++) {
      kvs.push_back({keys[i], values[i]});
    }

    Status result = raw_kv->BatchPut(kvs);
    DINGO_LOG(INFO) << "raw_kv batch_put:" << result.ToString();

    std::vector<dingodb::sdk::KVPair> batch_get_values;
    result = raw_kv->BatchGet(keys, batch_get_values);
    DINGO_LOG(INFO) << "raw_kv batch_get:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& kv : batch_get_values) {
        DINGO_LOG(INFO) << "raw_kv batch_get key:" << kv.key << ", value:" << kv.value;
      }
    }

    result = raw_kv->BatchDelete(keys);
    DINGO_LOG(INFO) << "raw_kv batch_delete:" << result.ToString();

    std::vector<dingodb::sdk::KVPair> tmp_batch_get_values;
    result = raw_kv->BatchGet(keys, tmp_batch_get_values);
    DINGO_LOG(INFO) << "raw_kv batch_get after batch delete:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& kv : tmp_batch_get_values) {
        DINGO_LOG(INFO) << "raw_kv batch_get after delete, key:" << kv.key << ", value:" << kv.value;
      }

      CHECK_EQ(0, tmp_batch_get_values.size());
    }
  }

  {
    // put if absent
    std::string key = "wb01";
    std::string value = "pong";

    bool state;
    Status result = raw_kv->PutIfAbsent(key, value, state);
    DINGO_LOG(INFO) << "raw_kv put_if_absent:" << result.ToString() << "; state:" << (state ? "true" : "false");

    std::string to_get;
    result = raw_kv->Get(key, to_get);
    DINGO_LOG(INFO) << "raw_kv get after put_if_absent:" << result.ToString() << ", value:" << to_get;
    if (result.IsOK()) {
      CHECK_EQ(value, to_get);
    }

    bool again_state;
    result = raw_kv->PutIfAbsent(key, value, again_state);
    DINGO_LOG(INFO) << "raw_kv put_if_absent again:" << result.ToString()
                    << "; state:" << (again_state ? "true" : "false");

    result = raw_kv->Delete(key);
    DINGO_LOG(INFO) << "raw_kv delete:" << result.ToString();
    if (result.IsOK()) {
      std::string tmp;
      result = raw_kv->Get(key, tmp);
      DINGO_LOG(INFO) << "raw_kv get after delete:" << result.ToString() << ", value:" << tmp;
      CHECK(tmp.empty());
    }
  }

  {
    // batch put if absent
    std::vector<dingodb::sdk::KVPair> kvs;
    kvs.reserve(keys.size());
    for (auto i = 0; i < keys.size(); i++) {
      kvs.push_back({keys[i], values[i]});
    }

    std::vector<dingodb::sdk::KeyOpState> keys_state;
    Status result = raw_kv->BatchPutIfAbsent(kvs, keys_state);
    DINGO_LOG(INFO) << "raw_kv batch_put_if_absent:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& key_state : keys_state) {
        DINGO_LOG(INFO) << "raw_kv batch_put_if_absent, key:" << key_state.key
                        << ", state:" << (key_state.state ? "true" : "false");
      }
    }

    std::vector<dingodb::sdk::KVPair> batch_get_values;
    result = raw_kv->BatchGet(keys, batch_get_values);
    DINGO_LOG(INFO) << "raw_kv batch_get after batch_put_if_absent:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& kv : batch_get_values) {
        DINGO_LOG(INFO) << "raw_kv batch_get after batch_put_if_absent:" << kv.key << ", value:" << kv.value;
      }
    }

    std::vector<dingodb::sdk::KeyOpState> again_keys_state;
    result = raw_kv->BatchPutIfAbsent(kvs, again_keys_state);
    DINGO_LOG(INFO) << "raw_kv batch_put_if_absent again:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& key_state : again_keys_state) {
        DINGO_LOG(INFO) << "raw_kv batch_put_if_absent again, key:" << key_state.key
                        << ", state:" << (key_state.state ? "true" : "false");
      }
    }

    result = raw_kv->BatchDelete(keys);
    DINGO_LOG(INFO) << "raw_kv batch_delete:" << result.ToString();

    std::vector<dingodb::sdk::KVPair> tmp_batch_get_values;
    result = raw_kv->BatchGet(keys, tmp_batch_get_values);
    DINGO_LOG(INFO) << "raw_kv batch_get after batch delete:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& kv : tmp_batch_get_values) {
        DINGO_LOG(INFO) << "raw_kv batch_get after delete, key:" << kv.key << ", value:" << kv.value;
      }
      CHECK_EQ(0, tmp_batch_get_values.size());
    }
  }

  {
    // delete range
    std::vector<dingodb::sdk::KVPair> kvs;
    kvs.reserve(keys.size());
    for (auto i = 0; i < keys.size(); i++) {
      kvs.push_back({keys[i], values[i]});
    }

    Status result = raw_kv->BatchPut(kvs);
    DINGO_LOG(INFO) << "raw_kv batch_put:" << result.ToString();

    std::vector<dingodb::sdk::KVPair> batch_get_values;
    result = raw_kv->BatchGet(keys, batch_get_values);
    DINGO_LOG(INFO) << "raw_kv batch_get:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& kv : batch_get_values) {
        DINGO_LOG(INFO) << "raw_kv batch_get key:" << kv.key << ", value:" << kv.value;
      }
    }

    int64_t delete_count = 0;
    result = raw_kv->DeleteRange("wb01", "wz01", delete_count);
    DINGO_LOG(INFO) << "raw_kv delete range:" << result.ToString();

    result = raw_kv->DeleteRangeNonContinuous("wb01", "wz01", delete_count);
    DINGO_LOG(INFO) << "raw_kv delete range non continuous:" << result.ToString();

    std::vector<dingodb::sdk::KVPair> tmp_batch_get_values;
    result = raw_kv->BatchGet(keys, tmp_batch_get_values);
    DINGO_LOG(INFO) << "raw_kv batch_get after delete_range:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& kv : tmp_batch_get_values) {
        DINGO_LOG(INFO) << "raw_kv batch_get after delete_range, key:" << kv.key << ", value:" << kv.value;
      }
    }
  }

  {
    // compare and set
    std::string key = "wb01";
    std::string value = "pong";

    bool state;
    Status result = raw_kv->CompareAndSet(key, value, "", state);
    DINGO_LOG(INFO) << "raw_kv compare_and_set:" << result.ToString() << " key:" << key << " value:" << value
                    << " expect:empty"
                    << " state:" << (state ? "true" : "false");

    std::string to_get;
    result = raw_kv->Get(key, to_get);
    DINGO_LOG(INFO) << "raw_kv get after compare_and_set:" << result.ToString() << ", value:" << to_get;
    if (result.IsOK()) {
      CHECK_EQ(value, to_get);
    }

    bool again_state;
    result = raw_kv->CompareAndSet(key, "ping", value, again_state);
    DINGO_LOG(INFO) << "raw_kv compare_and_set again:" << result.ToString() << " key:" << key << " value:ping"
                    << " expect:" << value << " state:" << (again_state ? "true" : "false");

    std::string again_get;
    result = raw_kv->Get(key, again_get);
    DINGO_LOG(INFO) << "raw_kv get after compare_and_set again:" << result.ToString() << ", value:" << again_get;
    if (result.IsOK()) {
      CHECK_EQ("ping", again_get);
    }

    result = raw_kv->Delete(key);
    DINGO_LOG(INFO) << "raw_kv delete:" << result.ToString();
    if (result.IsOK()) {
      std::string tmp;
      result = raw_kv->Get(key, tmp);
      DINGO_LOG(INFO) << "raw_kv get after delete:" << result.ToString() << ", value:" << tmp;
      CHECK(tmp.empty());
    }
  }

  {
    // batch compare and set
    {
      // first batch_compare_and_set
      Status result;
      std::vector<dingodb::sdk::KVPair> kvs;
      std::vector<std::string> expect_values;

      kvs.reserve(keys.size());
      for (auto i = 0; i < keys.size(); i++) {
        kvs.push_back({keys[i], values[i]});
      }

      expect_values.resize(kvs.size(), "");

      std::vector<dingodb::sdk::KeyOpState> keys_state;
      result = raw_kv->BatchCompareAndSet(kvs, expect_values, keys_state);
      DINGO_LOG(INFO) << "raw_kv batch_compare_and_set:" << result.ToString();

      if (result.IsOK()) {
        for (const auto& key_state : keys_state) {
          DINGO_LOG(INFO) << "raw_kv batch_compare_and_set, key:" << key_state.key
                          << ", state:" << (key_state.state ? "true" : "false");
          CHECK(key_state.state);
        }
      }

      std::vector<dingodb::sdk::KVPair> batch_get_values;
      result = raw_kv->BatchGet(keys, batch_get_values);
      DINGO_LOG(INFO) << "raw_kv batch_get after batch_compare_and_set:" << result.ToString();
      if (result.IsOK()) {
        for (const auto& kv : batch_get_values) {
          DINGO_LOG(INFO) << "raw_kv batch_get after batch_compare_and_set key:" << kv.key << ", value:" << kv.value;
          bool find = false;
          for (const auto& ele : kvs) {
            if (ele.key == kv.key) {
              CHECK_EQ(ele.key, kv.key);
              CHECK_EQ(ele.value, kv.value);
              find = true;
            }
          }
          CHECK(find);
        }
      }
    }

    {
      // batch_compare_and_set again
      std::vector<dingodb::sdk::KVPair> kvs;
      std::vector<std::string> expect_values;

      kvs.reserve(keys.size());
      for (auto& key : keys) {
        kvs.push_back({key, "ping"});
      }

      expect_values.reserve(values.size());
      for (auto& value : values) {
        expect_values.push_back(value);
      }

      CHECK_EQ(kvs.size(), expect_values.size());

      std::vector<dingodb::sdk::KeyOpState> again_keys_state;
      Status result = raw_kv->BatchCompareAndSet(kvs, expect_values, again_keys_state);
      DINGO_LOG(INFO) << "raw_kv batch_compare_and_set again:" << result.ToString();
      if (result.IsOK()) {
        for (const auto& key_state : again_keys_state) {
          DINGO_LOG(INFO) << "raw_kv batch_put_if_absent again, key:" << key_state.key
                          << ", state:" << (key_state.state ? "true" : "false");
          CHECK(key_state.state);
        }
      }

      std::vector<dingodb::sdk::KVPair> batch_get_values;
      result = raw_kv->BatchGet(keys, batch_get_values);
      DINGO_LOG(INFO) << "raw_kv batch_get after batch_compare_and_set again:" << result.ToString();
      if (result.IsOK()) {
        for (const auto& kv : batch_get_values) {
          DINGO_LOG(INFO) << "raw_kv batch_get after batch_compare_and_set again key:" << kv.key
                          << ", value:" << kv.value;
          bool find = false;
          for (const auto& ele : kvs) {
            if (ele.key == kv.key) {
              CHECK_EQ(ele.key, kv.key);
              CHECK_EQ(ele.value, kv.value);
              find = true;
            }
          }
          CHECK(find);
        }
      }
    }

    Status result = raw_kv->BatchDelete(keys);
    DINGO_LOG(INFO) << "raw_kv batch_delete:" << result.ToString();

    std::vector<dingodb::sdk::KVPair> tmp_batch_get_values;
    result = raw_kv->BatchGet(keys, tmp_batch_get_values);
    DINGO_LOG(INFO) << "raw_kv batch_get after batch delete:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& kv : tmp_batch_get_values) {
        DINGO_LOG(INFO) << "raw_kv batch_get after delete, key:" << kv.key << ", value:" << kv.value;
      }
      CHECK_EQ(0, tmp_batch_get_values.size());
    }
  }

  {
    // scan
    std::vector<dingodb::sdk::KVPair> kvs;
    kvs.reserve(keys.size());
    for (auto i = 0; i < keys.size(); i++) {
      kvs.push_back({keys[i], values[i]});
    }

    Status result = raw_kv->BatchPut(kvs);
    DINGO_LOG(INFO) << "raw_kv batch_put before scan:" << result.ToString();

    std::vector<dingodb::sdk::KVPair> batch_get_values;
    result = raw_kv->BatchGet(keys, batch_get_values);
    DINGO_LOG(INFO) << "raw_kv batch_get before scan:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& kv : batch_get_values) {
        DINGO_LOG(INFO) << "raw_kv batch_get before scan key:" << kv.key << ", value:" << kv.value;
      }
    }

    std::vector<dingodb::sdk::KVPair> scan_values;
    result = raw_kv->Scan("wa00000000", "wz00000000", 0, scan_values);
    DINGO_LOG(INFO) << "raw_kv scan:" << result.ToString();
    if (result.IsOK()) {
      for (const auto& kv : scan_values) {
        DINGO_LOG(INFO) << "raw_kv scan key:" << kv.key << ", value:" << kv.value;
      }
    }
  }
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_coordinator_url.empty()) {
    DINGO_LOG(ERROR) << "coordinator url is empty, try to use file://./coor_list";
    FLAGS_coordinator_url = "file://./coor_list";
  }

  CHECK(!FLAGS_coordinator_url.empty());
  g_coordinator_interaction = std::make_shared<dingodb::CoordinatorInteraction>();
  if (!g_coordinator_interaction->InitByNameService(
          FLAGS_coordinator_url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeCoordinator)) {
    DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << FLAGS_coordinator_url;
    return -1;
  }

  g_coordinator_proxy = std::make_shared<dingodb::sdk::CoordinatorProxy>();
  Status open = g_coordinator_proxy->Open(FLAGS_coordinator_url);
  if (!open.IsOK()) {
    DINGO_LOG(ERROR) << "Fail to open coordinator_proxy, please check parameter --url=" << FLAGS_coordinator_url;
    return -1;
  }

  dingodb::sdk::Client* tmp;
  Status built = dingodb::sdk::Client::Build(FLAGS_coordinator_url, &tmp);
  if (!built.ok()) {
    DINGO_LOG(ERROR) << "Fail to build client, please check parameter --url=" << FLAGS_coordinator_url;
    return -1;
  }
  CHECK_NOTNULL(tmp);
  g_client.reset(tmp);

  {
    CreateRegion("skd_example01", "wa00000000", "wc00000000", 3);
    CreateRegion("skd_example02", "wc00000000", "we00000000", 3);
    CreateRegion("skd_example03", "we00000000", "wg00000000", 3);

    CreateRegion("skd_example04", "wl00000000", "wn00000000", 3);

    MetaCacheExample();

    RawKVExample();
    PostClean();
  }

  {
    CreateRegion("skd_example01", "wa00000000", "wc00000000", 3, dingodb::sdk::kBTree);
    CreateRegion("skd_example02", "wc00000000", "we00000000", 3, dingodb::sdk::kBTree);
    CreateRegion("skd_example03", "we00000000", "wg00000000", 3, dingodb::sdk::kBTree);

    CreateRegion("skd_example04", "wl00000000", "wn00000000", 3, dingodb::sdk::kBTree);

    MetaCacheExample();

    RawKVExample();
    PostClean();
  }
}