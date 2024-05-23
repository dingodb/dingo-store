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
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "glog/logging.h"
#include "sdk/client.h"
#include "sdk/status.h"

using dingodb::sdk::Status;

DEFINE_string(coordinator_url, "", "coordinator url");
DEFINE_int64(scan_test_count, 5000, "scan test count");
DEFINE_string(range_start, "xa00000000", "range start key");
DEFINE_string(range_end, "xc00000000", "range end key");
DEFINE_bool(do_put, true, "do put operation");
DEFINE_bool(do_scan, true, "do scan operation");
DEFINE_bool(do_clean, true, "do clean operation");
DEFINE_bool(use_btree, false, "use btree engine");

static std::shared_ptr<dingodb::sdk::Client> g_client;

static std::vector<int64_t> g_region_ids;

static std::vector<std::string> keys;
static std::vector<std::string> values;
static std::unordered_map<std::string, std::string> key_values;
static void PrepareTxnData() {
  for (int32_t i = 0; i < FLAGS_scan_test_count; i++) {
    keys.push_back(FLAGS_range_start + std::to_string(i));
    values.push_back(std::to_string(i));
  }

  for (auto i = 0; i < keys.size(); i++) {
    key_values.emplace(std::make_pair(keys[i], values[i]));
  }
}

static void CreateRegion(std::string name, std::string start_key, std::string end_key, int replicas = 3) {
  CHECK(!name.empty()) << "name should not empty";
  CHECK(!start_key.empty()) << "start_key should not empty";
  CHECK(!end_key.empty()) << "end_key should not empty";
  CHECK(start_key < end_key) << "start_key must < end_key";
  CHECK(replicas > 0) << "replicas must > 0";

  dingodb::sdk::RegionCreator* tmp_creator;
  Status built = g_client->NewRegionCreator(&tmp_creator);
  CHECK(built.IsOK()) << "dingo creator build fail";
  std::shared_ptr<dingodb::sdk::RegionCreator> creator(tmp_creator);
  CHECK_NOTNULL(creator.get());

  int64_t region_id = -1;
  auto engine_type = dingodb::sdk::EngineType::kLSM;
  if (FLAGS_use_btree) {
    engine_type = dingodb::sdk::EngineType::kBTree;
  }

  Status tmp = creator->SetRegionName(name)
                   .SetEngineType(engine_type)
                   .SetRange(start_key, end_key)
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

static void PostClean() {
  for (const auto region_id : g_region_ids) {
    Status tmp = g_client->DropRegion(region_id);
    DINGO_LOG(INFO) << "drop region status: " << tmp.ToString() << ", region_id:" << region_id;
    bool inprogress = true;
    tmp = g_client->IsCreateRegionInProgress(region_id, inprogress);
    DINGO_LOG(INFO) << "query region status: " << tmp.ToString() << ", region_id:" << region_id;
  }
}

static std::shared_ptr<dingodb::sdk::Transaction> NewOptimisticTransaction(dingodb::sdk::TransactionIsolation isolation,
                                                                           uint32_t keep_alive_ms = 0) {
  dingodb::sdk::TransactionOptions options;
  options.isolation = isolation;
  options.kind = dingodb::sdk::kOptimistic;
  options.keep_alive_ms = keep_alive_ms;

  dingodb::sdk::Transaction* tmp;
  Status built = g_client->NewTransaction(options, &tmp);
  CHECK(built.ok()) << "dingo txn build fail";
  std::shared_ptr<dingodb::sdk::Transaction> txn(tmp);
  CHECK_NOTNULL(txn.get());
  return txn;
}

void OptimisticTxnPut() {
  // write data into store
  auto txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);
  if (FLAGS_do_put) {
    auto start_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    for (const auto& [key, value] : key_values) {
      txn->Put(key, value);
    }

    DINGO_LOG(INFO) << "prepare data time cost ms: "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                               .count() -
                           start_time_ms;

    Status precommit = txn->PreCommit();
    DINGO_LOG(INFO) << "precommit:" << precommit.ToString();
    DINGO_LOG(INFO) << "prewrite data time cost ms: "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                               .count() -
                           start_time_ms;

    Status commit = txn->Commit();
    DINGO_LOG(INFO) << "txn commit:" << commit.ToString();

    DINGO_LOG(INFO) << "commit time cost ms: "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                               .count() -
                           start_time_ms;
  }
}

void OptimisticTxnScan() {
  auto read_commit_txn = NewOptimisticTransaction(dingodb::sdk::kReadCommitted);
  auto start_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();

  // readCommiited should read txn commit data
  std::vector<dingodb::sdk::KVPair> kvs;
  Status scan = read_commit_txn->Scan(FLAGS_range_start, FLAGS_range_end, 0, kvs);
  DINGO_LOG(INFO) << "read_commit_txn scan:" << scan.ToString();

  Status precommit = read_commit_txn->PreCommit();
  DINGO_LOG(INFO) << "read_commit_txn precommit:" << precommit.ToString();
  Status commit = read_commit_txn->Commit();
  DINGO_LOG(INFO) << "read_commit_txn commit:" << commit.ToString();

  DINGO_LOG(INFO) << " time cost ms: "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                             .count() -
                         start_time_ms;
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  // FLAGS_v = dingodb::kGlobalValueOfDebug;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_coordinator_url.empty()) {
    DINGO_LOG(ERROR) << "coordinator url is empty, try to use file://./coor_list";
    FLAGS_coordinator_url = "file://./coor_list";
  }

  dingodb::sdk::Client* tmp;
  Status built = dingodb::sdk::Client::Build(FLAGS_coordinator_url, &tmp);
  if (!built.ok()) {
    DINGO_LOG(ERROR) << "Fail to build client, please check parameter --url=" << FLAGS_coordinator_url;
    return -1;
  }
  CHECK_NOTNULL(tmp);
  g_client.reset(tmp);

  if (FLAGS_do_put) {
    PrepareTxnData();

    CreateRegion("skd_example01", FLAGS_range_start, FLAGS_range_end, 3);

    OptimisticTxnPut();
  }

  if (FLAGS_do_scan) {
    OptimisticTxnScan();
  }

  if (FLAGS_do_clean) {
    PostClean();
  }
}
