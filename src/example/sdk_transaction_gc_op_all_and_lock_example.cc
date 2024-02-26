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

static std::shared_ptr<dingodb::sdk::Client> g_client;

static std::vector<int64_t> g_region_ids;

static std::vector<std::string> keys;
static std::vector<std::string> values;
static std::unordered_map<std::string, std::string> key_values;
static void PrepareTxnData() {
  keys.push_back("xa01");
  keys.push_back("xa02");
  keys.push_back("xa03");

  keys.push_back("xb01");
  keys.push_back("xb02");
  keys.push_back("xb03");

  keys.push_back("xc01");
  keys.push_back("xd01");
  keys.push_back("xf01");
  keys.push_back("xl01");
  keys.push_back("xm01");

  values.push_back("rxa01");
  values.push_back("rxa02");
  values.push_back("rxa03");

  values.push_back("rxb01");
  values.push_back("rxb02");
  values.push_back("rxb03");

  values.push_back("rxc01");
  values.push_back("rxd01");
  values.push_back("rxf01");
  values.push_back("rxl01");
  values.push_back("rxm01");

  assert(keys.size() == values.size());

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
  Status tmp =
      creator->SetRegionName(name).SetRange(start_key, end_key).SetReplicaNum(replicas).Wait(true).Create(region_id);
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

static void OptimisticTxnPostClean(dingodb::sdk::TransactionIsolation isolation) {
  {
    auto post_clean_txn = NewOptimisticTransaction(isolation);

    Status s = post_clean_txn->BatchDelete(keys);
    CHECK(s.ok());

    Status precommit = post_clean_txn->PreCommit();
    DINGO_LOG(INFO) << "post_clean_txn precommit:" << precommit.ToString();
    Status commit = post_clean_txn->Commit();
    DINGO_LOG(INFO) << "post_clean_txn commit:" << commit.ToString();
  }

  {
    auto post_clean_check_txn = NewOptimisticTransaction(isolation);
    {
      std::vector<dingodb::sdk::KVPair> kvs;
      Status got = post_clean_check_txn->BatchGet(keys, kvs);
      DINGO_LOG(INFO) << "post_clean_check_txn batch get:" << got.ToString();
      CHECK(got.ok());
      CHECK_EQ(kvs.size(), 0);
    }
  }
}

void OptimisticTxnBatch() {
  auto txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);

  // for (const auto& key : keys) {
  //   std::string tmp;
  //   Status got = txn->Get(key, tmp);
  //   CHECK(got.IsNotFound());
  // }

  // {
  //   std::vector<dingodb::sdk::KVPair> kvs;
  //   Status got = txn->BatchGet(keys, kvs);
  //   CHECK(got.ok());
  //   CHECK_EQ(kvs.size(), 0);
  // }

  {
    std::vector<dingodb::sdk::KVPair> kvs;
    kvs.reserve(keys.size());
    for (auto i = 0; i < keys.size(); i++) {
      kvs.push_back({keys[i], values[i]});
    }

    {
      // batch put
      Status s = txn->BatchPut(kvs);
      CHECK(s.ok());

      std::vector<dingodb::sdk::KVPair> tmp;
      s = txn->BatchGet(keys, tmp);
      CHECK(s.ok());
      CHECK_EQ(tmp.size(), kvs.size());
      for (const auto& kv : tmp) {
        CHECK_EQ(kv.value, key_values[kv.key]);
      }
    }

    //   {
    //     // batch put if absent
    //     Status s = txn->BatchPutIfAbsent(kvs);
    //     CHECK(s.ok());

    //     std::vector<dingodb::sdk::KVPair> tmp;
    //     s = txn->BatchGet(keys, tmp);
    //     CHECK(s.ok());
    //     CHECK_EQ(tmp.size(), kvs.size());
    //     for (const auto& kv : tmp) {
    //       CHECK_EQ(kv.value, key_values[kv.key]);
    //     }
    //   }

    //   {
    //     // batch delete
    //     Status s = txn->BatchDelete(keys);
    //     CHECK(s.ok());

    //     std::vector<dingodb::sdk::KVPair> tmp;
    //     s = txn->BatchGet(keys, tmp);
    //     CHECK(s.ok());
    //     CHECK_EQ(tmp.size(), 0);
    //   }

    //   {
    //     // batch put if absent again
    //     Status s = txn->BatchPutIfAbsent(kvs);
    //     CHECK(s.ok());

    //     std::vector<dingodb::sdk::KVPair> tmp;
    //     s = txn->BatchGet(keys, tmp);
    //     CHECK(s.ok());
    //     CHECK_EQ(tmp.size(), kvs.size());
    //     for (const auto& kv : tmp) {
    //       CHECK_EQ(kv.value, key_values[kv.key]);
    //     }
    //   }

    //   {
    //     // batch put override exist kvs, then batch delete
    //     std::vector<dingodb::sdk::KVPair> new_kvs;
    //     new_kvs.reserve(keys.size());
    //     for (auto& key : keys) {
    //       new_kvs.push_back({key, key});
    //     }

    //     Status s = txn->BatchPut(new_kvs);
    //     CHECK(s.ok());

    //     std::vector<dingodb::sdk::KVPair> tmp;
    //     s = txn->BatchGet(keys, tmp);
    //     CHECK(s.ok());
    //     CHECK_EQ(tmp.size(), new_kvs.size());
    //     for (const auto& kv : tmp) {
    //       CHECK_EQ(kv.value, kv.key);
    //     }

    //     s = txn->BatchDelete(keys);
    //     CHECK(s.ok());

    //     s = txn->BatchGet(keys, tmp);
    //     CHECK(s.ok());
    //     CHECK_EQ(tmp.size(), 0);
    //   }
  }

  Status precommit = txn->PreCommit();
  DINGO_LOG(INFO) << "precommit:" << precommit.ToString();
  Status commit = txn->Commit();
  DINGO_LOG(INFO) << "commit:" << commit.ToString();

  OptimisticTxnPostClean(dingodb::sdk::kSnapshotIsolation);
}

void OptimisticTxnSingleOp() {
  {
    dingodb::sdk::TransactionOptions options;
    options.isolation = dingodb::sdk::kSnapshotIsolation;
    options.kind = dingodb::sdk::kOptimistic;

    std::string put_key = "xb01";
    std::string put_if_absent_key = "xc01";
    std::string delete_key = "xd01";

    {
      auto txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);
      {
        txn->Put(put_key, key_values[put_key]);
        txn->PutIfAbsent(put_if_absent_key, key_values[put_if_absent_key]);
        txn->Delete(delete_key);

        Status precommit = txn->PreCommit();
        DINGO_LOG(INFO) << "precommit:" << precommit.ToString();
        Status commit = txn->Commit();
        DINGO_LOG(INFO) << "commit:" << commit.ToString();
      }
    }

    {
      dingodb::sdk::Transaction* tmp;
      Status built = g_client->NewTransaction(options, &tmp);
      CHECK(built.ok()) << "dingo txn build fail";
      std::shared_ptr<dingodb::sdk::Transaction> txn(tmp);
      CHECK_NOTNULL(txn.get());

      std::vector<dingodb::sdk::KVPair> kvs;
      Status got = txn->BatchGet(keys, kvs);
      DINGO_LOG(INFO) << "batch get:" << got.ToString();
      CHECK(got.ok());
      CHECK_EQ(kvs.size(), 2);

      for (const auto& kv : kvs) {
        CHECK(kv.key == put_key || kv.key == put_if_absent_key);
        if (kv.key == put_key) {
          CHECK_EQ(kv.value, key_values[put_key]);
        } else if (kv.key == put_if_absent_key) {
          CHECK_EQ(kv.value, key_values[put_if_absent_key]);
        }
      }

      Status precommit = txn->PreCommit();
      DINGO_LOG(INFO) << "precommit:" << precommit.ToString();
      Status commit = txn->Commit();
      DINGO_LOG(INFO) << "commit:" << commit.ToString();
    }

    OptimisticTxnPostClean(dingodb::sdk::kSnapshotIsolation);
  }
}

void OptimisticTxnLockConflict() {
  std::string put_key = "xa01";
  std::string put_if_absent_key = "xa02";
  std::string delete_key = "xa03";

  std::string put_value = "rxa01";
  std::string put_if_absent_value = "rxa02";

  auto txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);
  {
    txn->Put(put_key, put_value);
    txn->PutIfAbsent(put_if_absent_key, put_if_absent_value);
    txn->Delete(delete_key);

    Status precommit = txn->PreCommit();
    DINGO_LOG(INFO) << "precommit:" << precommit.ToString();
  }
}

void OptimisticTxnReadSnapshotAndReadCommiited() {
  std::string put_key = "xb01";
  std::string put_if_absent_key = "xc01";
  std::string delete_key = "xd01";

  auto txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);
  {
    std::vector<dingodb::sdk::KVPair> kvs;
    Status got = txn->BatchGet(keys, kvs);
    DINGO_LOG(INFO) << "batch get:" << got.ToString();
    CHECK(got.ok());
    CHECK_EQ(kvs.size(), 0);

    txn->Put(put_key, key_values[put_key]);
    txn->PutIfAbsent(put_if_absent_key, key_values[put_if_absent_key]);
    txn->Delete(delete_key);

    Status precommit = txn->PreCommit();
    DINGO_LOG(INFO) << "precommit:" << precommit.ToString();
  }

  auto read_commit_txn = NewOptimisticTransaction(dingodb::sdk::kReadCommitted);

  auto new_txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);
  {
    std::vector<dingodb::sdk::KVPair> kvs;
    Status got = new_txn->BatchGet(keys, kvs);
    DINGO_LOG(INFO) << "batch get:" << got.ToString();
    CHECK(got.IsTxnLockConflict());
  }

  {
    Status commit = txn->Commit();
    DINGO_LOG(INFO) << "txn commit:" << commit.ToString();
  }

  {
    // snapshot read nothing
    std::vector<dingodb::sdk::KVPair> kvs;
    Status got = new_txn->BatchGet(keys, kvs);
    DINGO_LOG(INFO) << "batch get:" << got.ToString();
    CHECK(got.ok());
    CHECK_EQ(kvs.size(), 0);
    Status precommit = new_txn->PreCommit();
    DINGO_LOG(INFO) << "new_txn precommit:" << precommit.ToString();
    Status commit = new_txn->Commit();
    DINGO_LOG(INFO) << "new_txn commit:" << commit.ToString();
  }

  {
    // readCommiited should read txn commit data
    std::vector<dingodb::sdk::KVPair> kvs;
    Status got = read_commit_txn->BatchGet(keys, kvs);
    DINGO_LOG(INFO) << "batch get:" << got.ToString();
    CHECK(got.ok());
    CHECK_EQ(kvs.size(), 2);

    for (const auto& kv : kvs) {
      CHECK(kv.key == put_key || kv.key == put_if_absent_key);
      if (kv.key == put_key) {
        CHECK_EQ(kv.value, key_values[put_key]);
      } else if (kv.key == put_if_absent_key) {
        CHECK_EQ(kv.value, key_values[put_if_absent_key]);
      }
    }

    Status precommit = read_commit_txn->PreCommit();
    DINGO_LOG(INFO) << "read_commit_txn precommit:" << precommit.ToString();
    Status commit = read_commit_txn->Commit();
    DINGO_LOG(INFO) << "read_commit_txn commit:" << commit.ToString();
  }

  OptimisticTxnPostClean(dingodb::sdk::kSnapshotIsolation);
}

void OptimisticTxnRollback() {
  std::string put_key = "xb01";
  std::string put_if_absent_key = "xb02";
  std::string delete_key = "xb03";

  std::string put_value = "rxa01";
  std::string put_if_absent_value = "rxa02";

  auto txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);
  {
    txn->Put(put_key, put_value);
    txn->PutIfAbsent(put_if_absent_key, put_if_absent_value);
    txn->Delete(delete_key);

    Status precommit = txn->PreCommit();
    DINGO_LOG(INFO) << "precommit:" << precommit.ToString();
  }

  {
    Status rollback = txn->Rollback();
    DINGO_LOG(INFO) << "txn rollback:" << rollback.ToString();
  }
}

void OptimisticTxnScan() {
  std::string put_key = "xb01";
  std::string put_if_absent_key = "xc01";
  std::string delete_key = "xd01";

  auto txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);
  {
    std::vector<dingodb::sdk::KVPair> kvs;
    Status got = txn->BatchGet(keys, kvs);
    DINGO_LOG(INFO) << "batch get:" << got.ToString();
    CHECK(got.ok());
    CHECK_EQ(kvs.size(), 0);

    txn->Put(put_key, key_values[put_key]);
    txn->PutIfAbsent(put_if_absent_key, key_values[put_if_absent_key]);
    txn->Delete(delete_key);

    Status precommit = txn->PreCommit();
    DINGO_LOG(INFO) << "precommit:" << precommit.ToString();
    Status commit = txn->Commit();
    DINGO_LOG(INFO) << "txn commit:" << commit.ToString();
  }

  auto read_commit_txn = NewOptimisticTransaction(dingodb::sdk::kReadCommitted);
  {
    // readCommiited should read txn commit data
    std::vector<dingodb::sdk::KVPair> kvs;
    Status got = read_commit_txn->BatchGet(keys, kvs);
    DINGO_LOG(INFO) << "batch get:" << got.ToString();
    CHECK(got.ok());
    CHECK_EQ(kvs.size(), 2);

    for (const auto& kv : kvs) {
      CHECK(kv.key == put_key || kv.key == put_if_absent_key);
      DINGO_LOG(INFO) << "batch get, key:" << kv.key << ",value:" << kv.value;
      if (kv.key == put_key) {
        CHECK_EQ(kv.value, key_values[put_key]);
      } else if (kv.key == put_if_absent_key) {
        CHECK_EQ(kv.value, key_values[put_if_absent_key]);
      }
    }
  }

  {
    // readCommiited should read txn commit data
    std::vector<dingodb::sdk::KVPair> kvs;
    Status scan = read_commit_txn->Scan("xa00000000", "xz00000000", 0, kvs);
    DINGO_LOG(INFO) << "read_commit_txn scan:" << scan.ToString();
    CHECK(scan.ok());
    if (kvs.size() != 2) {
      DINGO_LOG(WARNING) << "Internal error, expected kvs size:" << 2 << ", ectual:" << kvs.size();
    }
    CHECK_EQ(kvs.size(), 2);

    for (const auto& kv : kvs) {
      CHECK(kv.key == put_key || kv.key == put_if_absent_key);
      if (kv.key == put_key) {
        CHECK_EQ(kv.value, key_values[put_key]);
      } else if (kv.key == put_if_absent_key) {
        CHECK_EQ(kv.value, key_values[put_if_absent_key]);
      }
    }

    Status precommit = read_commit_txn->PreCommit();
    DINGO_LOG(INFO) << "read_commit_txn precommit:" << precommit.ToString();
    Status commit = read_commit_txn->Commit();
    DINGO_LOG(INFO) << "read_commit_txn commit:" << commit.ToString();
  }

  OptimisticTxnPostClean(dingodb::sdk::kSnapshotIsolation);
}

void OptimisticTxnScanReadSelf() {
  std::string put_key = "xb01";
  std::string put_if_absent_key = "xc01";
  std::string delete_key = "xd01";
  std::string put_keyf = "xf01";
  std::string put_keyl = "xl01";

  std::set<std::string> to_check;
  to_check.emplace(put_key);
  to_check.emplace(put_if_absent_key);
  to_check.emplace(delete_key);
  to_check.emplace(put_keyf);
  to_check.emplace(put_keyl);

  auto txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);
  {
    std::vector<dingodb::sdk::KVPair> kvs;
    Status got = txn->BatchGet(keys, kvs);
    DINGO_LOG(INFO) << "batch get:" << got.ToString();
    CHECK(got.ok());
    CHECK_EQ(kvs.size(), 0);

    txn->Put(put_key, key_values[put_key]);
    txn->PutIfAbsent(put_if_absent_key, key_values[put_if_absent_key]);
    txn->Delete(delete_key);
    txn->Put(put_keyf, key_values[put_keyf]);

    Status precommit = txn->PreCommit();
    DINGO_LOG(INFO) << "precommit:" << precommit.ToString();
    Status commit = txn->Commit();
    DINGO_LOG(INFO) << "txn commit:" << commit.ToString();
  }

  auto read_commit_txn = NewOptimisticTransaction(dingodb::sdk::kReadCommitted);
  {
    std::string self_put_key = "xb02";
    std::string self_put_if_absent_key = "xc02";
    std::string self_delete_key = "xd02";

    to_check.emplace(self_put_key);
    to_check.emplace(self_put_if_absent_key);
    to_check.emplace(self_delete_key);
    {
      {
        // overwrite
        read_commit_txn->Put(put_key, put_key);
        read_commit_txn->PutIfAbsent(put_if_absent_key, put_if_absent_key);
        read_commit_txn->Delete(delete_key);
        read_commit_txn->Delete(put_keyl);
      }

      read_commit_txn->Put(self_put_key, self_put_key);
      read_commit_txn->PutIfAbsent(self_put_if_absent_key, self_put_if_absent_key);
      read_commit_txn->Delete(self_delete_key);
    }

    {
      // scan without limit
      std::vector<dingodb::sdk::KVPair> kvs;
      Status scan = read_commit_txn->Scan("xa00000000", "xz00000000", 0, kvs);
      DINGO_LOG(INFO) << "read_commit_txn scan:" << scan.ToString();
      CHECK(scan.ok());
      for (const auto& kv : kvs) {
        DINGO_LOG(INFO) << "read_commit_txn scan key:" << kv.key << ", value:" << kv.value;
      }
      if (kvs.size() != 5) {
        DINGO_LOG(WARNING) << "Internal error, expected kvs size:" << 2 << ", ectual:" << kvs.size();
      }
      CHECK_EQ(kvs.size(), 5);

      for (const auto& kv : kvs) {
        if (kv.key != put_if_absent_key && kv.key != put_keyf) {
          CHECK_EQ(kv.value, kv.key);
        } else {
          CHECK_EQ(kv.value, key_values[kv.key]);
        }
        to_check.erase(kv.key);
      }
      CHECK_EQ(to_check.size(), 3);
      CHECK(to_check.find(delete_key) != to_check.cend());
      CHECK(to_check.find(self_delete_key) != to_check.cend());
      CHECK(to_check.find(put_keyl) != to_check.cend());
    }

    {
      // scan without limit
      int limit = 2;
      std::vector<dingodb::sdk::KVPair> kvs;
      Status scan = read_commit_txn->Scan("xa00000000", "xz00000000", limit, kvs);
      DINGO_LOG(INFO) << "read_commit_txn scan:" << scan.ToString();
      CHECK(scan.ok());
      for (const auto& kv : kvs) {
        DINGO_LOG(INFO) << "read_commit_txn scan key:" << kv.key << ", value:" << kv.value;
      }
      CHECK_EQ(kvs.size(), 2);
      // TODO: check key prefix is xb
    }

    Status precommit = read_commit_txn->PreCommit();
    DINGO_LOG(INFO) << "read_commit_txn precommit:" << precommit.ToString();
    Status commit = read_commit_txn->Commit();
    DINGO_LOG(INFO) << "read_commit_txn commit:" << commit.ToString();
  }

  {
    std::vector<std::string> keys(to_check.begin(), to_check.end());
    {
      auto clean_txn = NewOptimisticTransaction(dingodb::sdk::kSnapshotIsolation);
      Status s = clean_txn->BatchDelete(keys);
      CHECK(s.ok());

      Status precommit = clean_txn->PreCommit();
      DINGO_LOG(INFO) << "clean_txn precommit:" << precommit.ToString();
      Status commit = clean_txn->Commit();
      DINGO_LOG(INFO) << "clean_txn commit:" << commit.ToString();
    }

    {
      auto clean_check_txn = NewOptimisticTransaction(dingodb::sdk::kReadCommitted);
      std::vector<dingodb::sdk::KVPair> kvs;
      Status got = clean_check_txn->BatchGet(keys, kvs);
      DINGO_LOG(INFO) << "clean_check_txn batch get:" << got.ToString();
      CHECK(got.ok());
      CHECK_EQ(kvs.size(), 0);
    }
  }

  OptimisticTxnPostClean(dingodb::sdk::kSnapshotIsolation);
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

  CreateRegion("skd_example01", "xa00000000", "xc00000000", 1);
  CreateRegion("skd_example02", "xc00000000", "xe00000000", 1);
  CreateRegion("skd_example03", "xe00000000", "xg00000000", 1);

  CreateRegion("skd_example04", "xl00000000", "xn00000000", 1);

  PrepareTxnData();

  OptimisticTxnBatch();
  // OptimisticTxnSingleOp();
  OptimisticTxnLockConflict();
  // OptimisticTxnReadSnapshotAndReadCommiited();
  OptimisticTxnRollback();
  // OptimisticTxnScan();
  // OptimisticTxnScanReadSelf();

  // PostClean();
}