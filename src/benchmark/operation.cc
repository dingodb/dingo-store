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

#include "benchmark/operation.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "common/helper.h"
#include "fmt/core.h"

DEFINE_string(benchmark, "fillseq", "Benchmark type");

DEFINE_uint32(key_size, 64, "Key size");
DEFINE_uint32(value_size, 256, "Value size");

DEFINE_uint32(batch_size, 1, "Batch size");

DEFINE_uint32(arrange_kv_num, 10000, "The number of kv for read");

DEFINE_bool(is_pessimistic_txn, false, "Optimistic or pessimistic transaction");
DEFINE_string(txn_isolation_level, "SI", "Transaction isolation level");
DEFINE_validator(txn_isolation_level, [](const char*, const std::string& value) -> bool {
  auto isolation_level = dingodb::Helper::ToUpper(value);
  return isolation_level == "SI" || isolation_level == "RC";
});

namespace dingodb {
namespace benchmark {

static const std::string kClientRaw = "w";

static const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                                 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
                                 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

using BuildFuncer = std::function<OperationPtr(std::shared_ptr<sdk::Client> client)>;
using OperationBuilderMap = std::map<std::string, BuildFuncer>;

static OperationBuilderMap support_operations = {
    {"fillseq",
     [](std::shared_ptr<sdk::Client> client) -> OperationPtr { return std::make_shared<FillSeqOperation>(client); }},
    {"fillrandom",
     [](std::shared_ptr<sdk::Client> client) -> OperationPtr { return std::make_shared<FillRandomOperation>(client); }},
    {"readseq",
     [](std::shared_ptr<sdk::Client> client) -> OperationPtr { return std::make_shared<ReadSeqOperation>(client); }},
    {"readrandom",
     [](std::shared_ptr<sdk::Client> client) -> OperationPtr { return std::make_shared<ReadRandomOperation>(client); }},
    {"readmissing",
     [](std::shared_ptr<sdk::Client> client) -> OperationPtr {
       return std::make_shared<ReadMissingOperation>(client);
     }},
    {"filltxnseq",
     [](std::shared_ptr<sdk::Client> client) -> OperationPtr { return std::make_shared<FillTxnSeqOperation>(client); }},
    {"filltxnrandom",
     [](std::shared_ptr<sdk::Client> client) -> OperationPtr {
       return std::make_shared<FillTxnRandomOperation>(client);
     }},
};

static sdk::TransactionIsolation GetTxnIsolationLevel() {
  auto isolation_level = dingodb::Helper::ToUpper(FLAGS_txn_isolation_level);
  if (isolation_level == "SI") {
    return sdk::TransactionIsolation::kSnapshotIsolation;
  } else if (isolation_level == "RC") {
    return sdk::TransactionIsolation::kReadCommitted;
  }

  LOG(FATAL) << fmt::format("Not support transaction isolation level: {}", FLAGS_txn_isolation_level);
  return sdk::TransactionIsolation::kReadCommitted;
}

// rand string
static std::string GenRandomString(int len) {
  std::string result;
  int alphabet_len = sizeof(kAlphabet);

  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
  for (int i = 0; i < len; ++i) {
    result.append(1, kAlphabet[distrib(rng) % alphabet_len]);
  }

  return result;
}

static std::string GenSeqString(int num, int len) { return fmt::format("{0:0{1}}", num, len); }

static std::string EncodeRawKey(const std::string& str) { return kClientRaw + str; }

BaseOperation::BaseOperation(std::shared_ptr<sdk::Client> client) : client(client) {
  auto status = client->NewRawKV(raw_kv);
  if (!status.IsOK()) {
    LOG(FATAL) << fmt::format("New RawKv failed, error: {}", status.ToString());
  }
}

Operation::Result BaseOperation::KvPut(const std::string& prefix, bool is_random) {
  Operation::Result result;
  int random_str_len = FLAGS_key_size - prefix.size();

  size_t count = counter.fetch_add(1, std::memory_order_relaxed);
  std::string key;
  if (is_random) {
    key = EncodeRawKey(prefix + GenRandomString(random_str_len));
  } else {
    key = EncodeRawKey(prefix + GenSeqString(count, random_str_len));
  }

  std::string value = GenRandomString(FLAGS_value_size);
  result.write_bytes = key.size() + value.size();

  int64_t start_time = Helper::TimestampUs();

  result.status = raw_kv->Put(key, value);

  result.eplased_time = Helper::TimestampUs() - start_time;

  return result;
}

Operation::Result BaseOperation::KvBatchPut(const std::string& prefix, bool is_random) {
  Operation::Result result;
  int random_str_len = FLAGS_key_size - prefix.size();

  std::vector<sdk::KVPair> kvs;
  for (int i = 0; i < FLAGS_batch_size; ++i) {
    sdk::KVPair kv;
    if (is_random) {
      kv.key = EncodeRawKey(prefix + GenRandomString(random_str_len));
    } else {
      size_t count = counter.fetch_add(1, std::memory_order_relaxed);
      kv.key = EncodeRawKey(prefix + GenSeqString(count, random_str_len));
    }

    kv.value = GenRandomString(FLAGS_value_size);

    kvs.push_back(kv);
    result.write_bytes += kv.key.size() + kv.value.size();
  }

  int64_t start_time = Helper::TimestampUs();

  result.status = raw_kv->BatchPut(kvs);

  result.eplased_time = Helper::TimestampUs() - start_time;

  return result;
}

Operation::Result BaseOperation::KvGet(std::string key) {
  Operation::Result result;

  int64_t start_time = Helper::TimestampUs();

  std::string value;
  result.status = raw_kv->Get(key, value);

  result.read_bytes += value.size();

  result.eplased_time = Helper::TimestampUs() - start_time;

  return result;
}

Operation::Result BaseOperation::KvBatchGet(const std::vector<std::string>& keys) {
  Operation::Result result;

  int64_t start_time = Helper::TimestampUs();

  std::vector<sdk::KVPair> kvs;
  result.status = raw_kv->BatchGet(keys, kvs);

  for (auto& kv : kvs) {
    result.read_bytes += kv.key.size() + kv.value.size();
  }

  result.eplased_time = Helper::TimestampUs() - start_time;

  return result;
}

Operation::Result BaseOperation::KvTxnPut(const std::vector<std::string>& prefixes, bool is_random) {
  Operation::Result result;

  int64_t start_time = Helper::TimestampUs();

  sdk::Transaction* txn = nullptr;
  sdk::TransactionOptions options;
  options.kind = FLAGS_is_pessimistic_txn ? sdk::TransactionKind::kPessimistic : sdk::TransactionKind::kOptimistic;
  options.isolation = GetTxnIsolationLevel();

  result.status = client->NewTransaction(options, &txn);
  if (!result.status.IsOK()) {
    goto end;
  }

  for (const auto& prefix : prefixes) {
    int random_str_len = FLAGS_key_size - prefix.size();

    size_t count = counter.fetch_add(1, std::memory_order_relaxed);
    std::string key = is_random ? EncodeRawKey(prefix + GenRandomString(random_str_len))
                                : EncodeRawKey(prefix + GenSeqString(count, random_str_len));

    std::string value = GenRandomString(FLAGS_value_size);
    result.write_bytes = key.size() + value.size();

    result.status = txn->Put(key, value);
    if (!result.status.IsOK()) {
      goto end;
    }
  }

  result.status = txn->PreCommit();
  if (!result.status.IsOK()) {
    goto end;
  }

  result.status = txn->Commit();

end:
  result.eplased_time = Helper::TimestampUs() - start_time;
  delete txn;

  return result;
}

Operation::Result BaseOperation::KvTxnBatchPut(const std::vector<std::string>& prefixes, bool is_random) {
  Operation::Result result;

  int64_t start_time = Helper::TimestampUs();

  sdk::Transaction* txn = nullptr;
  sdk::TransactionOptions options;
  options.kind = FLAGS_is_pessimistic_txn ? sdk::TransactionKind::kPessimistic : sdk::TransactionKind::kOptimistic;
  options.isolation = GetTxnIsolationLevel();

  result.status = client->NewTransaction(options, &txn);
  if (!result.status.IsOK()) {
    goto end;
  }

  for (const auto& prefix : prefixes) {
    int random_str_len = FLAGS_key_size - prefix.size();

    std::vector<sdk::KVPair> kvs;
    for (int i = 0; i < FLAGS_batch_size; ++i) {
      sdk::KVPair kv;
      size_t count = counter.fetch_add(1, std::memory_order_relaxed);
      kv.key = is_random ? EncodeRawKey(prefix + GenRandomString(random_str_len))
                         : EncodeRawKey(prefix + GenSeqString(count, random_str_len));
      kv.value = GenRandomString(FLAGS_value_size);

      kvs.push_back(kv);
      result.write_bytes += kv.key.size() + kv.value.size();
    }

    result.status = txn->BatchPut(kvs);
    if (!result.status.IsOK()) {
      goto end;
    }
  }

  result.status = txn->PreCommit();
  if (!result.status.IsOK()) {
    goto end;
  }

  result.status = txn->Commit();

end:
  result.eplased_time = Helper::TimestampUs() - start_time;
  delete txn;

  return result;
}

Operation::Result FillSeqOperation::Execute(const std::string& prefix) {
  return FLAGS_batch_size == 1 ? KvPut(prefix, false) : KvBatchPut(prefix, false);
}

Operation::Result FillRandomOperation::Execute(const std::string& prefix) {
  return FLAGS_batch_size == 1 ? KvPut(prefix, true) : KvBatchPut(prefix, true);
}

bool ReadOperation::Arrange(const std::string& prefix) {
  int random_str_len = FLAGS_key_size - prefix.size();

  uint32_t batch_size = 256;
  std::vector<sdk::KVPair> kvs;
  kvs.reserve(batch_size);
  for (uint32_t i = 0; i < FLAGS_arrange_kv_num; ++i) {
    sdk::KVPair kv;
    size_t count = counter.fetch_add(1, std::memory_order_relaxed);
    kv.key = EncodeRawKey(prefix + GenSeqString(count, random_str_len));
    kv.value = GenRandomString(FLAGS_value_size);

    kvs.push_back(kv);
    keys.push_back(kv.key);

    if ((i + 1) % batch_size == 0 || (i + 1 == FLAGS_arrange_kv_num)) {
      auto status = raw_kv->BatchPut(kvs);
      if (!status.ok()) {
        return false;
      }
      kvs.clear();
      std::cout << '\r' << fmt::format("Region({}) put progress [{}%]", prefix, i * 100 / FLAGS_arrange_kv_num)
                << std::flush;
    }
  }

  std::cout << "\r" << fmt::format("Region({}) put ............ done", prefix) << '\n';

  return true;
}

Operation::Result ReadSeqOperation::Execute(const std::string&) { return KvGet(keys[index_++ % keys.size()]); }

Operation::Result ReadRandomOperation::Execute(const std::string&) {
  uint32_t index = Helper::GenerateRealRandomInteger(0, UINT32_MAX) % keys.size();
  return KvGet(keys[index]);
}

Operation::Result ReadMissingOperation::Execute(const std::string& prefix) {
  int random_str_len = FLAGS_key_size + 4 - prefix.size();
  std::string key = EncodeRawKey(prefix + GenRandomString(random_str_len));
  return KvGet(key);
}

Operation::Result FillTxnSeqOperation::Execute(const std::string& prefix) {
  return FLAGS_batch_size == 1 ? KvTxnPut({prefix}, false) : KvTxnBatchPut({prefix}, false);
}

Operation::Result FillTxnSeqOperation::Execute(const std::vector<std::string>& prefixes) {
  return FLAGS_batch_size == 1 ? KvTxnPut(prefixes, false) : KvTxnBatchPut(prefixes, false);
}

Operation::Result FillTxnRandomOperation::Execute(const std::string& prefix) {
  return FLAGS_batch_size == 1 ? KvTxnPut({prefix}, true) : KvTxnBatchPut({prefix}, true);
}

Operation::Result FillTxnRandomOperation::Execute(const std::vector<std::string>& prefixes) {
  return FLAGS_batch_size == 1 ? KvTxnPut(prefixes, true) : KvTxnBatchPut(prefixes, true);
}

bool IsSupportBenchmarkType(const std::string& benchmark) {
  auto it = support_operations.find(benchmark);
  return it != support_operations.end();
}

std::string GetSupportBenchmarkType() {
  std::string benchmarks;
  for (auto& [benchmark, _] : support_operations) {
    benchmarks += benchmark;
    benchmarks += " ";
  }

  return benchmarks;
}

OperationPtr NewOperation(std::shared_ptr<sdk::Client> client) {
  auto it = support_operations.find(FLAGS_benchmark);
  if (it == support_operations.end()) {
    return nullptr;
  }

  return it->second(client);
}

}  // namespace benchmark
}  // namespace dingodb
