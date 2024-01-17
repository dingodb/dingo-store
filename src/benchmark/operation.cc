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

#include "benchmark/color.h"
#include "common/helper.h"
#include "fmt/core.h"

DEFINE_string(benchmark, "fillseq", "Benchmark type");

DEFINE_uint32(key_size, 64, "Key size");
DEFINE_uint32(value_size, 256, "Value size");

DEFINE_uint32(batch_size, 1, "Batch size");

DEFINE_uint32(arrange_kv_num, 10000, "The number of kv for read");

namespace dingodb {
namespace benchmark {

static const std::string kClientRaw = "w";

static const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                                 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
                                 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

using BuildFuncer = std::function<OperationPtr(std::shared_ptr<sdk::Client> client, const std::string& prefix)>;
using OperationBuilderMap = std::map<std::string, BuildFuncer>;

static OperationBuilderMap support_operations = {
    {"fillseq",
     [](std::shared_ptr<sdk::Client> client, const std::string& prefix) -> OperationPtr {
       return std::make_shared<FillSeqOperation>(client, prefix);
     }},
    {"fillrandom",
     [](std::shared_ptr<sdk::Client> client, const std::string& prefix) -> OperationPtr {
       return std::make_shared<FillRandomOperation>(client, prefix);
     }},
    {"readseq",
     [](std::shared_ptr<sdk::Client> client, const std::string& prefix) -> OperationPtr {
       return std::make_shared<ReadSeqOperation>(client, prefix);
     }},
    {"readrandom",
     [](std::shared_ptr<sdk::Client> client, const std::string& prefix) -> OperationPtr {
       return std::make_shared<ReadRandomOperation>(client, prefix);
     }},
    {"readmissing",
     [](std::shared_ptr<sdk::Client> client, const std::string& prefix) -> OperationPtr {
       return std::make_shared<ReadMissingOperation>(client, prefix);
     }},
};

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

BaseOperation::BaseOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix)
    : client(client), prefix(prefix) {
  auto status = client->NewRawKV(raw_kv);
  if (!status.IsOK()) {
    LOG(FATAL) << fmt::format("New RawKv failed, error: {}", status.ToString());
  }
}

Operation::Result BaseOperation::KvPut(bool is_random) {
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

Operation::Result BaseOperation::KvBatchPut(bool is_random) {
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

Operation::Result FillSeqOperation::Execute() { return FLAGS_batch_size == 1 ? KvPut(false) : KvBatchPut(false); }

Operation::Result FillRandomOperation::Execute() { return FLAGS_batch_size == 1 ? KvPut(true) : KvBatchPut(true); }

bool ReadOperation::Arrange() {
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

  std::cout << "\r" << fmt::format("Region({}) put ............ done", prefix) << std::endl;

  return true;
}

Operation::Result ReadSeqOperation::Execute() { return KvGet(keys[index_++ % keys.size()]); }

Operation::Result ReadRandomOperation::Execute() {
  uint32_t index = Helper::GenerateRealRandomInteger(0, UINT32_MAX) % keys.size();
  return KvGet(keys[index]);
}

Operation::Result ReadMissingOperation::Execute() {
  int random_str_len = FLAGS_key_size + 4 - prefix.size();
  std::string key = EncodeRawKey(prefix + GenRandomString(random_str_len));
  return KvGet(key);
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

OperationPtr NewOperation(std::shared_ptr<sdk::Client> client, const std::string& prefix) {
  auto it = support_operations.find(FLAGS_benchmark);
  if (it == support_operations.end()) {
    return nullptr;
  }

  return it->second(client, prefix);
}

}  // namespace benchmark
}  // namespace dingodb
