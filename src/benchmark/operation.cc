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
#include <random>
#include <string>

#include "fmt/core.h"

DEFINE_string(benchmark, "fillseq", "Benchmark type");

DEFINE_uint32(key_size, 64, "Key size");
DEFINE_uint32(value_size, 256, "Value size");

DEFINE_uint32(batch_size, 1, "Batch size");

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
       return std::make_shared<FillrandomOperation>(client, prefix);
     }}

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
    key = EncodeRawKey(prefix + GenSeqString(count, random_str_len));
  } else {
    key = EncodeRawKey(prefix + GenRandomString(random_str_len));
  }

  std::string value = GenRandomString(FLAGS_value_size);
  result.write_bytes = key.size() + value.size();

  int64_t start_time = Helper::TimestampUs();

  result.status = raw_kv->Put(key, value);

  result.eplased_time = Helper::TimestampUs() - start_time;

  return result;
}

Operation::Result BaseOperation::BatchKvPut(bool is_random) {
  Operation::Result result;
  int random_str_len = FLAGS_key_size - prefix.size();
  size_t count = counter.fetch_add(1, std::memory_order_relaxed);

  std::vector<sdk::KVPair> expect_kvs;
  for (int i = 0; i < FLAGS_batch_size; ++i) {
    sdk::KVPair kv;
    if (is_random) {
      kv.key = EncodeRawKey(prefix + GenSeqString(count, random_str_len));
    } else {
      kv.key = EncodeRawKey(prefix + GenRandomString(random_str_len));
    }

    kv.value = GenRandomString(FLAGS_value_size);

    expect_kvs.push_back(kv);
    result.write_bytes += kv.key.size() + kv.value.size();
  }

  int64_t start_time = Helper::TimestampUs();

  result.status = raw_kv->BatchPut(expect_kvs);

  result.eplased_time = Helper::TimestampUs() - start_time;

  return result;
}

Operation::Result FillSeqOperation::Execute() { return FLAGS_batch_size == 1 ? KvPut(false) : BatchKvPut(false); }

Operation::Result FillrandomOperation::Execute() { return FLAGS_batch_size == 1 ? KvPut(true) : BatchKvPut(true); }

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
