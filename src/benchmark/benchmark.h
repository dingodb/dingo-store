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

#ifndef DINGODB_BENCHMARK_H_
#define DINGODB_BENCHMARK_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "benchmark/operation.h"
#include "bvar/latency_recorder.h"
#include "sdk/client.h"
#include "sdk/coordinator_proxy.h"

namespace dingodb {
namespace benchmark {

class Stats {
 public:
  Stats();
  ~Stats() = default;

  void Add(size_t duration, size_t write_bytes, size_t read_bytes);
  void AddError();

  void Clear();

  void Report(bool is_cumulative, size_t milliseconds) const;

 private:
  static std::string Header();

  uint32_t epoch_{1};
  size_t req_num_{0};
  size_t write_bytes_{0};
  size_t read_bytes_{0};
  size_t error_count_{0};
  std::shared_ptr<bvar::LatencyRecorder> latency_recorder_;
};

using StatsPtr = std::shared_ptr<Stats>;
using MultiStats = std::vector<StatsPtr>;

struct RegionEntry {
  int64_t region_id;
  std::string prefix;

  OperationPtr operation;
};

struct ThreadEntry {
  std::thread thread;
  std::atomic<bool> is_stop{false};

  std::shared_ptr<sdk::Client> client;
  std::vector<RegionEntry> region_entries;
};
using ThreadEntryPtr = std::shared_ptr<ThreadEntry>;

class Benchmark {
 public:
  Benchmark(std::shared_ptr<sdk::CoordinatorProxy> coordinator_proxy, std::shared_ptr<sdk::Client> client);
  ~Benchmark() = default;

  static std::shared_ptr<Benchmark> New(std::shared_ptr<sdk::CoordinatorProxy> coordinator_proxy,
                                        std::shared_ptr<sdk::Client> client);

  void Stop();

  void Run();

 private:
  std::vector<RegionEntry> ArrangeRegion(int num);

  int64_t CreateRegion(const std::string& name, const std::string& start_key, const std::string& end_key,
                       sdk::EngineType engine_type = sdk::kLSM, int replicas = 3);
  void DropRegion(int64_t region_id);

  void ThreadRoutine(ThreadEntryPtr thread_entry);

  bool IsStop();

  void IntervalReport();
  void Report(bool is_cumulative, size_t milliseconds);

  std::shared_ptr<sdk::CoordinatorProxy> coordinator_proxy_;
  std::shared_ptr<sdk::Client> client_;

  std::vector<ThreadEntryPtr> thread_entries_;

  std::mutex mutex_;
  StatsPtr stats_interval_;
  StatsPtr stats_cumulative_;
};
using BenchmarkPtr = std::shared_ptr<Benchmark>;

class Environment {
 public:
  static Environment& GetInstance();

  bool Init();

  std::shared_ptr<sdk::CoordinatorProxy> GetCoordinatorProxy() { return coordinator_proxy_; }
  std::shared_ptr<sdk::Client> GetClient() { return client_; }

  void AddBenchmark(BenchmarkPtr benchmark);
  void Stop();

 private:
  Environment() = default;
  ~Environment() = default;

  void PrintVersionInfo();
  static void PrintParam();

  std::vector<BenchmarkPtr> benchmarks_;

  std::shared_ptr<sdk::CoordinatorProxy> coordinator_proxy_;
  std::shared_ptr<sdk::Client> client_;
};

}  // namespace benchmark
}  // namespace dingodb

#endif  // DINGODB_BENCHMARK_H_
