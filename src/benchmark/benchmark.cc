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

#include "benchmark/benchmark.h"

#include <atomic>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <ios>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "benchmark/color.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "sdk/client.h"

DEFINE_string(coordinator_url, "file://./coor_list", "Coordinator url");
DEFINE_bool(show_version, false, "Show dingo-store version info");
DEFINE_string(prefix, "BENCH", "Region range prefix");

DEFINE_string(raw_engine, "LSM", "Raw engine type");
DEFINE_validator(raw_engine, [](const char*, const std::string& value) -> bool {
  auto raw_engine_type = dingodb::Helper::ToUpper(value);
  return raw_engine_type == "LSM" || raw_engine_type == "BTREE" || raw_engine_type == "XDP";
});

DEFINE_uint32(region_num, 1, "Region number");
DEFINE_uint32(concurrency, 1, "Concurrency of request");

DEFINE_uint64(req_num, 10000, "Request number");
DEFINE_uint32(timelimit, 0, "Time limit in seconds");

DEFINE_uint32(delay, 2, "Interval in seconds between intermediate reports");

DEFINE_bool(is_single_region_txn, true, "Is single region txn");

DECLARE_string(benchmark);
DECLARE_uint32(key_size);
DECLARE_uint32(value_size);
DECLARE_uint32(batch_size);
DECLARE_bool(is_pessimistic_txn);
DECLARE_string(txn_isolation_level);

namespace dingodb {
namespace benchmark {

static const std::string kClientRaw = "w";

static const std::string kRegionNamePrefix = "Benchmark_";

static std::string EncodeRawKey(const std::string& str) { return kClientRaw + str; }

sdk::EngineType GetRawEngineType() {
  auto raw_engine = Helper::ToUpper(FLAGS_raw_engine);
  if (raw_engine == "LSM") {
    return sdk::EngineType::kLSM;
  } else if (FLAGS_raw_engine == "BTREE") {
    return sdk::EngineType::kBTree;
  } else if (raw_engine == "XDP") {
    return sdk::EngineType::kXDPROCKS;
  }

  LOG(FATAL) << fmt::format("Not support raw_engine: {}", FLAGS_raw_engine);
  return sdk::EngineType::kLSM;
}

Stats::Stats() { latency_recorder_ = std::make_shared<bvar::LatencyRecorder>(); }

void Stats::Add(size_t duration, size_t write_bytes, size_t read_bytes) {
  ++req_num_;
  write_bytes_ += write_bytes;
  read_bytes_ += read_bytes;
  *latency_recorder_ << duration;
}

void Stats::AddError() { ++error_count_; }

void Stats::Clear() {
  ++epoch_;
  req_num_ = 0;
  write_bytes_ = 0;
  read_bytes_ = 0;
  error_count_ = 0;
  latency_recorder_ = std::make_shared<bvar::LatencyRecorder>();
}

void Stats::Report(bool is_cumulative, size_t milliseconds) const {
  double seconds = milliseconds / static_cast<double>(1000);

  if (is_cumulative) {
    std::cout << COLOR_GREEN << fmt::format("Cumulative({}ms):", milliseconds) << COLOR_RESET << '\n';
  } else {
    if (epoch_ == 1) {
      std::cout << COLOR_GREEN << fmt::format("Interval({}ms):", FLAGS_delay * 1000) << COLOR_RESET << '\n';
    }
    if (epoch_ % 20 == 1) {
      std::cout << COLOR_GREEN << Header() << COLOR_RESET << '\n';
    }
  }

  std::cout << fmt::format("{:>8}{:>8}{:>8}{:>8.0f}{:>8.2f}{:>16}{:>16}{:>16}{:>16}{:>16}", epoch_, req_num_,
                           error_count_, (req_num_ / seconds), (write_bytes_ / seconds / 1048576),
                           latency_recorder_->latency(), latency_recorder_->max_latency(),
                           latency_recorder_->latency_percentile(0.5), latency_recorder_->latency_percentile(0.95),
                           latency_recorder_->latency_percentile(0.99))
            << '\n';
}

std::string Stats::Header() {
  return fmt::format("{:>8}{:>8}{:>8}{:>8}{:>8}{:>16}{:>16}{:>16}{:>16}{:>16}", "EPOCH", "REQ_NUM", "ERRORS", "QPS",
                     "MB/s", "LATENCY_AVG(us)", "LATENCY_MAX(us)", "LATENCY_P50(us)", "LATENCY_P95(us)",
                     "LATENCY_P99(us)");
}

Benchmark::Benchmark(std::shared_ptr<sdk::CoordinatorProxy> coordinator_proxy, std::shared_ptr<sdk::Client> client)
    : coordinator_proxy_(coordinator_proxy), client_(client) {
  stats_interval_ = std::make_shared<Stats>();
  stats_cumulative_ = std::make_shared<Stats>();
}

std::shared_ptr<Benchmark> Benchmark::New(std::shared_ptr<sdk::CoordinatorProxy> coordinator_proxy,
                                          std::shared_ptr<sdk::Client> client) {
  return std::make_shared<Benchmark>(coordinator_proxy, client);
}

void Benchmark::Stop() {
  for (auto& thread_entry : thread_entries_) {
    thread_entry->is_stop.store(true, std::memory_order_relaxed);
  }
}

bool Benchmark::Run() {
  if (!Arrange()) {
    Clean();
    return false;
  }

  Launch();

  size_t start_time = Helper::TimestampMs();

  // Interval report
  IntervalReport();

  Wait();

  // Cumulative report
  Report(true, Helper::TimestampMs() - start_time);

  Clean();
  return true;
}

bool Benchmark::Arrange() {
  std::cout << COLOR_GREEN << "Arrange: " << COLOR_RESET << '\n';

  region_entries_ = ArrangeRegion(FLAGS_region_num);
  if (region_entries_.size() != FLAGS_region_num) {
    return false;
  }

  if (!ArrangeOperation()) {
    return false;
  }

  if (!ArrangeData()) {
    return false;
  }

  std::cout << '\n';
  return true;
}

std::vector<RegionEntryPtr> Benchmark::ArrangeRegion(int num) {
  std::vector<RegionEntryPtr> region_entries;

  for (int i = 0; i < num; ++i) {
    std::string prefix = fmt::format("{}{:06}", FLAGS_prefix, i);
    auto region_id =
        CreateRegion(kRegionNamePrefix + std::to_string(i + 1), prefix, Helper::PrefixNext(prefix), GetRawEngineType());
    if (region_id == 0) {
      return region_entries;
    }

    std::cout << fmt::format("Create region({}) {} done", prefix, region_id) << '\n';

    auto region_entry = std::make_shared<RegionEntry>();
    region_entry->prefix = prefix;
    region_entry->region_id = region_id;

    region_entries.push_back(region_entry);
  }

  return region_entries;
}

bool Benchmark::ArrangeOperation() {
  operation_ = NewOperation(client_);

  return true;
}

bool Benchmark::ArrangeData() {
  for (auto& region_entry : region_entries_) {
    operation_->Arrange(region_entry);
  }

  return true;
}

void Benchmark::Launch() {
  // Create multiple thread run benchmark
  thread_entries_.reserve(FLAGS_concurrency);
  for (int i = 0; i < FLAGS_concurrency; ++i) {
    auto thread_entry = std::make_shared<ThreadEntry>();
    thread_entry->client = client_;
    thread_entry->region_entries = region_entries_;

    thread_entry->thread =
        std::thread([this](ThreadEntryPtr thread_entry) mutable { ThreadRoutine(thread_entry); }, thread_entry);
    thread_entries_.push_back(thread_entry);
  }
}

void Benchmark::Wait() {
  for (auto& thread_entry : thread_entries_) {
    thread_entry->thread.join();
  }
}

void Benchmark::Clean() {
  // Drop region
  for (auto& region_entry : region_entries_) {
    DropRegion(region_entry->region_id);
  }
}

int64_t Benchmark::CreateRegion(const std::string& name, const std::string& start_key, const std::string& end_key,
                                sdk::EngineType engine_type, int replicas) {
  std::shared_ptr<sdk::RegionCreator> creator;
  auto status = client_->NewRegionCreator(creator);
  CHECK(status.ok()) << fmt::format("new region creator failed, {}", status.ToString());

  int64_t region_id;
  status = creator->SetRegionName(name)
               .SetEngineType(engine_type)
               .SetReplicaNum(replicas)
               .SetRange(EncodeRawKey(start_key), EncodeRawKey(end_key))
               .Create(region_id);
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("Create region failed, {}", status.ToString());
    return 0;
  }
  if (region_id == 0) {
    LOG(ERROR) << "region_id is 0, invalid";
  }

  return region_id;
}

bool Benchmark::IsStop() {
  bool all_stop = true;
  for (auto& thread_entry : thread_entries_) {
    if (!thread_entry->is_stop.load(std::memory_order_relaxed)) {
      all_stop = false;
    }
  }

  return all_stop;
}

void Benchmark::DropRegion(int64_t region_id) {
  CHECK(region_id != 0) << "region_id is invalid";
  auto status = client_->DropRegion(region_id);
  CHECK(status.IsOK()) << fmt::format("Drop region failed, {}", status.ToString());
}

static std::vector<std::string> ExtractPrefixs(const std::vector<RegionEntryPtr>& region_entries) {
  std::vector<std::string> prefixes;
  prefixes.reserve(region_entries.size());
  for (const auto& region_entry : region_entries) {
    prefixes.push_back(region_entry->prefix);
  }

  return prefixes;
}

static bool IsTransactionBenchmark() {
  return (FLAGS_benchmark == "filltxnseq" || FLAGS_benchmark == "filltxnrandom" || FLAGS_benchmark == "readtxnseq" ||
          FLAGS_benchmark == "readtxnrandom" || FLAGS_benchmark == "readtxnmissing");
}

void Benchmark::ThreadRoutine(ThreadEntryPtr thread_entry) {
  // Set signal
  sigset_t sig_set;
  if (sigemptyset(&sig_set) || sigaddset(&sig_set, SIGINT) || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr)) {
    std::cerr << "Cannot block signal" << '\n';
    exit(1);
  }

  if (IsTransactionBenchmark()) {
    if (FLAGS_is_single_region_txn) {
      ExecutePerRegion(thread_entry);
    } else {
      ExecuteMultiRegion(thread_entry);
    }

  } else {
    ExecutePerRegion(thread_entry);
  }

  thread_entry->is_stop.store(true, std::memory_order_relaxed);
}

void Benchmark::ExecutePerRegion(ThreadEntryPtr thread_entry) {
  auto region_entries = thread_entry->region_entries;

  int64_t req_num_per_thread = static_cast<int64_t>(FLAGS_req_num / (FLAGS_concurrency * FLAGS_region_num));
  for (int64_t i = 0; i < req_num_per_thread; ++i) {
    if (thread_entry->is_stop.load(std::memory_order_relaxed)) {
      break;
    }

    for (const auto& region_entry : region_entries) {
      size_t eplased_time;
      auto result = operation_->Execute(region_entry);
      {
        std::lock_guard lock(mutex_);
        if (result.status.ok()) {
          stats_interval_->Add(result.eplased_time, result.write_bytes, result.read_bytes);
          stats_cumulative_->Add(result.eplased_time, result.write_bytes, result.read_bytes);
        } else {
          stats_interval_->AddError();
          stats_cumulative_->AddError();
        }
      }
    }
  }
}

void Benchmark::ExecuteMultiRegion(ThreadEntryPtr thread_entry) {
  auto region_entries = thread_entry->region_entries;

  int64_t req_num_per_thread = static_cast<int64_t>(FLAGS_req_num / FLAGS_concurrency);

  for (int64_t i = 0; i < req_num_per_thread; ++i) {
    if (thread_entry->is_stop.load(std::memory_order_relaxed)) {
      break;
    }

    auto result = operation_->Execute(region_entries);
    {
      std::lock_guard lock(mutex_);
      if (result.status.ok()) {
        stats_interval_->Add(result.eplased_time, result.write_bytes, result.read_bytes);
        stats_cumulative_->Add(result.eplased_time, result.write_bytes, result.read_bytes);
      } else {
        stats_interval_->AddError();
        stats_cumulative_->AddError();
      }
    }
  }
}

void Benchmark::IntervalReport() {
  size_t delay_ms = FLAGS_delay * 1000;
  size_t start_time = Helper::TimestampMs();
  size_t cumulative_start_time = Helper::TimestampMs();

  for (;;) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    size_t milliseconds = Helper::TimestampMs() - start_time;
    if (milliseconds > delay_ms) {
      Report(false, milliseconds);
      start_time = Helper::TimestampMs();
    }

    // Check time limit
    if (FLAGS_timelimit > 0 && Helper::TimestampMs() - cumulative_start_time > FLAGS_timelimit * 1000) {
      Stop();
    }

    if (IsStop()) {
      break;
    }
  }
}

void Benchmark::Report(bool is_cumulative, size_t milliseconds) {
  std::lock_guard lock(mutex_);

  if (is_cumulative) {
    stats_cumulative_->Report(true, milliseconds);
    stats_interval_->Clear();
  } else {
    stats_interval_->Report(false, milliseconds);
    stats_interval_->Clear();
  }
}

Environment& Environment::GetInstance() {
  static Environment instance;
  return instance;
}

bool Environment::Init() {
  if (!IsSupportBenchmarkType(FLAGS_benchmark)) {
    std::cerr << fmt::format("Not support benchmark {}, just support: {}", FLAGS_benchmark, GetSupportBenchmarkType())
              << '\n';
    return false;
  }

  coordinator_proxy_ = std::make_shared<sdk::CoordinatorProxy>();
  auto status = coordinator_proxy_->Open(FLAGS_coordinator_url);
  CHECK(status.IsOK()) << "Open coordinator proxy failed, please check parameter --url=" << FLAGS_coordinator_url;

  status = sdk::Client::Build(FLAGS_coordinator_url, client_);
  CHECK(status.IsOK()) << fmt::format("Build sdk client failed, error: {}", status.ToString());

  PrintParam();

  if (FLAGS_show_version) {
    PrintVersionInfo();
  }

  return true;
}

void Environment::AddBenchmark(BenchmarkPtr benchmark) { benchmarks_.push_back(benchmark); }

void Environment::Stop() {
  for (auto& benchmark : benchmarks_) {
    benchmark->Stop();
  }
}

void Environment::PrintVersionInfo() {
  pb::coordinator::HelloRequest request;
  pb::coordinator::HelloResponse response;

  request.set_is_just_version_info(true);

  auto status = coordinator_proxy_->Hello(request, response);
  CHECK(status.IsOK()) << fmt::format("Hello failed, {}", status.ToString());

  auto version_info = response.version_info();

  std::cout << COLOR_GREEN << "Version(dingo-store):" << COLOR_RESET << '\n';

  std::cout << fmt::format("{:<24}: {:>64}", "git_commit_hash", version_info.git_commit_hash()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "git_tag_name", version_info.git_tag_name()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "git_commit_user", version_info.git_commit_user()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "git_commit_mail", version_info.git_commit_mail()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "git_commit_time", version_info.git_commit_time()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "major_version", version_info.major_version()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "minor_version", version_info.minor_version()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "dingo_build_type", version_info.dingo_build_type()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "dingo_contrib_build_type", version_info.dingo_contrib_build_type())
            << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_mkl", (version_info.use_mkl() ? "true" : "false")) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_openblas", (version_info.use_openblas() ? "true" : "false")) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_tcmalloc", (version_info.use_tcmalloc() ? "true" : "false")) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_profiler", (version_info.use_profiler() ? "true" : "false")) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_sanitizer", (version_info.use_sanitizer() ? "true" : "false"))
            << '\n';

  std::cout << '\n';
}

void Environment::PrintParam() {
  std::cout << COLOR_GREEN << "Parameter:" << COLOR_RESET << '\n';

  std::cout << fmt::format("{:<24}: {:>32}", "benchmark", FLAGS_benchmark) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "region_num", FLAGS_region_num) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "prefix", FLAGS_prefix) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "raw_engine", FLAGS_raw_engine) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "concurrency", FLAGS_concurrency) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "req_num", FLAGS_req_num) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "delay(s)", FLAGS_delay) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "timelimit(s)", FLAGS_timelimit) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "key_size(byte)", FLAGS_key_size) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "value_size(byte)", FLAGS_value_size) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "batch_size", FLAGS_batch_size) << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "is_single_region_txn", FLAGS_is_single_region_txn ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "is_pessimistic_txn", FLAGS_is_pessimistic_txn ? "true" : "false") << '\n';
  std::cout << fmt::format("{:<24}: {:>32}", "txn_isolation_level", FLAGS_txn_isolation_level) << '\n';
  std::cout << '\n';
}

}  // namespace benchmark
}  // namespace dingodb
