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
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "benchmark/color.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "sdk/client.h"
#include "sdk/vector.h"

DEFINE_string(coordinator_url, "file://./coor_list", "Coordinator url");
DEFINE_bool(show_version, false, "Show dingo-store version info");
DEFINE_string(prefix, "BENCH", "Region range prefix");

DEFINE_string(raw_engine, "LSM", "Raw engine type");
DEFINE_validator(raw_engine, [](const char*, const std::string& value) -> bool {
  auto raw_engine_type = dingodb::Helper::ToUpper(value);
  return raw_engine_type == "LSM" || raw_engine_type == "BTREE" || raw_engine_type == "XDP";
});

DEFINE_uint32(region_num, 1, "Region number");
DEFINE_uint32(vector_index_num, 1, "Vector index number");
DEFINE_uint32(concurrency, 1, "Concurrency of request");

DEFINE_uint64(req_num, 10000, "Request number");
DEFINE_uint32(timelimit, 0, "Time limit in seconds");

DEFINE_uint32(delay, 2, "Interval in seconds between intermediate reports");

DEFINE_bool(is_single_region_txn, true, "Is single region txn");
DEFINE_uint32(replica, 3, "Replica number");

DEFINE_bool(is_clean_region, true, "Is clean region");

DEFINE_string(vector_index_type, "HNSW", "Vector index type");
DEFINE_validator(vector_index_type, [](const char*, const std::string& value) -> bool {
  auto vector_index_type = dingodb::Helper::ToUpper(value);
  return vector_index_type == "HNSW" || vector_index_type == "FLAT" || vector_index_type == "IVF_FLAT" ||
         vector_index_type == "IVF_PQ" || vector_index_type == "BRUTE_FORCE";
});

// vector
DEFINE_uint32(vector_dimension, 256, "Vector dimension");
DEFINE_string(vector_value_type, "FLOAT", "Vector value type");
DEFINE_validator(vector_value_type, [](const char*, const std::string& value) -> bool {
  auto value_type = dingodb::Helper::ToUpper(value);
  return value_type == "FLOAT" || value_type == "UINT8";
});
DEFINE_uint32(vector_max_element_num, 100000, "Vector index contain max element number");
DEFINE_string(vector_metric_type, "L2", "Calcute vector distance method");
DEFINE_validator(vector_metric_type, [](const char*, const std::string& value) -> bool {
  auto metric_type = dingodb::Helper::ToUpper(value);
  return metric_type == "NONE" || metric_type == "L2" || metric_type == "IP" || metric_type == "COSINE";
});
DEFINE_string(vector_partition_vector_ids, "", "Vector id used by partition");

DEFINE_uint32(hnsw_ef_construction, 500, "HNSW ef construction");
DEFINE_uint32(hnsw_nlink_num, 64, "HNSW nlink number");

DEFINE_uint32(ivf_ncentroids, 2048, "IVF ncentroids");
DEFINE_uint32(ivf_nsubvector, 64, "IVF nsubvector");
DEFINE_uint32(ivf_bucket_init_size, 1000, "IVF bucket init size");
DEFINE_uint32(ivf_bucket_max_size, 1280000, "IVF bucket max size");
DEFINE_uint32(ivf_nbits_per_idx, 8, "IVF nbits per idx");

// vector search
DEFINE_string(vector_dataset, "", "Open source dataset, like sift/gist/glove/mnist etc.., hdf5 format");
DEFINE_validator(vector_dataset, [](const char*, const std::string& value) -> bool {
  return value.empty() || dingodb::Helper::IsExistPath(value);
});

DEFINE_uint32(vector_index_id, 0, "Vector index id");
DEFINE_string(vector_index_name, "", "Vector index name");

DEFINE_uint32(vector_search_topk, 10, "Vector search flag topk");
DEFINE_bool(vector_search_with_vector_data, true, "Vector search flag with_vector_data");
DEFINE_bool(vector_search_with_scalar_data, false, "Vector search flag with_scalar_data");
DEFINE_bool(vector_search_with_table_data, false, "Vector search flag with_table_data");
DEFINE_bool(vector_search_use_brute_force, false, "Vector search flag use_brute_force");
DEFINE_bool(vector_search_enable_range_search, false, "Vector search flag enable_range_search");
DEFINE_double(vector_search_radius, 0.1, "Vector search flag radius");

DECLARE_uint32(vector_put_batch_size);
DECLARE_uint32(vector_arrange_concurrency);
DECLARE_bool(vector_search_arrange_data);

DECLARE_string(benchmark);
DECLARE_uint32(key_size);
DECLARE_uint32(value_size);
DECLARE_uint32(batch_size);
DECLARE_bool(is_pessimistic_txn);
DECLARE_string(txn_isolation_level);

namespace dingodb {
namespace benchmark {

static const std::string kClientRaw = "w";
static const std::string kClientTxn = "x";

static const std::string kNamePrefix = "Benchmark";

static std::string EncodeRawKey(const std::string& str) { return kClientRaw + str; }
static std::string EncodeTxnKey(const std::string& str) { return kClientTxn + str; }

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

static bool IsTransactionBenchmark() {
  return (FLAGS_benchmark == "filltxnseq" || FLAGS_benchmark == "filltxnrandom" || FLAGS_benchmark == "readtxnseq" ||
          FLAGS_benchmark == "readtxnrandom" || FLAGS_benchmark == "readtxnmissing");
}

static bool IsVectorBenchmark() {
  return FLAGS_benchmark == "fillvectorseq" || FLAGS_benchmark == "fillvectorrandom" ||
         FLAGS_benchmark == "searchvector";
}

Stats::Stats() {
  latency_recorder_ = std::make_shared<bvar::LatencyRecorder>();
  recall_recorder_ = std::make_shared<bvar::LatencyRecorder>();
}

void Stats::Add(size_t duration, size_t write_bytes, size_t read_bytes) {
  ++req_num_;
  write_bytes_ += write_bytes;
  read_bytes_ += read_bytes;
  *latency_recorder_ << duration;
}

void Stats::Add(size_t duration, size_t write_bytes, size_t read_bytes, const std::vector<uint32_t>& recalls) {
  ++req_num_;
  write_bytes_ += write_bytes;
  read_bytes_ += read_bytes;
  *latency_recorder_ << duration;
  for (auto recall : recalls) {
    *recall_recorder_ << recall;
  }
}

void Stats::AddError() { ++error_count_; }

void Stats::Clear() {
  ++epoch_;
  req_num_ = 0;
  write_bytes_ = 0;
  read_bytes_ = 0;
  error_count_ = 0;
  latency_recorder_ = std::make_shared<bvar::LatencyRecorder>();
  recall_recorder_ = std::make_shared<bvar::LatencyRecorder>();
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

  if (FLAGS_vector_dataset.empty()) {
    std::cout << fmt::format("{:>8}{:>8}{:>8}{:>8.0f}{:>8.2f}{:>16}{:>8}{:>8}{:>8}{:>8}", epoch_, req_num_,
                             error_count_, (req_num_ / seconds), (write_bytes_ / seconds / 1048576),
                             latency_recorder_->latency(), latency_recorder_->max_latency(),
                             latency_recorder_->latency_percentile(0.5), latency_recorder_->latency_percentile(0.95),
                             latency_recorder_->latency_percentile(0.99))
              << '\n';
  } else {
    std::cout << fmt::format("{:>8}{:>8}{:>8}{:>8.0f}{:>8.2f}{:>16}{:>8}{:>8}{:>8}{:>8}{:>16.2f}", epoch_, req_num_,
                             error_count_, (req_num_ / seconds), (write_bytes_ / seconds / 1048576),
                             latency_recorder_->latency(), latency_recorder_->max_latency(),
                             latency_recorder_->latency_percentile(0.5), latency_recorder_->latency_percentile(0.95),
                             latency_recorder_->latency_percentile(0.99), recall_recorder_->latency() / 100.0)
              << '\n';
  }
}

std::string Stats::Header() {
  if (FLAGS_vector_dataset.empty()) {
    return fmt::format("{:>8}{:>8}{:>8}{:>8}{:>8}{:>16}{:>8}{:>8}{:>8}{:>8}", "EPOCH", "REQ_NUM", "ERRORS", "QPS",
                       "MB/s", "LATENCY AVG(us)", "MAX(us)", "P50(us)", "P95(us)", "P99(us)");
  } else {
    return fmt::format("{:>8}{:>8}{:>8}{:>8}{:>8}{:>16}{:>8}{:>8}{:>8}{:>8}{:>16}", "EPOCH", "REQ_NUM", "ERRORS", "QPS",
                       "MB/s", "LATENCY AVG(us)", "MAX(us)", "P50(us)", "P95(us)", "P99(us)", "RECALL AVG(%)");
  }
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

  if (IsVectorBenchmark()) {
    if (!FLAGS_vector_dataset.empty()) {
      dataset_ = Dataset::New(FLAGS_vector_dataset);
      if (dataset_ == nullptr || !dataset_->Init()) {
        return false;
      }
      if (dataset_->GetDimension() > 0) {
        FLAGS_vector_dimension = dataset_->GetDimension();
      }
    }

    if (FLAGS_vector_index_id > 0 || !FLAGS_vector_index_name.empty()) {
      vector_index_entries_ = ArrangeExistVectorIndex(FLAGS_vector_index_id, FLAGS_vector_index_name);
    } else {
      vector_index_entries_ = ArrangeVectorIndex(FLAGS_vector_index_num);
      if (vector_index_entries_.size() != FLAGS_vector_index_num) {
        return false;
      }
    }
  } else {
    region_entries_ = ArrangeRegion(FLAGS_region_num);
    if (region_entries_.size() != FLAGS_region_num) {
      return false;
    }
  }

  if (!ArrangeOperation()) {
    DINGO_LOG(ERROR) << "Arrange operation failed";
    return false;
  }

  if (!ArrangeData()) {
    DINGO_LOG(ERROR) << "Arrange data failed";
    return false;
  }

  std::cout << '\n';
  return true;
}

std::vector<RegionEntryPtr> Benchmark::ArrangeRegion(int num) {
  std::mutex mutex;
  std::vector<RegionEntryPtr> region_entries;

  bool is_txn_region = IsTransactionBenchmark();

  std::vector<std::thread> threads;
  threads.reserve(num);
  for (int thread_no = 0; thread_no < num; ++thread_no) {
    threads.emplace_back([this, is_txn_region, thread_no, &region_entries, &mutex]() {
      auto name = fmt::format("{}_{}_{}", kNamePrefix, Helper::TimestampMs(), thread_no + 1);
      std::string prefix = fmt::format("{}{:06}", FLAGS_prefix, thread_no);
      int64_t region_id = 0;
      if (is_txn_region) {
        region_id = CreateTxnRegion(name, prefix, Helper::PrefixNext(prefix), GetRawEngineType(), FLAGS_replica);
      } else {
        region_id = CreateRawRegion(name, prefix, Helper::PrefixNext(prefix), GetRawEngineType(), FLAGS_replica);
      }
      if (region_id == 0) {
        LOG(ERROR) << fmt::format("create region failed, name: {}", name);
        return;
      }

      std::cout << fmt::format("create region name({}) id({}) prefix({}) done", name, region_id, prefix) << '\n';

      auto region_entry = std::make_shared<RegionEntry>();
      region_entry->prefix = prefix;
      region_entry->region_id = region_id;

      std::lock_guard lock(mutex);
      region_entries.push_back(region_entry);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return region_entries;
}

std::vector<VectorIndexEntryPtr> Benchmark::ArrangeVectorIndex(int num) {
  std::vector<VectorIndexEntryPtr> vector_index_entries;

  std::vector<std::thread> threads;
  threads.reserve(num);
  std::mutex mutex;
  for (int thread_no = 0; thread_no < num; ++thread_no) {
    threads.emplace_back([this, thread_no, &vector_index_entries, &mutex]() {
      std::string name = fmt::format("{}_{}_{}", kNamePrefix, Helper::TimestampMs(), thread_no + 1);
      auto index_id = CreateVectorIndex(name, FLAGS_vector_index_type);
      if (index_id == 0) {
        LOG(ERROR) << fmt::format("create vector index failed, name: {}", name);
        return;
      }

      std::cout << fmt::format("create vector index name({}) id({}) type({}) done", name, index_id,
                               FLAGS_vector_index_type)
                << '\n';

      auto entry = std::make_shared<VectorIndexEntry>();
      entry->index_id = index_id;

      std::lock_guard lock(mutex);
      vector_index_entries.push_back(entry);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return vector_index_entries;
}

std::vector<VectorIndexEntryPtr> Benchmark::ArrangeExistVectorIndex(int64_t vector_index_id,
                                                                    const std::string& vector_index_name) {
  std::vector<VectorIndexEntryPtr> vector_index_entries;
  auto entry = std::make_shared<VectorIndexEntry>();
  if (vector_index_id > 0) {
    entry->index_id = vector_index_id;
    vector_index_entries.push_back(entry);
  } else if (!vector_index_name.empty()) {
    entry->index_id = GetVectorIndex(vector_index_name);
    if (entry->index_id > 0) {
      vector_index_entries.push_back(entry);
    }
  }

  return vector_index_entries;
}

bool Benchmark::ArrangeOperation() {
  operation_ = NewOperation(client_);

  return true;
}

bool Benchmark::ArrangeData() {
  for (auto& region_entry : region_entries_) {
    operation_->Arrange(region_entry);
  }

  for (auto& vector_index_entry : vector_index_entries_) {
    operation_->Arrange(vector_index_entry, dataset_);
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
    thread_entry->vector_index_entries = vector_index_entries_;

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
  if (FLAGS_is_clean_region) {
    // Drop region
    for (auto& region_entry : region_entries_) {
      DropRegion(region_entry->region_id);
    }

    // Drop vector index
    for (auto& vector_index_entry : vector_index_entries_) {
      DropVectorIndex(vector_index_entry->index_id);
    }
  }
}

int64_t Benchmark::CreateRawRegion(const std::string& name, const std::string& start_key, const std::string& end_key,
                                   sdk::EngineType engine_type, int replicas) {
  sdk::RegionCreator* tmp;
  auto status = client_->NewRegionCreator(&tmp);
  CHECK(status.ok()) << fmt::format("new region creator failed, {}", status.ToString());
  std::shared_ptr<sdk::RegionCreator> creator(tmp);

  int64_t region_id;
  status = creator->SetRegionName(name)
               .SetEngineType(engine_type)
               .SetReplicaNum(replicas)
               .SetRange(EncodeRawKey(start_key), EncodeRawKey(end_key))
               .Create(region_id);
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("create region failed, {}", status.ToString());
    return 0;
  }
  if (region_id == 0) {
    LOG(ERROR) << "region_id is 0, invalid";
  }

  return region_id;
}

int64_t Benchmark::CreateTxnRegion(const std::string& name, const std::string& start_key, const std::string& end_key,
                                   sdk::EngineType engine_type, int replicas) {
  sdk::RegionCreator* tmp;
  auto status = client_->NewRegionCreator(&tmp);
  CHECK(status.ok()) << fmt::format("new region creator failed, {}", status.ToString());
  std::shared_ptr<sdk::RegionCreator> creator(tmp);

  int64_t region_id;
  status = creator->SetRegionName(name)
               .SetEngineType(engine_type)
               .SetReplicaNum(replicas)
               .SetRange(EncodeTxnKey(start_key), EncodeTxnKey(end_key))
               .Create(region_id);
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("create region failed, {}", status.ToString());
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

sdk::MetricType GetMetricType(const std::string& metric_type) {
  auto upper_metric_type = dingodb::Helper::ToUpper(metric_type);

  if (upper_metric_type == "L2") {
    return sdk::MetricType::kL2;
  } else if (upper_metric_type == "IP") {
    return sdk::MetricType::kInnerProduct;
  } else if (upper_metric_type == "COSINE") {
    return sdk::MetricType::kCosine;
  }

  return sdk::MetricType::kNoneMetricType;
}

sdk::FlatParam GenFlatParam() {
  sdk::FlatParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type));
  return param;
}

sdk::IvfFlatParam GenIvfFlatParam() {
  sdk::IvfFlatParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type));
  param.ncentroids = FLAGS_ivf_ncentroids;
  return param;
}

sdk::IvfPqParam GenIvfPqParam() {
  sdk::IvfPqParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type));
  param.ncentroids = FLAGS_ivf_ncentroids;
  param.nsubvector = FLAGS_ivf_nsubvector;
  param.bucket_init_size = FLAGS_ivf_bucket_init_size;
  param.bucket_max_size = FLAGS_ivf_bucket_max_size;
  param.nbits_per_idx = FLAGS_ivf_nbits_per_idx;
  return param;
}

sdk::HnswParam GenHnswParam() {
  sdk::HnswParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type), FLAGS_vector_max_element_num);
  param.ef_construction = FLAGS_hnsw_ef_construction;
  param.nlinks = FLAGS_hnsw_nlink_num;

  return param;
}

sdk::BruteForceParam GenBruteForceParam() {
  sdk::BruteForceParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type));
  return param;
}

int64_t Benchmark::CreateVectorIndex(const std::string& name, const std::string& vector_index_type) {
  sdk::VectorIndexCreator* creator = nullptr;
  auto status = client_->NewVectorIndexCreator(&creator);
  CHECK(status.ok()) << fmt::format("new vector index creator failed, {}", status.ToString());

  int64_t vector_index_id = 0;
  std::vector<int64_t> separator_id;
  if (!FLAGS_vector_partition_vector_ids.empty()) {
    Helper::SplitString(FLAGS_vector_partition_vector_ids, ',', separator_id);
  }

  creator->SetName(name)
      .SetSchemaId(pb::meta::ReservedSchemaIds::DINGO_SCHEMA)
      .SetRangePartitions(separator_id)
      .SetReplicaNum(3);

  if (vector_index_type == "HNSW") {
    creator->SetHnswParam(GenHnswParam());
  } else if (vector_index_type == "BRUTE_FORCE") {
    creator->SetBruteForceParam(GenBruteForceParam());
  } else if (vector_index_type == "FLAT") {
    creator->SetFlatParam(GenFlatParam());
  } else if (vector_index_type == "IVF_FLAT") {
    creator->SetIvfFlatParam(GenIvfFlatParam());
  } else if (vector_index_type == "IVF_PQ") {
    creator->SetIvfPqParam(GenIvfPqParam());
  } else {
    LOG(ERROR) << fmt::format("Not support vector index type {}", vector_index_type);
    return 0;
  }

  status = creator->Create(vector_index_id);
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("create vector index failed, {}", status.ToString());
    return 0;
  }
  if (vector_index_id == 0) {
    LOG(ERROR) << "vector_index_id is 0, invalid";
  }

  std::this_thread::sleep_for(std::chrono::seconds(10));

  return vector_index_id;
}

void Benchmark::DropVectorIndex(int64_t vector_index_id) {
  CHECK(vector_index_id != 0) << "vector_index_id is invalid";
  auto status = client_->DropIndex(vector_index_id);
  CHECK(status.IsOK()) << fmt::format("drop vector index failed, {}", status.ToString());
}

int64_t Benchmark::GetVectorIndex(const std::string& name) {
  int64_t vector_index_id = 0;
  auto status = client_->GetIndexId(pb::meta::ReservedSchemaIds::DINGO_SCHEMA, name, vector_index_id);
  CHECK(status.ok()) << fmt::format("get vector index failed, {}", status.ToString());

  return vector_index_id;
}

static std::vector<std::string> ExtractPrefixs(const std::vector<RegionEntryPtr>& region_entries) {
  std::vector<std::string> prefixes;
  prefixes.reserve(region_entries.size());
  for (const auto& region_entry : region_entries) {
    prefixes.push_back(region_entry->prefix);
  }

  return prefixes;
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

  } else if (IsVectorBenchmark()) {
    ExecutePerVectorIndex(thread_entry);
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

void Benchmark::ExecutePerVectorIndex(ThreadEntryPtr thread_entry) {
  auto vector_index_entries = thread_entry->vector_index_entries;

  int64_t req_num_per_thread = static_cast<int64_t>(FLAGS_req_num / (FLAGS_concurrency * FLAGS_vector_index_num));
  for (int64_t i = 0; i < req_num_per_thread; ++i) {
    if (thread_entry->is_stop.load(std::memory_order_relaxed)) {
      break;
    }

    for (const auto& vector_index_entry : vector_index_entries) {
      size_t eplased_time;
      auto result = operation_->Execute(vector_index_entry);
      {
        std::lock_guard lock(mutex_);
        if (result.status.ok()) {
          stats_interval_->Add(result.eplased_time, result.write_bytes, result.read_bytes, result.recalls);
          stats_cumulative_->Add(result.eplased_time, result.write_bytes, result.read_bytes, result.recalls);
        } else {
          stats_interval_->AddError();
          stats_cumulative_->AddError();
        }
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

  sdk::Client* tmp;
  status = sdk::Client::Build(FLAGS_coordinator_url, &tmp);
  CHECK(status.IsOK()) << fmt::format("Build sdk client failed, error: {}", status.ToString());
  client_.reset(tmp);

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

  std::cout << fmt::format("{:<34}: {:>32}", "benchmark", FLAGS_benchmark) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "region_num", FLAGS_region_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "prefix", FLAGS_prefix) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "raw_engine", FLAGS_raw_engine) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "region_num", FLAGS_region_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_index_num", FLAGS_vector_index_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "is_clean_region", FLAGS_is_clean_region ? "true" : "false") << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "concurrency", FLAGS_concurrency) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "req_num", FLAGS_req_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "delay(s)", FLAGS_delay) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "timelimit(s)", FLAGS_timelimit) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "key_size(byte)", FLAGS_key_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "value_size(byte)", FLAGS_value_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "batch_size", FLAGS_batch_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "is_single_region_txn", FLAGS_is_single_region_txn ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "is_pessimistic_txn", FLAGS_is_pessimistic_txn ? "true" : "false") << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "txn_isolation_level", FLAGS_vector_search_topk) << '\n';

  std::cout << fmt::format("{:<34}: {:>32}", "vector_dimension", FLAGS_vector_dimension) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_value_type", FLAGS_vector_value_type) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_max_element_num", FLAGS_vector_max_element_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_metric_type", FLAGS_vector_metric_type) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_partition_vector_ids", FLAGS_vector_partition_vector_ids) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_dataset", FLAGS_vector_dataset) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_arrange_concurrency", FLAGS_vector_arrange_concurrency) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_put_batch_size", FLAGS_vector_put_batch_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "hnsw_ef_construction", FLAGS_hnsw_ef_construction) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "hnsw_nlink_num", FLAGS_hnsw_nlink_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_ncentroids", FLAGS_ivf_ncentroids) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_nsubvector", FLAGS_ivf_nsubvector) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_bucket_init_size", FLAGS_ivf_bucket_init_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_bucket_max_size", FLAGS_ivf_bucket_max_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_nbits_per_idx", FLAGS_ivf_nbits_per_idx) << '\n';

  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_topk", FLAGS_vector_search_topk) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_with_vector_data",
                           FLAGS_vector_search_with_vector_data ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_with_scalar_data",
                           FLAGS_vector_search_with_scalar_data ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_with_table_data",
                           FLAGS_vector_search_with_table_data ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_use_brute_force",
                           FLAGS_vector_search_use_brute_force ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_enable_range_search",
                           FLAGS_vector_search_enable_range_search ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_radius", FLAGS_vector_search_radius) << '\n';

  std::cout << '\n';
}

}  // namespace benchmark
}  // namespace dingodb
