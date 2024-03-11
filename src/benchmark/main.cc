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

#include <csignal>
#include <memory>
#include <string>

#include "benchmark/benchmark.h"
#include "benchmark/dataset.h"
#include "benchmark/dataset_util.h"
#include "common/helper.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

DECLARE_string(benchmark);

const std::string kVersion = "0.1.0";

static std::string GetUsageMessage() {
  std::string message;

  message += "\nUsage:";
  message += "\n  --coordinator_url dingo-store cluster endpoint, default(file://./coor_list)";
  message += "\n  --benchmark benchmark type, default(fillseq)";
  message += "\n  --show_version show dingo-store cluster version info, default(false)";
  message += "\n  --prefix region range prefix, used to distinguish region, default(BENCH)";
  message += "\n  --raw_engine raw engine type, support LSM/BTREE/XDP default(LSM)";
  message += "\n  --region_num region number, default(1)";
  message += "\n  --vector_index_num vector index number, default(1)";
  message += "\n  --is_clean_region is clean region, default(true)";
  message += "\n  --concurrency concurrency as thread number, default(1)";
  message += "\n  --req_num invoke RPC request number, default(10000)";
  message += "\n  --delay print benchmark metrics interval time, unit(second), default(2)";
  message += "\n  --timelimit the limit of run time, 0 is no limit, unit(second), default(0)";
  message += "\n  --key_size key size, default(64)";
  message += "\n  --value_size value size, default(256)";
  message += "\n  --batch_size batch put size, default(1)";
  message += "\n  --arrange_kv_num the number of arrange kv, used by readseq/readrandom/readmissing, default(10000)";
  message += "\n  --is_single_region_txn is single transaction, default(true)";
  message += "\n  --is_pessimistic_txn optimistic or pessimistic transaction, default(false)";
  message += "\n  --txn_isolation_level transaction isolation level SI/RC, default(SI)";
  message += "\n  --vector_dimension vector dimension, default(256)";
  message += "\n  --vector_value_type vector value type float/uint8, default(float)";
  message += "\n  --vector_max_element_num vector index contain max element number, default(100000)";
  message += "\n  --vector_metric_type calcute vector distance method L2/IP/COSINE, default(L2)";
  message += "\n  --vector_partition_vector_ids vector id used by partition, default()";
  message += "\n  --vector_arrange_concurrency vector arrange concurrency, default(10)";
  message += "\n  --vector_put_batch_size vector put batch size, default(512)";
  message += "\n  --hnsw_ef_construction HNSW ef construction, default(true)";
  message += "\n  --hnsw_nlink_num HNSW nlink number, default(32)";
  message += "\n  --ivf_ncentroids IVF ncentroids, default(2048)";
  message += "\n  --ivf_nsubvector IVF nsubvector, default(64)";
  message += "\n  --ivf_bucket_init_size IVF bucket init size, default(1000)";
  message += "\n  --ivf_bucket_max_size IVF bucket max size, default(1280000)";
  message += "\n  --ivf_nbits_per_idx IVF nbits per index, default(8)";
  message += "\n  --vector_index_id vector index id, default(0)";
  message += "\n  --vector_index_name vector index name, default()";
  message += "\n  --vector_search_arrange_data arrange data, default(true)";
  message += "\n  --vector_search_topk vector search flag topk, default(10)";
  message += "\n  --vector_search_with_vector_data vector search flag with_vector_data, default(true)";
  message += "\n  --vector_search_with_scalar_data vector search flag with_scalar_data, default(false)";
  message += "\n  --vector_search_with_table_data vector search flag with_table_data, default(false)";
  message += "\n  --vector_search_use_brute_force vector search flag use_brute_force, default(false)";
  message += "\n  --vector_search_enable_range_search vector search flag enable_range_search, default(false)";
  message += "\n  --vector_search_radius vector search flag radius, default(0.1)";

  return message;
}

static void SignalHandler(int signo) {  // NOLINT
  dingodb::benchmark::Environment::GetInstance().Stop();
}

void SetupSignalHandler() {
  sighandler_t s;
  s = signal(SIGTERM, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGTERM\n");
    exit(-1);
  }
  s = signal(SIGINT, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGINT\n");
    exit(-1);
  }
}

void InitLog(const std::string& log_dir) {
  if (!dingodb::Helper::IsExistPath(log_dir)) {
    dingodb::Helper::CreateDirectories(log_dir);
  }

  FLAGS_logbufsecs = 0;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;

  std::string program_name = "dingodb_bench";

  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(google::GLOG_INFO, fmt::format("{}/{}.info.log.", log_dir, program_name).c_str());
  google::SetLogDestination(google::GLOG_WARNING, fmt::format("{}/{}.warn.log.", log_dir, program_name).c_str());
  google::SetLogDestination(google::GLOG_ERROR, fmt::format("{}/{}.error.log.", log_dir, program_name).c_str());
  google::SetLogDestination(google::GLOG_FATAL, fmt::format("{}/{}.fatal.log.", log_dir, program_name).c_str());
  google::SetStderrLogging(google::GLOG_FATAL);
}

int main(int argc, char* argv[]) {
  InitLog("./log");

  google::SetVersionString(kVersion);
  google::SetUsageMessage(GetUsageMessage());
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_benchmark == "preprocess") {
    dingodb::benchmark::DatasetUtils::Main();
    return 0;
  }

  SetupSignalHandler();

  auto& environment = dingodb::benchmark::Environment::GetInstance();
  if (!environment.Init()) {
    return 1;
  }

  auto benchmark = dingodb::benchmark::Benchmark::New(environment.GetCoordinatorProxy(), environment.GetClient());

  environment.AddBenchmark(benchmark);

  benchmark->Run();

  return 0;
}