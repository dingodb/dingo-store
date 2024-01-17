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
#include <ostream>
#include <string>

#include "benchmark/benchmark.h"
#include "fmt/core.h"
#include "gflags/gflags.h"

const std::string kVersion = "0.1.0";

static std::string GetUsageMessage() {
  std::string message;

  message += "\nUsage:";
  message += "\n  --coordinator_url dingo-store cluster endpoint, default(file://./coor_list)";
  message += "\n  --benchmark benchmark type, default(fillseq)";
  message += "\n  --show_version show dingo-store cluster version info, default(false)";
  message += "\n  --prefix region range prefix, used to distinguish region, default(BENCH)";
  message += "\n  --region_num region number, default(1)";
  message += "\n  --concurrency concurrency as thread number, default(1)";
  message += "\n  --req_num invoke RPC request number, default(10000)";
  message += "\n  --delay print benchmark metrics interval time, unit(second), default(2)";
  message += "\n  --timelimit the limit of run time, 0 is no limit, unit(second), default(0)";
  message += "\n  --key_size key size, default(64)";
  message += "\n  --value_size value size, default(256)";
  message += "\n  --batch_size batch put size, default(1)";
  message += "\n  --arrange_kv_num the number of arrange kv, used by readseq/readrandom/readmissing, default(10000)";

  return message;
}

static void SignalHandler(int signo) {  // NOLINT
  // std::cerr << "Received signal " << signo << std::endl;

  dingodb::benchmark::Environment::GetInstance().Stop();
}

void SetupSignalHandler() {
  sighandler_t s;
  s = signal(SIGTERM, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGTERM\n");
    exit(-1);
  }
  s = signal(SIGABRT, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGABRT\n");
    exit(-1);
  }
  s = signal(SIGINT, SignalHandler);
  if (s == SIG_ERR) {
    printf("Failed to setup signal handler for SIGINT\n");
    exit(-1);
  }
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_ERROR;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  google::InitGoogleLogging(argv[0]);

  google::SetVersionString(kVersion);
  google::SetUsageMessage(GetUsageMessage());
  google::ParseCommandLineFlags(&argc, &argv, true);

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