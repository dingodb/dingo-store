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

#include "benchmark/benchmark.h"
#include "fmt/core.h"
#include "gflags/gflags.h"

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