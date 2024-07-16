
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

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "CLI/CLI.hpp"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "client_v2/coordinator.h"
#include "client_v2/dump.h"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "client_v2/kv.h"
#include "client_v2/meta.h"
#include "client_v2/store.h"
#include "client_v2/tools.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "proto/common.pb.h"

const std::string kProgramName = "dingodb_cli";
const std::string kProgramDesc = "dingo-store client tool.";

void PrintSubcommandHelp(const CLI::App& app, const std::string& subcommand_name) {
  CLI::App* subcommand = app.get_subcommand(subcommand_name);
  if (subcommand) {
    std::cout << subcommand->help() << std::endl;
  } else {
    std::cout << "Unknown subcommand: " << subcommand_name << std::endl;
  }
}

int InteractiveCli(CLI::App& app) {
  while (true) {
    std::cout << "> ";
    std::string input;
    std::getline(std::cin, input);

    if (input.empty()) {
      continue;
    }

    if (input == "exit" || input == "quit") {
      break;
    }

    if (input == "help") {
      std::cout << app.help() << std::endl;
      continue;
    } else if (input.rfind("help", 0) == 0) {
      std::string subcommand_name = input.substr(5);
      if (subcommand_name.empty()) {
        std::cout << app.help() << std::endl;
      } else {
        PrintSubcommandHelp(app, subcommand_name);
      }
      continue;
    }

    std::vector<std::string> args = {"dingo_client_v2"};
    std::istringstream iss(input);
    for (std::string s; iss >> s;) {
      args.push_back(s);
    }

    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (auto& arg : args) {
      argv.push_back(arg.data());
    }
    argv.push_back(nullptr);
    try {
      app.parse(argv.size() - 1, argv.data());
    } catch (const CLI::ParseError& e) {
      std::cout << "Error: " << e.what() << std::endl;
    }
  }

  return 0;
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

  google::InitGoogleLogging(kProgramName.c_str());
  google::SetLogDestination(google::GLOG_INFO, fmt::format("{}/{}.info.log.", log_dir, kProgramName).c_str());
  google::SetLogDestination(google::GLOG_WARNING, fmt::format("{}/{}.warn.log.", log_dir, kProgramName).c_str());
  google::SetLogDestination(google::GLOG_ERROR, fmt::format("{}/{}.error.log.", log_dir, kProgramName).c_str());
  google::SetLogDestination(google::GLOG_FATAL, fmt::format("{}/{}.fatal.log.", log_dir, kProgramName).c_str());
  google::SetStderrLogging(google::GLOG_FATAL);
}

int main(int argc, char* argv[]) {
  InitLog("./log");

  CLI::App app{kProgramDesc, kProgramName};
  app.get_formatter()->column_width(40);
  client_v2::SetUpCoordinatorSubCommands(app);
  client_v2::SetUpKVSubCommands(app);
  client_v2::SetUpMetaSubCommands(app);
  client_v2::SetUpStoreSubCommands(app);
  client_v2::SetUpToolSubCommands(app);

  if (argc > 1) {
    CLI11_PARSE(app, argc, argv);

  } else {
    InteractiveCli(app);
  }

  return 0;
}
