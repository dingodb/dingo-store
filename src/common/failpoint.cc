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

#include "common/failpoint.h"

#include <functional>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/bthread.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"

namespace dingodb {

// Action builder type
using ActionBuilder = std::function<std::shared_ptr<Action>(int, int, std::string)>;
using ActionBuilderMap = std::unordered_map<std::string, ActionBuilder>;

static const ActionBuilderMap kActionBuilders = {
    {"panic",
     [](uint32_t percent, int max_count, const std::string& arg) -> std::shared_ptr<Action> {
       return std::make_shared<PanicAction>(percent, max_count, arg);
     }},
    {"print",
     [](uint32_t percent, int max_count, const std::string& arg) -> std::shared_ptr<Action> {
       return std::make_shared<PrintAction>(percent, max_count, arg);
     }},
    {"sleep",
     [](uint32_t percent, int max_count, const std::string& arg) -> std::shared_ptr<Action> {
       return std::make_shared<SleepAction>(percent, max_count, arg);
     }},
    {"delay",
     [](uint32_t percent, int max_count, const std::string& arg) -> std::shared_ptr<Action> {
       return std::make_shared<DelayAction>(percent, max_count, arg);
     }},
    {"yield", [](uint32_t percent, int max_count, const std::string& arg) -> std::shared_ptr<Action> {
       return std::make_shared<YieldAction>(percent, max_count, arg);
     }}};

butil::Status Action::Run() {
  if (max_count_ != 0 && count_ >= max_count_) {
    return butil::Status(pb::error::EFAIL_POINT, "The maximum number of times was reached");
  }

  if (Helper::GenerateRandomInteger(0, 100) > percent_) {
    return butil::Status(pb::error::EFAIL_POINT, "probability miss");
  }

  ++count_;

  return Execute(arg_);
}

butil::Status PanicAction::Execute(const std::string&) {
  // Use fatal log make server exit.
  DINGO_LOG(FATAL) << "Failpoint[Panic] panic...";
  return butil::Status();
}

butil::Status SleepAction::Execute(const std::string& arg) {
  int sleep_ms = arg.empty() ? 10 : std::stoi(arg);
  bthread_usleep(sleep_ms * 1000);
  return butil::Status();
}

butil::Status PrintAction::Execute(const std::string& arg) {
  DINGO_LOG(INFO) << "Failpoint[print]: " << arg;
  return butil::Status();
}

butil::Status YieldAction::Execute(const std::string&) {
  DINGO_LOG(INFO) << "Failpoint[Yield] yield...";
  bthread_yield();
  return butil::Status();
}

butil::Status DelayAction::Execute(const std::string& arg) {
  int delay_ms = arg.empty() ? 10 : std::stoi(arg);
  auto start_time = Helper::TimestampMs();
  for (;;) {
    // do work
    int count = 0;
    for (int i = 0; i < 10000; ++i) {
      count += count;
    }

    if (Helper::TimestampMs() - start_time >= delay_ms) {
      break;
    }
  }

  return butil::Status();
}

bool FailPoint::Init() {
  auto tokens = ParseConfig(config_);
  if (tokens.empty() || (tokens.size() % 4 != 0)) {
    DINGO_LOG(ERROR) << "Parse failpoint config failed, config: " << config_;
    return false;
  }

  for (int i = 0; i < tokens.size(); i += 4) {
    uint32_t percent = tokens[0].empty() ? 100 : std::stoi(tokens[0]);
    percent = percent <= 100 ? percent : 100;
    uint32_t max_count = tokens[1].empty() ? 0 : std::stoi(tokens[1]);
    std::string type = tokens[2];
    std::string arg = tokens[3];

    DINGO_LOG(DEBUG) << fmt::format("Failpoint action[{}] percent[{}] max_count[{}] arg[{}]", type, percent, max_count,
                                    arg);

    auto it = kActionBuilders.find(type);
    if (it == kActionBuilders.end()) {
      DINGO_LOG(ERROR) << "Unkown failpoint action " << type;
      continue;
    }

    auto action_builder = it->second;
    actions_.push_back(action_builder(percent, max_count, arg));
  }

  return true;
}

butil::Status FailPoint::Run() {
  for (auto& action : actions_) {
    auto status = action->Run();
    if (status.error_code() == pb::error::EFAIL_POINT_RETURN) {
      return status;
    }
  }

  return butil::Status();
}

std::vector<std::string> FailPoint::ParseConfig(const std::string& config) {
  std::regex e(R"(((\d+)\%)?((\d+)\*)?(sleep|panic|print|yield|delay)(\(([\w\s]+)\))?)");

  auto begin = std::sregex_iterator(config.begin(), config.end(), e);
  auto end = std::sregex_iterator();

  std::vector<std::string> result;
  for (auto i = begin; i != end; ++i) {
    const std::smatch& match = *i;
    DINGO_LOG(DEBUG) << "match_results: " << match.size() << " " << match.length() << '\n';
    result.push_back(match.str(2));
    result.push_back(match.str(4));
    result.push_back(match.str(5));
    result.push_back(match.str(7));
  }

  return result;
}

FailPointManager& FailPointManager::GetInstance() {
  static FailPointManager instance;
  return instance;
}

butil::Status FailPointManager::ApplyFailPoint(const std::string& name) {
  DINGO_LOG(INFO) << "ApplyFailPoint name: " << name;
  std::shared_ptr<FailPoint> failpoint = nullptr;
  {
    BAIDU_SCOPED_LOCK(mutex_);

    auto it = failpoints_.find(name);
    if (it == failpoints_.end()) {
      return butil::Status();
    }
    failpoint = it->second;
  }

  if (failpoint != nullptr) {
    return failpoint->Run();
  }

  return butil::Status();
}

butil::Status FailPointManager::ConfigureFailPoint(const std::string& name, const std::string& config) {
  auto failpoint = std::make_shared<FailPoint>(name, config);
  if (!failpoint->Init()) {
    DINGO_LOG(ERROR) << "Failpoint init failed, config: " << config;
    return butil::Status(pb::error::EFAIL_POINT, "Failpoint init failed");
  }

  {
    BAIDU_SCOPED_LOCK(mutex_);
    failpoints_.insert_or_assign(name, failpoint);
  }

  return butil::Status();
}

butil::Status FailPointManager::DeleteFailPoint(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);
  failpoints_.erase(name);

  return butil::Status();
}

std::shared_ptr<FailPoint> FailPointManager::GetFailPoint(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = failpoints_.find(name);
  return (it == failpoints_.end()) ? nullptr : it->second;
}

std::vector<std::shared_ptr<FailPoint>> FailPointManager::GetAllFailPoints() {
  BAIDU_SCOPED_LOCK(mutex_);

  std::vector<std::shared_ptr<FailPoint>> failpoints;
  failpoints.reserve(failpoints_.size());
  for (auto& [_, failpoint] : failpoints_) {
    failpoints.push_back(failpoint);
  }

  return failpoints;
}

}  // namespace dingodb
