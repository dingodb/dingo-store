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

#ifndef DINGODB_COMMON_FAILPOINT_H_
#define DINGODB_COMMON_FAILPOINT_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "bthread/mutex.h"
#include "butil/status.h"

namespace dingodb {

class FailPointManager;

#if defined(ENABLE_FAILPOINT)

#define FAIL_POINT(name)                                  \
  do {                                                    \
    FailPointManager::GetInstance().ApplyFailPoint(name); \
  } while (0)

#else

#define FAIL_POINT(name)

#endif

// Action base class.
class Action {
 public:
  using ActionType = std::string;

  Action(uint32_t percent, int max_count, ActionType type, const std::string& arg)
      : percent_(percent), max_count_(max_count), type_(type), arg_(arg) {}
  virtual ~Action() = default;

  butil::Status Run();
  virtual butil::Status Execute(const std::string& arg) = 0;

  uint32_t Percent() const { return percent_; }
  uint32_t MaxCount() const { return max_count_; }
  uint32_t Count() const { return count_; }
  ActionType GetType() const { return type_; }
  std::string Arg() const { return arg_; };

 private:
  uint32_t percent_;    // Random execute percent
  uint32_t max_count_;  // Max execute times
  uint32_t count_{0};   // Current execute times
  ActionType type_;     // Action type, like panic/print/sleep/delay/yield
  std::string arg_;     // Action execute parameter
};

// Panic
class PanicAction : public Action {
 public:
  PanicAction(uint32_t percent, int max_count, const std::string& arg) : Action(percent, max_count, "panic", arg) {}
  ~PanicAction() override = default;

  butil::Status Execute(const std::string& arg) override;
};

// Idle waiting a little time.
class SleepAction : public Action {
 public:
  SleepAction(uint32_t percent, int max_count, const std::string& arg) : Action(percent, max_count, "sleep", arg) {}
  ~SleepAction() override = default;

  butil::Status Execute(const std::string& arg) override;
};

// Print some message by arg.
class PrintAction : public Action {
 public:
  PrintAction(uint32_t percent, int max_count, const std::string& arg) : Action(percent, max_count, "print", arg) {}
  ~PrintAction() override = default;

  butil::Status Execute(const std::string& arg) override;
};

// Yield bthread.
class YieldAction : public Action {
 public:
  YieldAction(uint32_t percent, int max_count, const std::string& arg) : Action(percent, max_count, "yield", arg) {}
  ~YieldAction() override = default;

  butil::Status Execute(const std::string& arg) override;
};

// Busy waiting a little time.
class DelayAction : public Action {
 public:
  DelayAction(uint32_t percent, int max_count, const std::string& arg) : Action(percent, max_count, "delay", arg) {}
  ~DelayAction() override = default;

  butil::Status Execute(const std::string& arg) override;
};

// A failpoint have mutil action.
// When trigger failpoint, run every action.
class FailPoint {
 public:
  FailPoint(std::string name, std::string config) : name_(name), config_(config) {}
  ~FailPoint() = default;

  // Init failpoint, set action from config.
  bool Init();

  // Run action chain.
  butil::Status Run();

  const std::string& Name() const { return name_; }
  const std::string& Config() const { return config_; }
  std::vector<std::shared_ptr<Action>> GetActions() { return actions_; }

  // Parse failpoint action config
  static std::vector<std::string> ParseConfig(const std::string& config);

 private:
  std::string name_;
  std::string config_;
  std::vector<std::shared_ptr<Action>> actions_;
};

// Manage all failpoint.
class FailPointManager {
 public:
  FailPointManager() { bthread_mutex_init(&mutex_, nullptr); }
  ~FailPointManager() { bthread_mutex_destroy(&mutex_); }

  static FailPointManager& GetInstance();

  butil::Status ApplyFailPoint(const std::string& name);

  // config: [<pct>%][<cnt>*]<type>[(args...)][-><more terms>]
  // example: 50%10*sleep(10)->60%6*panic
  butil::Status ConfigureFailPoint(const std::string& name, const std::string& config);

  butil::Status DeleteFailPoint(const std::string& name);

  std::shared_ptr<FailPoint> GetFailPoint(const std::string& name);
  std::vector<std::shared_ptr<FailPoint>> GetAllFailPoints();

 private:
  bthread_mutex_t mutex_;
  std::unordered_map<std::string, std::shared_ptr<FailPoint>> failpoints_;
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_FAILPOINT_H_