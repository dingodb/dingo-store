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

#include "common/logging.h"

#include <cstdint>
#include <iomanip>

#include "fmt/core.h"
#include "proto/node.pb.h"

namespace dingodb {

void DingoLogger::InitLogger(const std::string& log_dir, const std::string& role, const pb::node::LogLevel& level) {
  FLAGS_logbufsecs = 0;
  FLAGS_max_log_size = 80;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  ChangeGlogLevelUsingDingoLevel(level, kGlobalValueOfDebug);

  const std::string program_name = fmt::format("./{}", role);
  google::InitGoogleLogging(program_name.c_str(), &CustomLogFormatPrefix);
  google::SetLogDestination(google::GLOG_INFO, fmt::format("{}/{}.info.log.", log_dir, role).c_str());
  google::SetLogDestination(google::GLOG_WARNING, fmt::format("{}/{}.warn.log.", log_dir, role).c_str());
  google::SetLogDestination(google::GLOG_ERROR, fmt::format("{}/{}.error.log.", log_dir, role).c_str());
  google::SetLogDestination(google::GLOG_FATAL, fmt::format("{}/{}.fatal.log.", log_dir, role).c_str());
}

void DingoLogger::SetMinLogLevel(int level) { FLAGS_minloglevel = level; }

int DingoLogger::GetMinLogLevel() { return FLAGS_minloglevel; }

void DingoLogger::SetMinVerboseLevel(int v) { FLAGS_v = v; }

int DingoLogger::GetMinVerboseLevel() { return FLAGS_v; }

int DingoLogger::GetLogBuffSecs() { return FLAGS_logbufsecs; }

void DingoLogger::SetLogBuffSecs(uint32_t secs) { FLAGS_logbufsecs = secs; }

int32_t DingoLogger::GetMaxLogSize() { return FLAGS_max_log_size; }
void DingoLogger::SetMaxLogSize(uint32_t max_log_size) { FLAGS_max_log_size = max_log_size; }

bool DingoLogger::GetStoppingWhenDiskFull() { return FLAGS_stop_logging_if_full_disk; }

void DingoLogger::SetStoppingWhenDiskFull(bool is_stop) { FLAGS_stop_logging_if_full_disk = is_stop; }

void DingoLogger::CustomLogFormatPrefix(std::ostream& s, const google::LogMessageInfo& l, void*) {
  s << "[" << l.severity[0] << std::setw(4) << "][" << 1900 + l.time.year() << std::setw(2) << 1 + l.time.month()
    << std::setw(2) << l.time.day() << ' ' << std::setw(2) << l.time.hour() << ':' << std::setw(2) << l.time.min()
    << ':' << std::setw(2) << l.time.sec() << "." << std::setw(6) << l.time.usec() << std::setw(5) << "]["
    << l.thread_id << "][" << l.filename << ':' << l.line_number << "]";
}

void DingoLogger::ChangeGlogLevelUsingDingoLevel(const pb::node::LogLevel& log_level, uint32_t verbose) {
  if (log_level == pb::node::DEBUG) {
    DingoLogger::SetMinLogLevel(0);
    DingoLogger::SetMinVerboseLevel(verbose == 0 ? kGlobalValueOfDebug : verbose);
  } else {
    DingoLogger::SetMinLogLevel(static_cast<int>(log_level) - 1);
    DingoLogger::SetMinVerboseLevel(1);
  }
}

}  // namespace dingodb
