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

#include "butil/strings/stringprintf.h"

namespace dingodb {

void DingoLogger::InitLogger(const std::string& log_dir, const std::string& role) {
  FLAGS_logbufsecs = 0;
  FLAGS_max_log_size = 80;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;

  const std::string program_name = butil::StringPrintf("./%s", role.c_str());
  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(google::GLOG_INFO,
                            butil::StringPrintf("%s/%s.info.log.", log_dir.c_str(), role.c_str()).c_str());
  google::SetLogDestination(google::GLOG_WARNING,
                            butil::StringPrintf("%s/%s.warn.log.", log_dir.c_str(), role.c_str()).c_str());
  google::SetLogDestination(google::GLOG_ERROR,
                            butil::StringPrintf("%s/%s.error.log.", log_dir.c_str(), role.c_str()).c_str());
  google::SetLogDestination(google::GLOG_FATAL,
                            butil::StringPrintf("%s/%s.fatal.log.", log_dir.c_str(), role.c_str()).c_str());
}

void DingoLogger::SetMinLogLevel(int level) { FLAGS_minloglevel = level; }

int DingoLogger::GetMinLogLevel() { return FLAGS_minloglevel; }

void DingoLogger::SetMinVerboseLevel(int v) { FLAGS_v = v; }

int DingoLogger::GetMinVerboseLevel() { return FLAGS_v; }

}  // namespace dingodb

