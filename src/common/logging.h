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

#ifndef DINGODB_COMMON_LOGGING_H_
#define DINGODB_COMMON_LOGGING_H_

#include "glog/logging.h"

namespace dingodb {

/**
 * define the debug log level.
 * The larger the number, the more comprehensive information is displayed.
 */
#define DEBUG 88

#define DINGO_LOG(level) DINGO_LOG_##level

#define DINGO_LOG_INFO LOG(INFO)
#define DINGO_LOG_WARNING LOG(WARNING)
#define DINGO_LOG_ERROR LOG(ERROR)
#define DINGO_LOG_FATAL LOG(FATAL)
#define DINGO_LOG_DEBUG VLOG(DEBUG)

class DingoLogger {
 public:
  static void InitLogger(const std::string& log_dir, const std::string& role);
  static void SetMinLogLevel(int level);
  static int GetMinLogLevel();
  static void SetMinVerboseLevel(int v);
  static int GetMinVerboseLevel();
};

}  // namespace dingodb
#endif

