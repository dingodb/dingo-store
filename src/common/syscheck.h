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

#ifndef DINGODB_SYSCHECK_H_
#define DINGODB_SYSCHECK_H_

#include <string>

namespace dingodb {

#ifdef __linux__
int CheckTHPEnabled(std::string &error_msg);
int CheckOvercommit(std::string &error_msg);
#endif

int DoSystemCheck();

}  // namespace dingodb

#endif  // DINGODB_SYSCHECK_H_
