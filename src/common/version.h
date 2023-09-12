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

#ifndef DINGODB_COMMON_VERSION_H_
#define DINGODB_COMMON_VERSION_H_

#include "gflags/gflags.h"

namespace dingodb {

#ifndef GIT_VERSION
#define GIT_VERSION "unknown"
#endif

#ifndef MAJOR_VERSION
#define MAJOR_VERSION "v0.8.0"
#endif

#ifndef MINOR_VERSION
#define MINOR_VERSION "0"
#endif

#ifndef GIT_TAG_NAME
#define GIT_TAG_NAME "v0.8.0"
#endif

#ifndef DINGO_BUILD_TYPE
#define DINGO_BUILD_TYPE "unknown"
#endif

#ifndef DINGO_CONTRIB_BUILD_TYPE
#define DINGO_CONTRIB_BUILD_TYPE "unknown"
#endif

DECLARE_bool(show_version);

void DingoShowVerion();
void DingoLogVerion();

}  // namespace dingodb

#endif  // DINGODB_COMMON_VERSION_H_
