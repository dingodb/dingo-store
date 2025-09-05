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

#ifndef DINGODB_COMMON_GFLAG_VALIDATATOR_H_
#define DINGODB_COMMON_GFLAG_VALIDATATOR_H_

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace dingodb {

static bool PassDouble(const char*, double) { return true; }
static bool PassInt64(const char*, int64_t) { return true; }
static bool PassUint64(const char*, uint64_t) { return true; }
static bool PassInt32(const char*, int32_t) { return true; }
static bool PassUint32(const char*, uint32_t) { return true; }
static bool PassBool(const char*, bool) { return true; }

}  // namespace dingodb

#endif  // DINGODB_COMMON_GFLAG_VALIDATATOR_H_
