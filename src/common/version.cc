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

#include "common/version.h"

#include "common/logging.h"
#include "gflags/gflags_declare.h"

namespace dingodb {

DEFINE_string(git_commit_hash, GIT_VERSION, "current git commit version");
DEFINE_string(git_tag_name, GIT_TAG_NAME, "current dingo git tag version");
DEFINE_string(major_version, MAJOR_VERSION, "current dingo major version");
DEFINE_string(minor_version, MINOR_VERSION, "current dingo mino version");
DEFINE_string(dingo_build_type, DINGO_BUILD_TYPE, "current dingo build type");
DEFINE_string(dingo_contrib_build_type, DINGO_CONTRIB_BUILD_TYPE, "current dingo contrib build type");

void DingoShowVerion() {
  printf("DINGO_STORE VERSION:[%s-%s]\n", FLAGS_major_version.c_str(), FLAGS_minor_version.c_str());
  printf("DINGO_STORE GIT_TAG_VERSION:[%s]\n", FLAGS_git_tag_name.c_str());
  printf("DINGO_STORE GIT_COMMIT_HASH:[%s]\n", FLAGS_git_commit_hash.c_str());
  printf("DINGO_STORE BUILD_TYPE:[%s] CONTRIB_BUILD_TYPE:[%s]\n", FLAGS_dingo_build_type.c_str(),
         FLAGS_dingo_contrib_build_type.c_str());
}

void DingoLogVerion() {
  DINGO_LOG(INFO) << "DINGO_STORE VERSION:[" << FLAGS_major_version << "-" << FLAGS_minor_version << "]";
  DINGO_LOG(INFO) << "DINGO_STORE GIT_TAG_VERSION:[" << FLAGS_git_tag_name << "]";
  DINGO_LOG(INFO) << "DINGO_STORE GIT_COMMIT_HASH:[" << FLAGS_git_commit_hash << "]";
  DINGO_LOG(INFO) << "DINGO_STORE BUILD_TYPE:[" << FLAGS_dingo_build_type << "] CONTRIB_BUILD_TYPE:["
                  << FLAGS_dingo_contrib_build_type << "]";
}

DEFINE_bool(show_version, false, "Print DingoStore version Flag");

}  // namespace dingodb
