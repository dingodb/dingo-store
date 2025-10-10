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

#include "butil/string_printf.h"
#include "common/logging.h"

#if __has_include("dingo_eureka_version.h")
#include "dingo_eureka_version.h"
#define DINGO_EUREKA_VERSION_EXIST 1
#else
#define DINGO_EUREKA_VERSION_EXIST 0
#endif

namespace dingodb {

#if defined(ENABLE_DISKANN_MODULE)
#if ENABLE_DISKANN_MODULE == 1
#define USE_DISKANN true
#else
#define USE_DISKANN false
#endif
#else  // #if defined(ENABLE_DISKANN_MODULE)
#define USE_DISKANN false
#endif

#if defined(DISKANN_DEPEND_ON_SYSTEM)
#if DISKANN_DEPEND_ON_SYSTEM == 1
#define ENABLE_DISKANN_DEPEND_ON_SYSTEM true
#else
#define ENABLE_DISKANN_DEPEND_ON_SYSTEM false
#endif
#else  // #if defined(DISKANN_DEPEND_ON_SYSTEM)
#define ENABLE_DISKANN_DEPEND_ON_SYSTEM false
#endif

#if !defined(BOOST_SUMMARY)
#define BOOST_SUMMARY ""
#endif

DEFINE_string(git_commit_hash, GIT_VERSION, "current git commit version");
DEFINE_string(git_tag_name, GIT_TAG_NAME, "current dingo git tag version");
DEFINE_string(git_commit_user, GIT_COMMIT_USER, "current dingo git commit user");
DEFINE_string(git_commit_mail, GIT_COMMIT_MAIL, "current dingo git commit mail");
DEFINE_string(git_commit_time, GIT_COMMIT_TIME, "current dingo git commit time");
DEFINE_string(git_submodule, GIT_SUBMODULE, "current dingo git submodule");
DEFINE_string(major_version, MAJOR_VERSION, "current dingo major version");
DEFINE_string(minor_version, MINOR_VERSION, "current dingo mino version");
DEFINE_string(dingo_build_type, DINGO_BUILD_TYPE, "current dingo build type");
DEFINE_string(dingo_contrib_build_type, DINGO_CONTRIB_BUILD_TYPE, "current dingo contrib build type");
DEFINE_bool(use_mkl, false, "use mkl");
DEFINE_bool(use_openblas, false, "use openblas");
DEFINE_bool(use_tcmalloc, false, "use tcmalloc");
DEFINE_bool(use_profiler, false, "use profiler");
DEFINE_bool(use_sanitizer, false, "use sanitizer");
DEFINE_bool(use_diskann, USE_DISKANN, "use diskann");
DEFINE_bool(diskann_depend_on_system, ENABLE_DISKANN_DEPEND_ON_SYSTEM,
            "if true, diskann depend on system, else use third_party.");
DEFINE_string(boost_summary, BOOST_SUMMARY, "boost summary");
DEFINE_bool(with_vector_index_use_document, false, "use vector index use document");

std::string GetBuildFlag() {
#ifdef USE_MKL
  FLAGS_use_mkl = true;
#else
  FLAGS_use_mkl = false;
#endif

#ifdef USE_OPENBLAS
  FLAGS_use_openblas = true;
#else
  FLAGS_use_openblas = false;
#endif

#ifdef LINK_TCMALLOC
  FLAGS_use_tcmalloc = true;
#else
  FLAGS_use_tcmalloc = false;
#endif

#ifdef BRPC_ENABLE_CPU_PROFILER
  FLAGS_use_profiler = true;
#else
  FLAGS_use_profiler = false;
#endif

#ifdef USE_SANITIZE
  FLAGS_use_sanitizer = true;
#else
  FLAGS_use_sanitizer = false;
#endif

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  FLAGS_with_vector_index_use_document = true;
#else
  FLAGS_with_vector_index_use_document = false;
#endif

  return butil::string_printf(
      "DINGO_STORE USE_MKL:[%s] USE_OPENBLAS:[%s] LINK_TCMALLOC:[%s] BRPC_ENABLE_CPU_PROFILER:[%s] "
      "USE_SANITIZE:[%s]\nDINGO_STORE USE_DISKANN:[%s]  DISKANN_DEPEND_ON_SYSTEM:[%s] "
      "WITH_VECTOR_INDEX_USE_DOCUMENT:[%s]\nDINGO_STORE "
      "BOOST_SUMMARY:[%s]\n",
      FLAGS_use_mkl ? "ON" : "OFF", FLAGS_use_openblas ? "ON" : "OFF", FLAGS_use_tcmalloc ? "ON" : "OFF",
      FLAGS_use_profiler ? "ON" : "OFF", FLAGS_use_sanitizer ? "ON" : "OFF", FLAGS_use_diskann ? "ON" : "OFF",
      FLAGS_diskann_depend_on_system ? "ON" : "OFF", FLAGS_with_vector_index_use_document ? "ON" : "OFF",
      FLAGS_boost_summary.c_str());
}

static void ReplaceAll(std::string& str, const std::string& from, const std::string& to) {
  if (from.empty()) return;

  size_t start_pos = 0;
  while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
}

void DingoEurekaShowVerion() {
#if DINGO_EUREKA_VERSION_EXIST
  std::cout << FormatDingoEurekaVersion() << std::endl;
#endif
}

void DingoShowVerion() {
  printf("DINGO_STORE VERSION:[%s-%s]\n", FLAGS_major_version.c_str(), FLAGS_minor_version.c_str());
  printf("DINGO_STORE GIT_TAG_VERSION:[%s]\n", FLAGS_git_tag_name.c_str());
  printf("DINGO_STORE GIT_COMMIT_HASH:[%s]\n", FLAGS_git_commit_hash.c_str());
  printf("DINGO_STORE BUILD_TYPE:[%s] CONTRIB_BUILD_TYPE:[%s]\n", FLAGS_dingo_build_type.c_str(),
         FLAGS_dingo_contrib_build_type.c_str());
  printf("%s", GetBuildFlag().c_str());
  std::string git_submodule = FLAGS_git_submodule;
  ReplaceAll(git_submodule, "\t", "\n");
  printf("DINGO_STORE GIT_SUBMODULE:[\n%s]\n", git_submodule.c_str());
  DingoEurekaShowVerion();
}

void DingoEurekaLogVerion() {
#if DINGO_EUREKA_VERSION_EXIST
  DINGO_LOG(INFO) << FormatDingoEurekaVersion();
#endif
}

void DingoLogVerion() {
  DINGO_LOG(INFO) << "DINGO_STORE VERSION:[" << FLAGS_major_version << "-" << FLAGS_minor_version << "]";
  DINGO_LOG(INFO) << "DINGO_STORE GIT_TAG_VERSION:[" << FLAGS_git_tag_name << "]";
  DINGO_LOG(INFO) << "DINGO_STORE GIT_COMMIT_HASH:[" << FLAGS_git_commit_hash << "]";
  DINGO_LOG(INFO) << "DINGO_STORE BUILD_TYPE:[" << FLAGS_dingo_build_type << "] CONTRIB_BUILD_TYPE:["
                  << FLAGS_dingo_contrib_build_type << "]";
  DINGO_LOG(INFO) << GetBuildFlag();
  std::string git_submodule = FLAGS_git_submodule;
  ReplaceAll(git_submodule, "\t", "\n");
  DINGO_LOG(INFO) << "DINGO_STORE GIT_SUBMODULE:[\n" << git_submodule << "]";
  DINGO_LOG(INFO) << "PID: " << getpid();
  DingoEurekaLogVerion();
}

pb::common::VersionInfo GetVersionInfo() {
  pb::common::VersionInfo version_info;
  version_info.set_git_commit_hash(FLAGS_git_commit_hash);
  version_info.set_git_tag_name(FLAGS_git_tag_name);
  version_info.set_git_commit_user(FLAGS_git_commit_user);
  version_info.set_git_commit_mail(FLAGS_git_commit_mail);
  version_info.set_git_commit_time(FLAGS_git_commit_time);
  version_info.set_major_version(FLAGS_major_version);
  version_info.set_minor_version(FLAGS_minor_version);
  version_info.set_dingo_build_type(FLAGS_dingo_build_type);
  version_info.set_dingo_contrib_build_type(FLAGS_dingo_contrib_build_type);
  version_info.set_use_mkl(FLAGS_use_mkl);
  version_info.set_use_openblas(FLAGS_use_openblas);
  version_info.set_use_tcmalloc(FLAGS_use_tcmalloc);
  version_info.set_use_profiler(FLAGS_use_profiler);
  version_info.set_use_sanitizer(FLAGS_use_sanitizer);
  version_info.set_use_diskann(FLAGS_use_diskann);
  version_info.set_diskann_depend_on_system(FLAGS_diskann_depend_on_system);
  version_info.set_boost_summary(FLAGS_boost_summary);
  return version_info;
}

DEFINE_bool(show_version, false, "Print DingoStore version Flag");

}  // namespace dingodb
