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

#include "common/syscheck.h"

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "common/logging.h"

#ifdef __linux__
#include <sys/mman.h>
#endif

namespace dingodb {

#ifdef __linux__

#define NUM_FILE 1048576
#define NUM_PROC 1048576

static std::string ReadSysfsLine(char *path) {
  std::string ret_value;
  char buf[256];
  FILE *f = fopen(path, "r");
  if (!f) {
    return ret_value;
  }

  if (!fgets(buf, sizeof(buf), f)) {
    auto ret = fclose(f);
    return ret_value;
  }

  auto ret = fclose(f);

  ret_value = std::string(buf);

  DINGO_LOG(INFO) << "ReadSysfsLine path:[" << path << "] value:[" << ret_value << "]";

  return ret_value;
}

/* Check if overcommit is enabled.
 * When overcommit memory is disabled Linux will kill the forked child of a background save
 * if we don't have enough free memory to satisfy double the current memory usage even though
 * the forked child uses copy-on-write to reduce its actual memory usage. */
int CheckOvercommit(std::string &error_msg) {
  FILE *fp = fopen("/proc/sys/vm/overcommit_memory", "r");
  char buf[64];

  if (!fp) return 0;
  if (fgets(buf, 64, fp) == nullptr) {
    auto ret = fclose(fp);
    return 0;
  }
  auto ret = fclose(fp);

  if (strtol(buf, nullptr, 10) != 1) {
    error_msg = std::string(
        "Memory overcommit must be enabled! Without it, a background save may fail under low memory condition. To fix "
        "this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl "
        "vm.overcommit_memory=1' for this to take effect.");
    return -1;
  } else {
    return 1;
  }
}

/*  Check if THP is enabled
 * Make sure transparent huge pages aren't always enabled. When they are this can cause copy-on-write logic
 * to consume much more memory and reduce performance during forks. */
int CheckThpEnabled(std::string &error_msg) {
  char buf[1024];

  FILE *fp = fopen("/sys/kernel/mm/transparent_hugepage/enabled", "r");
  if (!fp) return 0;
  if (fgets(buf, sizeof(buf), fp) == nullptr) {
    auto ret = fclose(fp);
    return 0;
  }
  auto ret = fclose(fp);

  if (strstr(buf, "[always]") != nullptr) {
    error_msg = std::string(
        "You have Transparent Huge Pages (THP) support enabled in your kernel. "
        "This will create latency and memory usage issues with DingoDB. "
        "To fix this issue run the command 'echo madvise > /sys/kernel/mm/transparent_hugepage/enabled' as root, "
        "and add it to your /etc/rc.local in order to retain the setting after a reboot. "
        "DingoDB must be restarted after THP is disabled (set to 'madvise' or 'never').");
    return -1;
  } else {
    return 1;
  }
}

int CheckNproc(std::string &error_msg) {
  struct rlimit rlim;
  if (getrlimit(RLIMIT_NPROC, &rlim) == 0) {
    DINGO_LOG(INFO) << "NPROC rlim_cur:[" << rlim.rlim_cur << "] rlim_max:[" << rlim.rlim_max << "]";

    if (rlim.rlim_cur < NUM_PROC) {
      DINGO_LOG(ERROR) << "NPROC rlim_cur:[" << rlim.rlim_cur << "] rlim_max:[" << rlim.rlim_max << "]";

      struct rlimit new_rlim;
      new_rlim.rlim_cur = NUM_PROC;
      new_rlim.rlim_max = NUM_PROC;
      auto ret = setrlimit(RLIMIT_NPROC, &rlim);
      if (ret < 0) {
        error_msg = std::string("ulimit -u: ") + std::to_string(rlim.rlim_cur) + std::string(" rlim_max: ") +
                    std::to_string(rlim.rlim_max) + std::string(" rlim_max should be at least 800000");
        return -1;
      } else {
        DINGO_LOG(ERROR) << "NPROC is set to rlim_cur:[" << new_rlim.rlim_cur << "] rlim_max:[" << new_rlim.rlim_max
                         << "]";
        return 1;
      }
    }
  } else {
    error_msg = std::string("Cannot get nproc limit, DingoDB may not be able to fork().");
    return -1;
  }

  return 1;
}

int CheckNofile(std::string &error_msg) {
  struct rlimit rlim;
  if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
    DINGO_LOG(INFO) << "NOFILE rlim_cur:[" << rlim.rlim_cur << "] rlim_max:[" << rlim.rlim_max << "]";
    if (rlim.rlim_cur < NUM_FILE) {
      DINGO_LOG(ERROR) << "NOFILE rlim_cur:[" << rlim.rlim_cur << "] rlim_max:[" << rlim.rlim_max << "]";

      struct rlimit new_rlim;
      new_rlim.rlim_cur = NUM_FILE;
      new_rlim.rlim_max = NUM_FILE;
      auto ret = setrlimit(RLIMIT_NOFILE, &rlim);
      if (ret < 0) {
        error_msg = std::string("ulimit -n: ") + std::to_string(rlim.rlim_cur) + std::string(" rlim_max: ") +
                    std::to_string(rlim.rlim_max) + std::string(" rlim_max should be at least 800000");
        return -1;
      } else {
        DINGO_LOG(ERROR) << "NOFILE is set to rlim_cur:[" << new_rlim.rlim_cur << "] rlim_max:[" << new_rlim.rlim_max
                         << "]";
        return 1;
      }
    }
  } else {
    error_msg = std::string("Cannot get nofile limit, DingoDB may not be able to fork().");
    return -1;
  }

  return 1;
}

#endif /* __linux__ */

/*
 * Each check has a name `name` and a functions pointer `CheckFunction`.
 * `CheckFunction` should return:
 *   -1 in case the check fails.
 *   1 in case the check passes.
 *   0 in case the check could not be completed (usually because of some unexpected failed system call).
 */
using SysCheckFunctions = struct {
  const char *name;
  int (*CheckFunction)(std::string &);
};

SysCheckFunctions sys_check_functions[] = {
#ifdef __linux__
    {.name = "overcommit", .CheckFunction = CheckOvercommit},
    {.name = "THP", .CheckFunction = CheckThpEnabled},
    {.name = "nofile", .CheckFunction = CheckNofile},
    {.name = "nproc", .CheckFunction = CheckNproc},
#endif
    {.name = nullptr, .CheckFunction = nullptr}};

// Do system checks
int DoSystemCheck() {
  SysCheckFunctions *sys_check = sys_check_functions;
  int result = 0;
  std::string err_msg;
  while (sys_check->CheckFunction) {
    auto ret = sys_check->CheckFunction(err_msg);
    if (ret == 0) {
      DINGO_LOG(INFO) << "Syscheck skipped:[" << sys_check->name << "]";
    } else if (ret == 1) {
      DINGO_LOG(INFO) << "Syscheck OK:[" << sys_check->name << "]";
    } else {
      DINGO_LOG(ERROR) << "Syscheck failed:[" << sys_check->name << "] err_msg:[" << err_msg << "]";
      ret = 0;
    }

    if (result > ret) {
      result = ret;
    }

    sys_check++;
  }

  return result;
}

}  // namespace dingodb
