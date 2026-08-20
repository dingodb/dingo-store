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

// Runtime-adjustable gflags: brpc's /flags service only accepts setvalue for
// flags carrying a registered gflags validator, and the validator doubles as
// the guard against nonsense values. google::SetCommandLineOption returns an
// empty string exactly when the validator rejects — that's the observable
// pinned here.

#include <gtest/gtest.h>

#include <string>

#include "gflags/gflags.h"

namespace dingodb {

DECLARE_int32(gc_compaction_filter_orphan_batch_keys);
DECLARE_int64(gc_compaction_filter_orphan_batch_bytes);
DECLARE_int32(gc_compaction_filter_orphan_queue_size);
DECLARE_int32(gc_compaction_filter_orphan_retry_num);

namespace {

bool SetFlag(const char* name, const std::string& value) {
  return !google::SetCommandLineOption(name, value.c_str()).empty();
}

}  // namespace

// Orphan cleaner sizing knobs are hot-read on every enqueue/flush; with a
// validator they become fully dynamic. Zero or negative batch/queue sizes
// would stall or drop the cleaner, so the validator must reject them.
TEST(DynamicGflagsTest, OrphanSizingFlagsRejectNonPositive) {
  struct Case {
    const char* name;
    int64_t old_value;
  };
  const Case cases[] = {
      {"gc_compaction_filter_orphan_batch_keys", FLAGS_gc_compaction_filter_orphan_batch_keys},
      {"gc_compaction_filter_orphan_batch_bytes", FLAGS_gc_compaction_filter_orphan_batch_bytes},
      {"gc_compaction_filter_orphan_queue_size", FLAGS_gc_compaction_filter_orphan_queue_size},
  };

  for (const auto& c : cases) {
    EXPECT_FALSE(SetFlag(c.name, "0")) << c.name;
    EXPECT_FALSE(SetFlag(c.name, "-1")) << c.name;

    EXPECT_TRUE(SetFlag(c.name, std::to_string(c.old_value + 1))) << c.name;
    EXPECT_TRUE(SetFlag(c.name, std::to_string(c.old_value))) << c.name;
  }

  EXPECT_EQ(FLAGS_gc_compaction_filter_orphan_batch_keys, cases[0].old_value);
  EXPECT_EQ(FLAGS_gc_compaction_filter_orphan_batch_bytes, cases[1].old_value);
  EXPECT_EQ(FLAGS_gc_compaction_filter_orphan_queue_size, cases[2].old_value);
}

// Retry count tolerates 0 (the flush path clamps with max(1, flag)), but a
// negative count is meaningless.
TEST(DynamicGflagsTest, OrphanRetryNumRejectsNegative) {
  const int32_t old_value = FLAGS_gc_compaction_filter_orphan_retry_num;

  EXPECT_FALSE(SetFlag("gc_compaction_filter_orphan_retry_num", "-1"));
  EXPECT_TRUE(SetFlag("gc_compaction_filter_orphan_retry_num", "0"));
  EXPECT_EQ(FLAGS_gc_compaction_filter_orphan_retry_num, 0);

  EXPECT_TRUE(SetFlag("gc_compaction_filter_orphan_retry_num", std::to_string(old_value)));
  EXPECT_EQ(FLAGS_gc_compaction_filter_orphan_retry_num, old_value);
}

}  // namespace dingodb
