// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "coordinator/coordinator_control.h"
#include "proto/error.pb.h"

// Unit tests for CoordinatorControl::DecideShadowAction.
//
// DecideShadowAction is the pure 4-step shadow-peer maintenance algorithm
// extracted from ChangePeerRegionWithJob's verify_peer_on_store=true branch.
// It is a pure function: no RPCs, no CoordinatorControl state, no logging.
// Each test case below maps to one row of the 15-row case matrix in plan
// precious-plotting-dewdrop.md.
//
// Store id mapping used throughout: A=1, B=2, C=3, D=4 (matches matrix notation).

class DecideShadowActionTest : public testing::Test {
 protected:
  using ShadowDecision = dingodb::CoordinatorControl::ShadowDecision;

  static ShadowDecision Decide(int64_t region_id, const std::vector<int64_t>& new_ids,
                               const std::vector<int64_t>& old_ids, const std::vector<int64_t>& exist_ids,
                               const std::vector<int64_t>& not_exist_ids) {
    return dingodb::CoordinatorControl::DecideShadowAction(region_id, new_ids, old_ids, exist_ids, not_exist_ids);
  }

  static constexpr int64_t kRegionId = 42;
  static constexpr int64_t kStoreA = 1;
  static constexpr int64_t kStoreB = 2;
  static constexpr int64_t kStoreC = 3;
  static constexpr int64_t kStoreD = 4;
};

// === kAddShadow: activate exactly one shadow peer ===

TEST_F(DecideShadowActionTest, AddShadowAcceptedCases) {
  // Row 2: new=ABC, old=ABC, exist=AB, not_exist=C -> activate shadow C
  {
    auto decision = Decide(kRegionId, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB},
                           {kStoreC});

    EXPECT_EQ(decision.action, ShadowDecision::kAddShadow);
    EXPECT_EQ(decision.target_store_id, kStoreC);
    EXPECT_EQ(decision.reject_step, 0);
    EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::OK);
    EXPECT_TRUE(decision.error_msg.empty());

    EXPECT_EQ(decision.to_add, std::vector<int64_t>{kStoreC});
    EXPECT_TRUE(decision.to_remove_real.empty());
    EXPECT_TRUE(decision.shadow_dropped.empty());
  }

  // Row 8: new=AB, old=AB, exist=A, not_exist=B -> activate shadow B
  {
    auto decision = Decide(kRegionId, {kStoreA, kStoreB}, {kStoreA, kStoreB}, {kStoreA}, {kStoreB});

    EXPECT_EQ(decision.action, ShadowDecision::kAddShadow);
    EXPECT_EQ(decision.target_store_id, kStoreB);
    EXPECT_EQ(decision.reject_step, 0);
    EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::OK);
    EXPECT_TRUE(decision.error_msg.empty());

    EXPECT_EQ(decision.to_add, std::vector<int64_t>{kStoreB});
    EXPECT_TRUE(decision.to_remove_real.empty());
    EXPECT_TRUE(decision.shadow_dropped.empty());
  }
}

// === kRemoveShadow: clean up exactly one shadow peer ===

TEST_F(DecideShadowActionTest, RemoveShadowAcceptedCases) {
  // Row 5: new=AB, old=ABC, exist=AB, not_exist=C -> cleanup shadow C
  {
    auto decision =
        Decide(kRegionId, {kStoreA, kStoreB}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});

    EXPECT_EQ(decision.action, ShadowDecision::kRemoveShadow);
    EXPECT_EQ(decision.target_store_id, kStoreC);
    EXPECT_EQ(decision.reject_step, 0);
    EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::OK);
    EXPECT_TRUE(decision.error_msg.empty());

    EXPECT_TRUE(decision.to_add.empty());
    EXPECT_TRUE(decision.to_remove_real.empty());
    EXPECT_EQ(decision.shadow_dropped, std::vector<int64_t>{kStoreC});
  }

  // Row 9: new=A, old=AB, exist=A, not_exist=B -> cleanup shadow B
  {
    auto decision = Decide(kRegionId, {kStoreA}, {kStoreA, kStoreB}, {kStoreA}, {kStoreB});

    EXPECT_EQ(decision.action, ShadowDecision::kRemoveShadow);
    EXPECT_EQ(decision.target_store_id, kStoreB);
    EXPECT_EQ(decision.reject_step, 0);
    EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::OK);
    EXPECT_TRUE(decision.error_msg.empty());

    EXPECT_TRUE(decision.to_add.empty());
    EXPECT_TRUE(decision.to_remove_real.empty());
    EXPECT_EQ(decision.shadow_dropped, std::vector<int64_t>{kStoreB});
  }
}

// === kNoOp: nothing to do ===

TEST_F(DecideShadowActionTest, NoOpCases) {
  // Row 3: new=ABC, old=ABC, exist=ABC, not_exist=empty -> no-op
  {
    auto decision = Decide(kRegionId, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreC},
                           {kStoreA, kStoreB, kStoreC}, {});

    EXPECT_EQ(decision.action, ShadowDecision::kNoOp);
    EXPECT_EQ(decision.target_store_id, 0);
    EXPECT_EQ(decision.reject_step, 0);
    EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::OK);
    EXPECT_TRUE(decision.error_msg.empty());

    EXPECT_TRUE(decision.to_add.empty());
    EXPECT_TRUE(decision.to_remove_real.empty());
    EXPECT_TRUE(decision.shadow_dropped.empty());
  }

  // Row 7: new=AB, old=AB, exist=AB, not_exist=empty -> no-op
  {
    auto decision = Decide(kRegionId, {kStoreA, kStoreB}, {kStoreA, kStoreB}, {kStoreA, kStoreB}, {});

    EXPECT_EQ(decision.action, ShadowDecision::kNoOp);
    EXPECT_EQ(decision.target_store_id, 0);
    EXPECT_EQ(decision.reject_step, 0);
    EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::OK);
    EXPECT_TRUE(decision.error_msg.empty());

    EXPECT_TRUE(decision.to_add.empty());
    EXPECT_TRUE(decision.to_remove_real.empty());
    EXPECT_TRUE(decision.shadow_dropped.empty());
  }
}

// === step1 reject: new contains store_id not registered in coordinator ===

TEST_F(DecideShadowActionTest, Step1RejectCases) {
  // Row 12: new=ABC, old=ABD, exist=AB, not_exist=D -> step1 reject (C not in old)
  {
    auto decision = Decide(kRegionId, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreD}, {kStoreA, kStoreB},
                           {kStoreD});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 1);
    EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
    EXPECT_NE(decision.error_msg.find("not in coordinator definition"), std::string::npos);
    EXPECT_NE(decision.error_msg.find("store_id:3"), std::string::npos);

    // step1 reject returns BEFORE step2 runs; buckets must be empty.
    EXPECT_TRUE(decision.to_add.empty());
    EXPECT_TRUE(decision.to_remove_real.empty());
    EXPECT_TRUE(decision.shadow_dropped.empty());
    EXPECT_EQ(decision.target_store_id, 0);
  }

  // Edge: new contains a store_id (99) that is totally unknown to coordinator.
  {
    auto decision =
        Decide(kRegionId, {kStoreA, kStoreB, 99}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 1);
    EXPECT_NE(decision.error_msg.find("store_id:99"), std::string::npos);
    EXPECT_TRUE(decision.to_add.empty());
  }

  // Edge: first id in new is the bad one; reject must trigger on first invalid id.
  {
    auto decision =
        Decide(kRegionId, {99, kStoreA, kStoreB}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 1);
    EXPECT_NE(decision.error_msg.find("store_id:99"), std::string::npos);
  }
}

// === step3 reject: would take down a really-running peer ===

TEST_F(DecideShadowActionTest, Step3RejectCases) {
  // Row 6: new=AB, old=ABC, exist=ABC, not_exist=empty -> step3 reject (would remove real C)
  {
    auto decision = Decide(kRegionId, {kStoreA, kStoreB}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreC}, {});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 3);
    EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
    EXPECT_NE(decision.error_msg.find("cannot remove real peer"), std::string::npos);
    EXPECT_NE(decision.error_msg.find("verify_peer_on_store=false"), std::string::npos);

    // step2 buckets must be populated.
    EXPECT_TRUE(decision.to_add.empty());
    EXPECT_EQ(decision.to_remove_real, std::vector<int64_t>{kStoreC});
    EXPECT_TRUE(decision.shadow_dropped.empty());
  }

  // Row 10: new=A, old=AB, exist=AB, not_exist=empty -> step3 reject (would remove real B)
  {
    auto decision = Decide(kRegionId, {kStoreA}, {kStoreA, kStoreB}, {kStoreA, kStoreB}, {});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 3);
    EXPECT_EQ(decision.to_remove_real, std::vector<int64_t>{kStoreB});
    EXPECT_TRUE(decision.shadow_dropped.empty());
  }

  // Row 15: new=A, old=ABC, exist=AB, not_exist=C
  // -> step3 reject (would remove real B); shadow_dropped C is also classified
  //    in step2 but step3 fires first.
  {
    auto decision = Decide(kRegionId, {kStoreA}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 3);
    EXPECT_EQ(decision.to_remove_real, std::vector<int64_t>{kStoreB});
    EXPECT_EQ(decision.shadow_dropped, std::vector<int64_t>{kStoreC});
  }
}

// === step4 reject: too many shadows to activate (n>=2 on the to_add side) ===

TEST_F(DecideShadowActionTest, Step4RejectTooManyToActivate) {
  // Row 1: new=ABC, old=ABC, exist=A, not_exist=BC -> step4 reject (activate B+C)
  {
    auto decision =
        Decide(kRegionId, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreC}, {kStoreA}, {kStoreB, kStoreC});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 4);
    EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
    EXPECT_NE(decision.error_msg.find("only one shadow operation per call"), std::string::npos);

    // 2 shadows would be activated; flagged as too many.
    EXPECT_EQ(decision.to_add.size(), 2u);
    EXPECT_TRUE(decision.to_remove_real.empty());
    EXPECT_TRUE(decision.shadow_dropped.empty());
  }

  // Row 4 (theoretical, exist=empty in production caught earlier by leader check):
  // new=ABC, old=ABC, exist=empty, not_exist=ABC -> step4 reject (3 shadows)
  {
    auto decision = Decide(kRegionId, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreC}, {},
                           {kStoreA, kStoreB, kStoreC});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 4);
    EXPECT_EQ(decision.to_add.size(), 3u);
  }
}

// === step4 reject: too many shadows to clean up (n>=2 on the shadow_dropped side) ===

TEST_F(DecideShadowActionTest, Step4RejectTooManyToCleanup) {
  // Row 13: new=A, old=ABC, exist=A, not_exist=BC -> step4 reject (cleanup B+C)
  auto decision = Decide(kRegionId, {kStoreA}, {kStoreA, kStoreB, kStoreC}, {kStoreA}, {kStoreB, kStoreC});

  EXPECT_EQ(decision.action, ShadowDecision::kReject);
  EXPECT_EQ(decision.reject_step, 4);
  EXPECT_EQ(decision.error_code, dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
  EXPECT_NE(decision.error_msg.find("only one shadow operation per call"), std::string::npos);

  EXPECT_TRUE(decision.to_add.empty());
  EXPECT_TRUE(decision.to_remove_real.empty());
  EXPECT_EQ(decision.shadow_dropped.size(), 2u);
}

// === step4 reject: mixed activate + cleanup in single call ===

TEST_F(DecideShadowActionTest, Step4RejectMixedActivateAndCleanup) {
  // Row 14: new=AB, old=ABC, exist=A, not_exist=BC -> step4 reject (activate B + cleanup C)
  {
    auto decision = Decide(kRegionId, {kStoreA, kStoreB}, {kStoreA, kStoreB, kStoreC}, {kStoreA}, {kStoreB, kStoreC});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 4);
    EXPECT_EQ(decision.to_add, std::vector<int64_t>{kStoreB});
    EXPECT_TRUE(decision.to_remove_real.empty());
    EXPECT_EQ(decision.shadow_dropped, std::vector<int64_t>{kStoreC});
  }

  // Row 11 (theoretical, exist=empty): new=A, old=AB, exist=empty, not_exist=AB
  // -> step4 reject (activate A + cleanup B)
  {
    auto decision = Decide(kRegionId, {kStoreA}, {kStoreA, kStoreB}, {}, {kStoreA, kStoreB});

    EXPECT_EQ(decision.action, ShadowDecision::kReject);
    EXPECT_EQ(decision.reject_step, 4);
    EXPECT_EQ(decision.to_add, std::vector<int64_t>{kStoreA});
    EXPECT_EQ(decision.shadow_dropped, std::vector<int64_t>{kStoreB});
  }
}

// === Step ordering invariant: step3 must be evaluated before step4 ===

TEST_F(DecideShadowActionTest, Step3FiresBeforeStep4) {
  // Construct a request that violates BOTH step3 (would remove real peer) AND
  // step4 (shadow_dropped >= 2). Step3 must win.
  // new=A, old=ABCD, exist=AB, not_exist=CD
  //   to_add = empty, to_remove_real = {B}, shadow_dropped = {C, D}
  auto decision =
      Decide(kRegionId, {kStoreA}, {kStoreA, kStoreB, kStoreC, kStoreD}, {kStoreA, kStoreB}, {kStoreC, kStoreD});

  EXPECT_EQ(decision.action, ShadowDecision::kReject);
  EXPECT_EQ(decision.reject_step, 3);  // step3, not step4
  EXPECT_EQ(decision.to_remove_real, std::vector<int64_t>{kStoreB});
  EXPECT_EQ(decision.shadow_dropped.size(), 2u);
  EXPECT_NE(decision.error_msg.find("cannot remove real peer"), std::string::npos);
}

// === Step2 bucket population correctness ===

TEST_F(DecideShadowActionTest, Step2BucketPopulation) {
  // Use row 5 inputs to verify each bucket independently.
  // new=AB, old=ABC, exist=AB, not_exist=C
  auto decision = Decide(kRegionId, {kStoreA, kStoreB}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});

  // to_add = new \ exist = {} (A and B already in exist).
  EXPECT_TRUE(decision.to_add.empty());

  // to_remove_real = exist \ new = {} (A and B both in new).
  EXPECT_TRUE(decision.to_remove_real.empty());

  // shadow_dropped = not_exist \ new = {C} (C is shadow, not in new).
  EXPECT_EQ(decision.shadow_dropped, std::vector<int64_t>{kStoreC});
}

// === Edge: empty new_store_ids ===

TEST_F(DecideShadowActionTest, EmptyNewStoreIds) {
  // new is empty.
  //   step1 passes vacuously (no element of new is missing from old).
  //   step2 makes to_remove_real = exist (nothing in new), shadow_dropped = not_exist.
  //   step3 fires because to_remove_real is non-empty.
  // (In production this case is caught upstream by ChangePeerRegionWithJob's
  //  size validation before DecideShadowAction is even called.)
  auto decision = Decide(kRegionId, {}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});

  EXPECT_EQ(decision.action, ShadowDecision::kReject);
  EXPECT_EQ(decision.reject_step, 3);
  EXPECT_TRUE(decision.to_add.empty());
  EXPECT_EQ(decision.to_remove_real.size(), 2u);  // A and B
  EXPECT_EQ(decision.shadow_dropped, std::vector<int64_t>{kStoreC});
}

// === Edge: all four input vectors empty ===

TEST_F(DecideShadowActionTest, AllInputsEmpty) {
  // Pathological but well-defined: new=empty, old=empty, exist=empty, not_exist=empty.
  //   step1 passes vacuously.
  //   step2 produces all-empty buckets.
  //   step3 passes (to_remove_real empty).
  //   step4 evaluates n=0 -> kNoOp.
  auto decision = Decide(kRegionId, {}, {}, {}, {});

  EXPECT_EQ(decision.action, ShadowDecision::kNoOp);
  EXPECT_EQ(decision.reject_step, 0);
  EXPECT_EQ(decision.target_store_id, 0);
  EXPECT_TRUE(decision.error_msg.empty());
}

// === Error message format guard: ensure diagnostic context is preserved ===

TEST_F(DecideShadowActionTest, ErrorMessageContainsContext) {
  // step1 reject must contain region_id, new_store_ids, old_store_ids in the message.
  {
    auto decision = Decide(kRegionId, {kStoreA, 99}, {kStoreA, kStoreB}, {kStoreA, kStoreB}, {});

    EXPECT_NE(decision.error_msg.find("region_id:42"), std::string::npos);
    EXPECT_NE(decision.error_msg.find("new_store_ids:[1,99]"), std::string::npos);
    EXPECT_NE(decision.error_msg.find("old_store_ids:[1,2]"), std::string::npos);
  }

  // step3 reject must contain region_id, new/old/to_remove_real lists.
  {
    auto decision = Decide(kRegionId, {kStoreA}, {kStoreA, kStoreB}, {kStoreA, kStoreB}, {});

    EXPECT_NE(decision.error_msg.find("region_id:42"), std::string::npos);
    EXPECT_NE(decision.error_msg.find("new_store_ids:[1]"), std::string::npos);
    EXPECT_NE(decision.error_msg.find("old_store_ids:[1,2]"), std::string::npos);
    EXPECT_NE(decision.error_msg.find("to_remove_real:[2]"), std::string::npos);
  }

  // step4 reject must contain region_id, new/old/to_add/shadow_dropped lists.
  {
    auto decision = Decide(kRegionId, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreC}, {kStoreA},
                           {kStoreB, kStoreC});

    EXPECT_NE(decision.error_msg.find("region_id:42"), std::string::npos);
    EXPECT_NE(decision.error_msg.find("to_add:[2,3]"), std::string::npos);
    EXPECT_NE(decision.error_msg.find("shadow_dropped:[]"), std::string::npos);
  }
}

// === region_id propagates into error messages ===

TEST_F(DecideShadowActionTest, RegionIdInMessages) {
  auto decision = Decide(9999, {kStoreA, kStoreB, 99}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});
  EXPECT_NE(decision.error_msg.find("region_id:9999"), std::string::npos);
}

// === Determinism: identical inputs produce identical outputs across calls ===

TEST_F(DecideShadowActionTest, DeterministicOutput) {
  // Pure function => repeated calls with same inputs yield the same result.
  auto decision_1 =
      Decide(kRegionId, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});
  auto decision_2 =
      Decide(kRegionId, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});

  EXPECT_EQ(decision_1.action, decision_2.action);
  EXPECT_EQ(decision_1.target_store_id, decision_2.target_store_id);
  EXPECT_EQ(decision_1.reject_step, decision_2.reject_step);
  EXPECT_EQ(decision_1.error_code, decision_2.error_code);
  EXPECT_EQ(decision_1.error_msg, decision_2.error_msg);
  EXPECT_EQ(decision_1.to_add, decision_2.to_add);
  EXPECT_EQ(decision_1.to_remove_real, decision_2.to_remove_real);
  EXPECT_EQ(decision_1.shadow_dropped, decision_2.shadow_dropped);
}
