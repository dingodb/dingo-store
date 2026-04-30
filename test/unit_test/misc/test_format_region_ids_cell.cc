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

#include "client_v2/pretty.h"

class FormatRegionIdsCellTest : public testing::Test {
 protected:
  static std::string Fmt(std::vector<int64_t> ids) { return client_v2::Pretty::FormatRegionIdsCell(ids); }
};

TEST_F(FormatRegionIdsCellTest, EmptyInputReturnsEmptyString) {
  EXPECT_EQ(Fmt({}), "");
}

TEST_F(FormatRegionIdsCellTest, SingleRegion) {
  EXPECT_EQ(Fmt({42}), "42");
}

TEST_F(FormatRegionIdsCellTest, MultipleSortedAscending) {
  EXPECT_EQ(Fmt({1, 2, 3}), "1,2,3");
}

TEST_F(FormatRegionIdsCellTest, UnsortedInputGetsSortedAscending) {
  // Helper should sort internally so callers don't have to.
  EXPECT_EQ(Fmt({3, 1, 2}), "1,2,3");
  EXPECT_EQ(Fmt({100, 5, 50, 1}), "1,5,50,100");
}

TEST_F(FormatRegionIdsCellTest, ExactlyFiveRegionsNoEllipsis) {
  // Boundary: 5 regions fit exactly, no ellipsis or total-count suffix.
  EXPECT_EQ(Fmt({5, 4, 3, 2, 1}), "1,2,3,4,5");
}

TEST_F(FormatRegionIdsCellTest, SixRegionsTruncatedWithEllipsisAndTotal) {
  // Boundary: 6 > 5, list first 5 (sorted) and append "...6".
  EXPECT_EQ(Fmt({1, 2, 3, 4, 5, 6}), "1,2,3,4,5...6");
}

TEST_F(FormatRegionIdsCellTest, TenRegionsTruncatedShowsTotalCount) {
  EXPECT_EQ(Fmt({10, 9, 8, 7, 6, 5, 4, 3, 2, 1}), "1,2,3,4,5...10");
}
