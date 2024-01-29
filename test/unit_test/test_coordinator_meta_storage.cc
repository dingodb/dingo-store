#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/helper.h"
#include "coordinator/coordinator_meta_storage.h"  // Include the header file where GetNextIds is defined

namespace dingodb {

class CoordinatorMetaStorageTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(CoordinatorMetaStorageTest, GetNextId) {
  DingoSafeIdEpochMap id_epoch_map;
  id_epoch_map.Init(100);

  int64_t value;

  // Test with invalid key
  EXPECT_EQ(-1, id_epoch_map.GetNextId(-1, value));

  // Test with valid key
  // Note: This test assumes that the InnerGetNextId function and the safe_map are set up in a certain way.
  // You may need to adjust this test depending on how they are actually implemented.
  EXPECT_EQ(1, id_epoch_map.GetNextId(10, value));

  // check if the id is continuous
  int64_t value2;
  EXPECT_EQ(1, id_epoch_map.GetNextId(10, value2));

  int64_t value3;
  EXPECT_EQ(1, id_epoch_map.GetNextId(10, value3));

  EXPECT_EQ(1, value3 - value2);
}

TEST_F(CoordinatorMetaStorageTest, GetNextIds) {
  DingoSafeIdEpochMap id_epoch_map;
  id_epoch_map.Init(100);

  std::vector<int64_t> values;

  // Test with invalid key
  EXPECT_EQ(-1, id_epoch_map.GetNextIds(-1, 10, values));

  // Test with invalid count
  EXPECT_EQ(-1, id_epoch_map.GetNextIds(10, -1, values));

  // Test with valid key and count
  EXPECT_EQ(1, id_epoch_map.GetNextIds(10, 10, values));
  EXPECT_EQ(10, values.size());

  std::set<int64_t> set_values(values.begin(), values.end());
  EXPECT_TRUE(dingodb::Helper::IsContinuous(set_values));

  // Test with multiple calls to GetNextIds
  for (int i = 0; i < 10; ++i) {
    values.clear();
    EXPECT_EQ(1, id_epoch_map.GetNextIds(10, 10, values));
    EXPECT_EQ(10, values.size());

    std::set<int64_t> set_values2(values.begin(), values.end());
    EXPECT_TRUE(dingodb::Helper::IsContinuous(set_values2));

    // Check that the IDs returned from the second call are different from the first call
    EXPECT_TRUE(set_values.find(*set_values2.begin()) == set_values.end());

    // Check that the IDs returned from the second call are continuous with the first call
    EXPECT_EQ(*set_values.rbegin() + 1, *set_values2.begin());

    set_values = set_values2;
  }

  // Test with different keys
  for (int i = 1; i < 10; ++i) {
    values.clear();
    EXPECT_EQ(1, id_epoch_map.GetNextIds(i, 10, values));
    EXPECT_EQ(10, values.size());

    std::set<int64_t> set_values2(values.begin(), values.end());
    EXPECT_TRUE(dingodb::Helper::IsContinuous(set_values2));
  }
}

TEST_F(CoordinatorMetaStorageTest, UpdatePresentId) {
  DingoSafeIdEpochMap id_epoch_map;
  id_epoch_map.Init(100);

  int64_t value = id_epoch_map.GetPresentId(10);
  EXPECT_EQ(0, value);

  EXPECT_EQ(1, id_epoch_map.UpdatePresentId(10, 100));
  value = id_epoch_map.GetPresentId(10);
  EXPECT_EQ(100, value);

  EXPECT_EQ(1, id_epoch_map.UpdatePresentId(10, 200));
  value = id_epoch_map.GetPresentId(10);
  EXPECT_EQ(200, value);

  EXPECT_EQ(1, id_epoch_map.GetNextId(10, value));
  EXPECT_EQ(201, value);

  std::vector<int64_t> values;
  EXPECT_EQ(1, id_epoch_map.GetNextIds(10, 10, values));
  EXPECT_EQ(1, id_epoch_map.GetPresentId(10, value));
  EXPECT_EQ(211, value);
}

}  // namespace dingodb
