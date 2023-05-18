#include <tuple>

#include "assertions.h"
#include "codec.h"
#include "runner.h"

using namespace dingodb::expr;

class ExprTest : public testing::TestWithParam<std::tuple<std::string, Tuple *, int, std::any>> {};

TEST_P(ExprTest, Run) {
  auto &para = GetParam();
  Runner runner;
  auto input = std::get<0>(para);
  auto len = input.size() / 2;
  byte buf[len];
  HexToBytes(buf, input.data(), input.size());
  runner.Decode(buf, len);
  auto result = runner.RunAny(std::get<1>(para));
  EXPECT_TRUE(EqualsByType(std::get<2>(para), result, std::get<3>(para)));
}

INSTANTIATE_TEST_SUITE_P(  // Test cases with consts
    ConstExpr, ExprTest,
    testing::Values(                                                    //
        std::make_tuple("1101", nullptr,                                // 1
                        TYPE_INT32, wrap<int32_t>(1)),                  // 1
        std::make_tuple("2101", nullptr,                                // -1
                        TYPE_INT32, wrap<int32_t>(-1)),                 // -1
        std::make_tuple("119601", nullptr,                              // 150
                        TYPE_INT32, wrap<int32_t>(150)),                // 150
        std::make_tuple("219601", nullptr,                              // -150
                        TYPE_INT32, wrap<int32_t>(-150)),               // -150
        std::make_tuple("13", nullptr,                                  // true
                        TYPE_BOOL, wrap<bool>(true)),                   // true
        std::make_tuple("23", nullptr,                                  // false
                        TYPE_BOOL, wrap<bool>(false)),                  // false
        std::make_tuple("15401F333333333333", nullptr,                  // 7.8
                        TYPE_DOUBLE, wrap<double>(7.8)),                // 7.8
        std::make_tuple("15400921FB4D12D84A", nullptr,                  // 3.1415926
                        TYPE_DOUBLE, wrap<double>(3.1415926)),          // 3.1415926
        std::make_tuple("1541B1E1A300000000", nullptr,                  // 3E8
                        TYPE_DOUBLE, wrap<double>(3E8)),                // 3E8
        std::make_tuple("110111018301", nullptr,                        // 1 + 1
                        TYPE_INT32, wrap<int32_t>(2)),                  // 2
        std::make_tuple("110211038301", nullptr,                        // 2 + 3
                        TYPE_INT32, wrap<int32_t>(5)),                  // 5
        std::make_tuple("120112018302", nullptr,                        // 1L + 1L
                        TYPE_INT64, wrap<int64_t>(2)),                  // 2L
        std::make_tuple("120212038302", nullptr,                        // 2L + 3L
                        TYPE_INT64, wrap<int64_t>(5)),                  // 5L
        std::make_tuple("11031104110685018301", nullptr,                // 3 + 4 * 6
                        TYPE_INT32, wrap<int32_t>(27)),                 // 27
        std::make_tuple("110511068301110B9101", nullptr,                // 5 + 6 = 11
                        TYPE_BOOL, wrap<bool>(true)),                   // true
        std::make_tuple("110711088301110E930111061105950152", nullptr,  // 7 + 8 > 14 && 6 < 5
                        TYPE_BOOL, wrap<bool>(false)),                  // false
        std::make_tuple("1115F021", nullptr,                            // int64(21)
                        TYPE_INT64, wrap<int64_t>(21)),                 // 21L
        std::make_tuple("230352", nullptr,                              // false && null
                        TYPE_BOOL, wrap<bool>(false)),                  // false
        std::make_tuple("130352", nullptr,                              // true && null
                        TYPE_BOOL, wrap<bool>()),                       // null
        std::make_tuple("01A101", nullptr,                              // is_null(null)
                        TYPE_BOOL, wrap<bool>(true)),                   // true
        std::make_tuple("1101A201", nullptr,                            // is_true(1)
                        TYPE_BOOL, wrap<bool>(true))                    // true
        ));

static Tuple tuple1{wrap<int32_t>(1), wrap<int32_t>(2)};
static Tuple tuple2{wrap<int64_t>(35), wrap<int64_t>(46)};
static Tuple tuple3{wrap<double>(3.5), wrap<double>(4.6)};

INSTANTIATE_TEST_SUITE_P(  // Test cases with vars
    VarExpr, ExprTest,
    testing::Values(                                      //
        std::make_tuple("3100", &tuple1,                  // t0
                        TYPE_INT32, wrap<int32_t>(1)),    // 1
        std::make_tuple("3101", &tuple1,                  // t1
                        TYPE_INT32, wrap<int32_t>(2)),    // 2
        std::make_tuple("310031018301", &tuple1,          // t0 + t1
                        TYPE_INT32, wrap<int32_t>(3)),    // 3
        std::make_tuple("3200", &tuple2,                  // t0
                        TYPE_INT64, wrap<int64_t>(35)),   // 35
        std::make_tuple("3201", &tuple2,                  // t1
                        TYPE_INT64, wrap<int64_t>(46)),   // 46
        std::make_tuple("320032018302", &tuple2,          // t0 + t1
                        TYPE_INT64, wrap<int64_t>(81)),   // 81
        std::make_tuple("3500", &tuple3,                  // t0
                        TYPE_DOUBLE, wrap<double>(3.5)),  // 3.5
        std::make_tuple("3501", &tuple3,                  // t1
                        TYPE_DOUBLE, wrap<double>(4.6)),  // 4.6
        std::make_tuple("350035018305", &tuple3,          // t0 + t1
                        TYPE_DOUBLE, wrap<double>(8.1))   // 8.1
        ));
