include_directories(${CMAKE_SOURCE_DIR}/src/expr)

add_executable(test_expr
  test_expr.cc
)
target_link_libraries(test_expr
  gtest
  gtest_main
  pthread
  dingo_expr
)

include(GoogleTest)
gtest_discover_tests(test_expr)
