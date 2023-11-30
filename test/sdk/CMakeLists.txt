include_directories(${CMAKE_SOURCE_DIR}/src/coordinator)
include_directories(${CMAKE_SOURCE_DIR}/src/client)
include_directories(${CMAKE_SOURCE_DIR}/src/sdk)
# use for gmock
include_directories(${GTEST_INCLUDE_DIR}/include)

include(GoogleTest)

file(GLOB TEST_SRCS "test_*.cc")

add_executable(sdk_unit_test
  main.cc
  ${TEST_SRCS}
)

target_link_libraries(sdk_unit_test
  sdk
  ${GTEST_LIBRARIES}
  ${GMOCK_LIBRARIES}
)
