include_directories(${CMAKE_SOURCE_DIR}/src/coordinator)
include_directories(${CMAKE_SOURCE_DIR}/src/client)
include_directories(${CMAKE_SOURCE_DIR}/src/sdk)
# use for gmock
include_directories(${GTEST_INCLUDE_DIR}/include)

include(GoogleTest)

add_executable(test_meta_cache
    test_meta_cache.cc)
target_link_libraries(test_meta_cache
  sdk
  ${GTEST_MAIN_LIBRARIES}
  ${GMOCK_LIBRARIES}
  ${GMOCK_MAIN_LIBRARIES}
  )
gtest_discover_tests(test_meta_cache)
