include_directories(${CMAKE_SOURCE_DIR}/src/coordinator)
include_directories(${CMAKE_SOURCE_DIR}/src/client)
include_directories(${CMAKE_SOURCE_DIR}/src/sdk)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/)
# use for gmock
include_directories(${GTEST_INCLUDE_DIR}/include)

include(GoogleTest)

set(TEST_SRCS
  test_meta_cache.cc
  test_region.cc
  test_store_rpc_controller.cc
  test_thread_pool_actuator.cc
  test_vector_index_cache.cc
  rawkv/test_raw_kv.cc
  rawkv/test_region_scanner.cc
  transaction/test_txn_buffer.cc
  transaction/test_txn_impl.cc
  transaction/test_txn_lock_resolver.cc
  vector/test_vector_common.cc
)

add_executable(sdk_unit_test
  main.cc
  ${TEST_SRCS}
)

target_link_libraries(sdk_unit_test
  sdk
  ${GTEST_LIBRARIES}
  ${GMOCK_LIBRARIES}
)
