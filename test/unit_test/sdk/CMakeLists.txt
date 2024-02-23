include_directories(${CMAKE_CURRENT_SOURCE_DIR}/)

file(GLOB SDK_UNIT_TEST_RAWKV_SRCS "rawkv/*.cc")
file(GLOB SDK_UNIT_TEST_TRANSACTION_SRCS "transaction/*.cc")
file(GLOB SDK_UNIT_TEST_VECTOR_SRCS "vector/*.cc")

set(SDK_UNIT_TEST_SRCS
  test_meta_cache.cc
  test_region.cc
  test_store_rpc_controller.cc
  test_thread_pool_actuator.cc
  ${SDK_UNIT_TEST_RAWKV_SRCS}
  ${SDK_UNIT_TEST_TRANSACTION_SRCS}
  ${SDK_UNIT_TEST_VECTOR_SRCS}
)

add_library(SDK_UNIT_TEST_OBJS
            OBJECT
            ${SDK_UNIT_TEST_SRCS}
)

add_dependencies(SDK_UNIT_TEST_OBJS sdk)