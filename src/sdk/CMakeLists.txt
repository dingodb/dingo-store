add_library(sdk
  admin_tool.cc
  client_stub.cc
  client.cc
  coordinator_proxy.cc
  meta_cache.cc
  region.cc
  status.cc
  rawkv/raw_kv_task.cc
  rawkv/raw_kv_get_task.cc
  rawkv/raw_kv_batch_get_task.cc
  rawkv/raw_kv_put_task.cc
  rawkv/raw_kv_batch_put_task.cc
  rawkv/raw_kv_put_if_absent_task.cc
  rawkv/raw_kv_batch_put_if_absent_task.cc
  rawkv/raw_kv_delete_task.cc
  rawkv/raw_kv_batch_delete_task.cc
  rawkv/raw_kv_compare_and_set_task.cc
  rawkv/raw_kv_batch_compare_and_set_task.cc
  rawkv/raw_kv_delete_range_task.cc
  rawkv/raw_kv_scan_task.cc
  rawkv/raw_kv_region_scanner_impl.cc
  rpc/rpc_interaction.cc
  store/store_rpc_controller.cc
  store/store_rpc.cc
  transaction/txn_buffer.cc
  transaction/txn_impl.cc
  transaction/txn_lock_resolver.cc
  transaction/txn_region_scanner_impl.cc
  vector/index_service_rpc.cc
  vector/vector_client.cc
  vector/vector_index_cache.cc
  vector/vector_index_creator.cc
  vector/vector_index.cc
  vector/vector_param.cc
  vector/vector_task.cc
  vector/vector_add_task.cc
  vector/vector_batch_query_task.cc
  vector/vector_count_task.cc
  vector/vector_delete_task.cc
  vector/vector_get_border_task.cc
  vector/vector_get_index_metrics_task.cc
  vector/vector_scan_query_task.cc
  vector/vector_search_task.cc
  utils/thread_pool_actuator.cc
  common/param_config.cc
  # TODO: use libary
  ${PROJECT_SOURCE_DIR}/src/coordinator/coordinator_interaction.cc
  ${PROJECT_SOURCE_DIR}/src/common/role.cc
  ${PROJECT_SOURCE_DIR}/src/common/helper.cc
  ${PROJECT_SOURCE_DIR}/src/common/service_access.cc
  ${PROJECT_SOURCE_DIR}/src/common/synchronization.cc
  ${PROJECT_SOURCE_DIR}/src/common/threadpool.cc
  ${PROJECT_SOURCE_DIR}/src/coprocessor/utils.cc
  ${PROJECT_SOURCE_DIR}/src/vector/codec.cc
  ${SERIAL1_SRCS}
  ${SERIAL2_SRCS}
  ${VERSION_SRCS})


if(ENABLE_COVERAGE)
  set(GCOV_LIBRAR gcov)
endif()

set_target_properties(sdk
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
)

# CAUTION: the order of the libraries is important, because of the dependencies between them.
set(SDK_LIBS
    ${BRPC_LIBRARIES}
    ${PROTOBUF_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${LEVELDB_LIBRARIES}
    ${SNAPPY_LIBRARIES}
    # ${LZ4_LIBRARIES}
    # ${ZSTD_LIBRARIES}
    ${FMT_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${GTEST_LIBRARIES}
    # ${LIBUNWIND_GENERIC_LIBRARIES}
    # ${LIBUNWIND_ARCH_LIBRARIES}
)

if(BRPC_ENABLE_CPU_PROFILER)
  set(SDK_LIBS ${SDK_LIBS} ${GPERFTOOLS_LIBRARIES}  ${LIBUNWIND_LIBRARIES})
endif()

if(ENABLE_COVERAGE)
  set(SDK_LIBS ${SDK_LIBS} ${GCOV_LIBRAR})
endif()

set(SDK_LIBS ${SDK_LIBS} dl Threads::Threads)

target_link_libraries(sdk
PRIVATE
    $<TARGET_OBJECTS:PROTO_OBJS>
    ${SDK_LIBS}
    )

add_dependencies(sdk ${DEPEND_LIBS})
