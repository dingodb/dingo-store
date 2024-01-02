add_library(sdk
  admin_tool.cc
  client_stub.cc
  client.cc
  coordinator_proxy.cc
  meta_cache.cc
  region.cc
  status.cc
  rawkv/raw_kv_impl.cc
  rawkv/region_scanner_impl.cc
  rpc/rpc_interaction.cc
  store/store_rpc_controller.cc
  store/store_rpc.cc
  transaction/txn_buffer.cc
  transaction/txn_impl.cc
  transaction/txn_lock_resolver.cc
  transaction/txn_region_scanner_impl.cc
  utils/thread_pool_executor.cc
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

target_link_libraries(
  sdk
  PRIVATE $<TARGET_OBJECTS:PROTO_OBJS>
          protobuf
          glog
          brpc
          fmt
          ${GFLAGS_LIBRARIES}
          ${DYNAMIC_LIB})

add_dependencies(sdk ${DEPEND_LIBS})
