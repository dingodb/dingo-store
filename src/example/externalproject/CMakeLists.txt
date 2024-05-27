message(STATUS "THIRD_PARTY_PATH: ${THIRD_PARTY_PATH}")

set(prefix_path "${GRPC_INSTALL_DIR}|${CMAKE_CURRENT_BINARY_DIR}/../../sdk/")
message(STATUS "brpc search prefix_path in ${prefix_path}")

ExternalProject_Add(extern_sdk_example
  SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/.."
  BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/.."
  INSTALL_COMMAND ""
  UPDATE_COMMAND ""
  BUILD_ALWAYS TRUE
  CMAKE_ARGS
        -DCMAKE_PREFIX_PATH=${prefix_path}
        -DSDK_ENABLE_GRPC=ON
        -DTHIRD_PARTY_PATH=${THIRD_PARTY_PATH}
        -DGLOG_INCLUDE_DIR=${GLOG_INCLUDE_DIR}
        -DGRPC_INSTALL_DIR=${GRPC_INSTALL_DIR}
        -DFMT_LIBRARIES=${FMT_LIBRARIES}
        -DGFLAGS_LIBRARIES=${GFLAGS_LIBRARIES}
        -DZLIB_LIBRARIES=${ZLIB_LIBRARIES}
        -DGLOG_LIBRARIES=${GLOG_LIBRARIES}
        -DLIBUNWIND_LIBRARIES=${LIBUNWIND_LIBRARIES}
  CMAKE_CACHE_ARGS
        -Dutf8_range_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/utf8_range
        -DProtobuf_DIR:PATH=${_FINDPACKAGE_PROTOBUF_CONFIG_DIR}
        -Dc-ares_DIR:PATH=${GRPC_INSTALL_DIR}/lib/cmake/c-ares
        -Dre2_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/re2
        -DZLIB_ROOT:STRING=${THIRD_PARTY_PATH}/install/zlib
        -Dabsl_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/absl
        -DgRPC_DIR:PATH=${GRPC_INSTALL_DIR}/lib/cmake/grpc
  DEPENDS sdk 
  STEP_TARGETS configure
)
add_dependencies(extern_sdk_example extern_sdk_example-configure)
