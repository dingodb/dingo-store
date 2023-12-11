
add_library(sdk
    admin_tool.cc
    client_stub.cc
    client.cc
    coordinator_proxy.cc
    meta_cache.cc
    raw_kv_impl.cc
    region_scanner_impl.cc
    rpc_interaction.cc
    region.cc
    status.cc
    store_rpc_controller.cc
    store_rpc.cc
    # TODO: use libary
    ${PROJECT_SOURCE_DIR}/src/coordinator/coordinator_interaction.cc
    ${PROJECT_SOURCE_DIR}/src/common/role.cc
    ${PROJECT_SOURCE_DIR}/src/common/helper.cc
    ${PROJECT_SOURCE_DIR}/src/common/service_access.cc
    ${PROJECT_SOURCE_DIR}/src/coprocessor/utils.cc
    ${PROJECT_SOURCE_DIR}/src/vector/codec.cc
    ${SERIAL1_SRCS}
    ${SERIAL2_SRCS}
    ${VERSION_SRCS}
)

target_link_libraries(sdk 
    PRIVATE
    $<TARGET_OBJECTS:PROTO_OBJS>
    protobuf
    glog
    brpc
    fmt
    ${GFLAGS_LIBRARIES}
    ${DYNAMIC_LIB}
)

add_dependencies(sdk ${DEPEND_LIBS})
