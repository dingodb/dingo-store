
add_library(sdk
    meta_cache.cc
    status.cc
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
    ${GFLAGS_LIBRARIES}
    protobuf
    glog
    brpc
    fmt
    ${DYNAMIC_LIB}
)

add_dependencies(sdk ${DEPEND_LIBS})
