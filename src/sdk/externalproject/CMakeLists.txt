INCLUDE(ExternalProject)

SET(SDK_LIBRARIES "${CMAKE_CURRENT_BINARY_DIR}/../libsdk.a" CACHE FILEPATH "sdk library." FORCE)
message(STATUS "SDK_LIBRARIES: ${SDK_LIBRARIES}")

# Builds absl project from the git submodule.
ExternalProject_Add(absl
  PREFIX absl
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/abseil-cpp"
  BINARY_DIR "${GRPC_BUILD_DIR}/absl"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DCMAKE_INSTALL_PREFIX:PATH=${GRPC_INSTALL_DIR}
)

# Builds absl project from the git submodule.
ExternalProject_Add(utf8_range
  PREFIX utf8_range
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/utf8_range"
  BINARY_DIR "${GRPC_BUILD_DIR}/utf8_range"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -Dutf8_range_ENABLE_TESTS:BOOL=OFF
        -Dabsl_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/absl
        -DCMAKE_INSTALL_PREFIX:PATH=${GRPC_INSTALL_DIR}
  DEPENDS absl
)

# Builds c-ares project from the git submodule.
ExternalProject_Add(c-ares
  PREFIX c-ares
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/cares/cares"
  BINARY_DIR "${GRPC_BUILD_DIR}/c-ares"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DCARES_SHARED:BOOL=OFF
        -DCARES_STATIC:BOOL=ON
        -DCARES_STATIC_PIC:BOOL=ON
        -DCMAKE_INSTALL_PREFIX:PATH=${GRPC_INSTALL_DIR}
)

# Builds protobuf project from the git submodule.
ExternalProject_Add(grpc_protobuf
  PREFIX grpc_protobuf
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/protobuf"
  BINARY_DIR "${GRPC_BUILD_DIR}/protobuf"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -Dprotobuf_BUILD_TESTS:BOOL=OFF
        -Dprotobuf_WITH_ZLIB:BOOL=OFF
        -Dprotobuf_ABSL_PROVIDER:STRING=package
        -Dabsl_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/absl
        -Dutf8_range_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/utf8_range
        -Dprotobuf_MSVC_STATIC_RUNTIME:BOOL=OFF
        -DCMAKE_INSTALL_PREFIX:PATH=${GRPC_INSTALL_DIR}
  DEPENDS absl utf8_range
)

# Builds re2 project from the git submodule.
ExternalProject_Add(re2
  PREFIX re2
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/re2"
  BINARY_DIR "${GRPC_BUILD_DIR}/re2"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DCMAKE_INSTALL_PREFIX:PATH=${GRPC_INSTALL_DIR}
)

# # Builds zlib project from the git submodule.
# ExternalProject_Add(zlib
#   PREFIX zlib
#   SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../../third_party/zlib"
#   CMAKE_CACHE_ARGS
#         -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/zlib
# )

# the location where protobuf-config.cmake will be installed varies by platform
if (WIN32)
  set(_FINDPACKAGE_PROTOBUF_CONFIG_DIR "${GRPC_INSTALL_DIR}/cmake")
else()
  set(_FINDPACKAGE_PROTOBUF_CONFIG_DIR "${GRPC_INSTALL_DIR}/lib/cmake/protobuf")
endif()

# Builds gRPC based on locally checked-out sources and set arguments so that all the dependencies
# are correctly located.
ExternalProject_Add(extern_grpc
    ${EXTERNAL_PROJECT_LOG_ARGS}
    PREFIX grpc
    SOURCE_DIR ${GRPC_SOURCES_DIR}
    BINARY_DIR "${GRPC_BUILD_DIR}/grpc"
    CMAKE_ARGS
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        ${EXTERNAL_OPTIONAL_ARGS}

  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DgRPC_INSTALL:BOOL=ON
        -DgRPC_BUILD_TESTS:BOOL=OFF
        -DgRPC_BUILD_MSVC_MP_COUNT:STRING=-1
        -Dutf8_range_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/utf8_range
        -DgRPC_PROTOBUF_PROVIDER:STRING=package
        -DProtobuf_DIR:PATH=${_FINDPACKAGE_PROTOBUF_CONFIG_DIR}
        -DgRPC_RE2_PROVIDER:STRING=package
        -Dre2_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/re2
        -DgRPC_ZLIB_PROVIDER:STRING=package
        -DZLIB_ROOT:STRING=${THIRD_PARTY_PATH}/install/zlib
        -DgRPC_ABSL_PROVIDER:STRING=package
        -Dabsl_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/absl
        -DgRPC_CARES_PROVIDER:STRING=package
        -Dc-ares_DIR:PATH=${GRPC_INSTALL_DIR}/lib/cmake/c-ares
        -DgRPC_SSL_PROVIDER:STRING=package
        -DCMAKE_INSTALL_PREFIX:PATH=${GRPC_INSTALL_DIR}
  DEPENDS zlib grpc_protobuf c-ares  re2  absl utf8_range
)

message(STATUS "THIRD_PARTY_PATH: ${THIRD_PARTY_PATH}")
ExternalProject_Add(extern_sdk
  SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/.."
  BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/.."
  INSTALL_COMMAND ""
  UPDATE_COMMAND ""
  BUILD_ALWAYS TRUE
  CMAKE_ARGS
        -DCMAKE_PREFIX_PATH=${GRPC_INSTALL_DIR}
        -DSDK_ENABLE_GRPC=ON
        -DTHIRD_PARTY_PATH=${THIRD_PARTY_PATH}
        -DFMT_LIBRARIES=${FMT_LIBRARIES}
        -DGFLAGS_LIBRARIES=${GFLAGS_LIBRARIES}
        -DZLIB_LIBRARIES=${ZLIB_LIBRARIES}
        -DGLOG_LIBRARIES=${GLOG_LIBRARIES}
        -DGTEST_LIBRARIES=${GTEST_LIBRARIES}
        -DLIBUNWIND_LIBRARIES=${LIBUNWIND_LIBRARIES}
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -Dutf8_range_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/utf8_range
        -DProtobuf_DIR:PATH=${_FINDPACKAGE_PROTOBUF_CONFIG_DIR}
        -Dc-ares_DIR:PATH=${GRPC_INSTALL_DIR}/lib/cmake/c-ares
        -Dre2_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/re2
        -DZLIB_ROOT:STRING=${THIRD_PARTY_PATH}/install/zlib
        -Dabsl_DIR:STRING=${GRPC_INSTALL_DIR}/lib/cmake/absl
        -DgRPC_DIR:PATH=${GRPC_INSTALL_DIR}/lib/cmake/grpc
  DEPENDS extern_grpc protobuf glog fmt gflags zlib gtest nlohmann-json libunwind
  STEP_TARGETS configure
)
add_dependencies(extern_sdk extern_sdk-configure)

ADD_LIBRARY(sdk STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET sdk PROPERTY IMPORTED_LOCATION ${SDK_LIBRARIES})
add_dependencies(sdk extern_sdk ${DEPEND_LIBS})
target_link_libraries(sdk
  INTERFACE
    ${FMT_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${GTEST_LIBRARIES}
    ${LIBUNWIND_LIBRARIES}
    dl 
    Threads::Threads
    )
