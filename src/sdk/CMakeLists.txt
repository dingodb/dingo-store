# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.23.1 FATAL_ERROR)
project(sdk C CXX)

set(SDK_SRCS
  admin_tool.cc
  auto_increment_manager.cc
  client_stub.cc
  client.cc
  meta_cache.cc
  meta_member_info.cc
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
  rpc/coordinator_rpc_controller.cc
  rpc/store_rpc_controller.cc
  transaction/txn_buffer.cc
  transaction/txn_impl.cc
  transaction/txn_lock_resolver.cc
  transaction/txn_region_scanner_impl.cc
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
  utils/thread_pool_impl.cc
  common/param_config.cc
  expression/coding.cc
  expression/langchain_expr_encoder.cc
  expression/langchain_expr_factory.cc
  expression/langchain_expr.cc
)

if(ENABLE_COVERAGE)
  set(GCOV_LIBRAR gcov)
endif()

set(SDK_LIBS "")
message(STATUS "search path${CMAKE_PREFIX_PATH}")

if(SDK_ENABLE_GRPC)
    message(STATUS "Build sdk with grpc")
    add_definitions(-DUSE_GRPC=1)
    add_definitions(-DGLOG_CUSTOM_PREFIX_SUPPORT)

    message(STATUS "third_party path: ${THIRD_PARTY_PATH}")
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
    include_directories(${CMAKE_CURRENT_BINARY_DIR}/../..)

    include_directories(${THIRD_PARTY_PATH}/install/grpc/include)
    include_directories(${THIRD_PARTY_PATH}/install/fmt/include)
    include_directories(${THIRD_PARTY_PATH}/install/glog/include)
    include_directories(${THIRD_PARTY_PATH}/install/nlohmann-json/include)

    # # https://github.com/llvm/llvm-project/blob/main/cmake/Modules/FindGRPC.cmake
    # This setup requires gRPC to be built from sources using CMake and installed
    # to ${GRPC_INSTALL_PATH} via -DCMAKE_INSTALL_PREFIX=${GRPC_INSTALL_PATH}.
    # Libraries will be linked according to gRPC build policy which generates
    # static libraries when BUILD_SHARED_LIBS is Off and dynamic libraries when
    # it's On (NOTE: This is a variable passed to gRPC CMake build invocation,
    # LLVM's BUILD_SHARED_LIBS has no effect).
    set(GRPC_INSTALL_PATH ${THIRD_PARTY_PATH}/install/grpc)
    # set(protobuf_MODULE_COMPATIBLE TRUE)
    # find_package(Protobuf CONFIG REQUIRED HINTS ${GRPC_INSTALL_PATH})
    find_package(Protobuf CONFIG REQUIRED)
    message(STATUS "Using protobuf ${Protobuf_VERSION},dir:${Protobuf_DIR}, inlucde:${Protobuf_INCLUDE_DIRS}" )
    # find_package(gRPC CONFIG REQUIRED HINTS ${GRPC_INSTALL_PATH})
    find_package(gRPC CONFIG REQUIRED)
    message(STATUS "Using gRPC ${gRPC_VERSION}, include:${gRPC_INCLUDE_DIRS}")
  
    include_directories(${Protobuf_INCLUDE_DIRS})
  
    # gRPC CMake CONFIG gives the libraries slightly odd names, make them match
    # the conventional system-installed names.
    set_target_properties(protobuf::libprotobuf PROPERTIES IMPORTED_GLOBAL TRUE)
    add_library(grpc_protobuf ALIAS protobuf::libprotobuf)
  
    set_target_properties(gRPC::grpc++ PROPERTIES IMPORTED_GLOBAL TRUE)
    add_library(grpc++ ALIAS gRPC::grpc++)
    if (ENABLE_GRPC_REFLECTION)
      set_target_properties(gRPC::grpc++_reflection PROPERTIES IMPORTED_GLOBAL TRUE)
      add_library(grpc++_reflection ALIAS gRPC::grpc++_reflection)
    endif()
  
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
    message(STATUS "Using grpc_cpp_plugin ${GRPC_CPP_PLUGIN}")
    find_program(PROTOC protoc REQUIRED)
    message(STATUS "Using protoc ${PROTOC}")

    set(GRPC_PROTO_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto_tmp)
    add_custom_target(create_grpc_proto_dir ALL
      COMMAND ${CMAKE_COMMAND} -E make_directory ${GRPC_PROTO_DIR}
    )
    set(GRPC_PROTO_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/../../proto_grpc)
    add_custom_target(create_grpc_proto_output_dir ALL
      COMMAND ${CMAKE_COMMAND} -E make_directory ${GRPC_PROTO_OUTPUT_DIR}
    )
    message(STATUS "grpc proto_tmp:${GRPC_PROTO_DIR}, proto_grpc:${GRPC_PROTO_OUTPUT_DIR}")

    set(GRPC_PROTO_SRCS "")
    set(GRPC_PROTO_HDRS "")
    set(GRPC_PROTO_GRPC_SRCS "")
    set(GRPC_PROTO_GRPC_HDRS "")

    set(PROTOS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../contrib/dingo-store-proto/proto)
    message(STATUS "PROTOS_DIR:${PROTOS_DIR}")
    file(GLOB_RECURSE MSG_PROTOS ${PROTOS_DIR}/*.proto)
    foreach(msg ${MSG_PROTOS})
        get_filename_component(FIL_WE ${msg} NAME_WE)

        set(TMP_PROTO ${GRPC_PROTO_DIR}/${FIL_WE}.proto)
        message(STATUS "proto:${msg}, grpc_proto: ${TMP_PROTO}")

        add_custom_command(
            OUTPUT ${TMP_PROTO}
            COMMAND cp ${msg} ${TMP_PROTO}
            COMMAND sed -i "/cc_generic_services/c\\option cc_generic_services = false;" ${TMP_PROTO}
            COMMENT "Copying ${msg} to ${TMP_PROTO} and replacing cc_generic_services option"
            VERBATIM
        )

        set(FILE_PREFIX_NAME "${GRPC_PROTO_OUTPUT_DIR}/${FIL_WE}")
        list(APPEND GRPC_PROTO_SRCS "${FILE_PREFIX_NAME}.pb.cc")
        list(APPEND GRPC_PROTO_HDRS "${FILE_PREFIX_NAME}.pb.h")
        list(APPEND GRPC_PROTO_GRPC_SRCS "${FILE_PREFIX_NAME}.grpc.pb.cc")
        list(APPEND GRPC_PROTO_GRPC_HDRS "${FILE_PREFIX_NAME}.grpc.pb.h")

        add_custom_command(
          OUTPUT "${FILE_PREFIX_NAME}.pb.cc"
                 "${FILE_PREFIX_NAME}.pb.h"
                 "${FILE_PREFIX_NAME}.grpc.pb.cc"
                 "${FILE_PREFIX_NAME}.grpc.pb.h"
          COMMAND  ${PROTOC}
          ARGS --grpc_out "${GRPC_PROTO_OUTPUT_DIR}"
            --cpp_out "${GRPC_PROTO_OUTPUT_DIR}"
            -I "${GRPC_PROTO_DIR}"
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            "${TMP_PROTO}"
          DEPENDS ${TMP_PROTO}
          COMMENT "Running GRpc protocol buffer compiler on ${TMP_PROTO}"
          VERBATIM
        )
    endforeach()

    set_source_files_properties(
      ${GRPC_PROTO_SRCS} 
      ${GRPC_PROTO_HDRS} 
      ${GRPC_PROTO_GRPC_SRCS} 
      ${GRPC_PROTO_GRPC_HDRS} 
      PROPERTIES GENERATED TRUE)

    list(APPEND SDK_SRCS
        rpc/grpc/grpc_rpc_client.cc
        rpc/grpc/coordinator_rpc.cc
        rpc/grpc/index_service_rpc.cc
        rpc/grpc/store_rpc.cc
        ${GRPC_PROTO_SRCS} 
        ${GRPC_PROTO_HDRS} 
        ${GRPC_PROTO_GRPC_SRCS} 
        ${GRPC_PROTO_GRPC_HDRS} 
    )

    file(GLOB SERIAL1_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/../serial/*.cc)
    file(GLOB SERIAL2_SRCS ${PROJECT_SOURCE_DIR}/../serial/schema/*.cc)

    add_library(sdk
        ${SDK_SRCS}
        # TODO: use libary
        ${SERIAL1_SRCS}
        ${SERIAL2_SRCS}
    )

    if(ENABLE_COVERAGE)
      set(SDK_LIBS ${SDK_LIBS} ${GCOV_LIBRAR})
    endif()

    target_link_libraries(sdk
      PRIVATE
        ${SDK_LIBS}
        grpc++ 
        grpc_protobuf
        ${FMT_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${GLOG_LIBRARIES}
        ${GTEST_LIBRARIES}
        ${LIBUNWIND_LIBRARIES}
        dl 
        Threads::Threads
    )

else()
    message(STATUS "Build sdk with brpc")
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
        # ${ZLIB_LIBRARIES}
        ${OPENSSL_LIBRARIES}
        ${CRYPTO_LIBRARIES}
        ${GLOG_LIBRARIES}
        ${GTEST_LIBRARIES}
        ${LIBUNWIND_LIBRARIES}
        # ${LIBUNWIND_GENERIC_LIBRARIES}
        # ${LIBUNWIND_ARCH_LIBRARIES}
    )

   if(BRPC_ENABLE_CPU_PROFILER)
     set(SDK_LIBS ${SDK_LIBS} ${GPERFTOOLS_LIBRARIES})
   endif()

   if(ENABLE_COVERAGE)
     set(SDK_LIBS ${SDK_LIBS} ${GCOV_LIBRAR})
   endif()

   set(SDK_LIBS ${SDK_LIBS} ${ZLIB_LIBRARIES} dl Threads::Threads)

    list(APPEND SDK_SRCS
        rpc/brpc/brpc_rpc_client.cc
        rpc/brpc/coordinator_rpc.cc
        rpc/brpc/index_service_rpc.cc
        rpc/brpc/store_rpc.cc
    )

    add_library(sdk
        ${SDK_SRCS}
        # TODO: use libary
        ${SERIAL1_SRCS}
        ${SERIAL2_SRCS}
    )


    set_target_properties(sdk
      PROPERTIES
      POSITION_INDEPENDENT_CODE ON
    )

   target_link_libraries(sdk
   PRIVATE
       $<TARGET_OBJECTS:PROTO_OBJS>
       ${SDK_LIBS}
   )

  add_dependencies(sdk ${DEPEND_LIBS})
endif()

