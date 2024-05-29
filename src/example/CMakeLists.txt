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
project(sdk-example C CXX)

if(SDK_ENABLE_GRPC)
    message(STATUS "sdk_example: CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}" )
    message(STATUS "sdk_example: GRPC_INSTALL_DIR ${GRPC_INSTALL_DIR}, THIRD_PARTY_PATH:${THIRD_PARTY_PATH}" )

    add_definitions(-DGLOG_CUSTOM_PREFIX_SUPPORT)

    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
    include_directories(${GLOG_INCLUDE_DIR})

    message(STATUS "sdk_example: link dir ${CMAKE_CURRENT_BINARY_DIR}/../sdk" )
    link_directories("${CMAKE_CURRENT_BINARY_DIR}/../sdk")

    find_package(utf8_range CONFIG REQUIRED)
    # set(GRPC_INSTALL_PATH ${THIRD_PARTY_PATH}/install/grpc)
    # set(protobuf_MODULE_COMPATIBLE TRUE)
    # find_package(Protobuf CONFIG REQUIRED HINTS ${GRPC_INSTALL_PATH})
    find_package(Protobuf CONFIG REQUIRED)
    message(STATUS "sdk_example: Using protobuf ${Protobuf_VERSION},dir:${Protobuf_DIR}, inlucde:${Protobuf_INCLUDE_DIRS}" )
    # find_package(gRPC CONFIG REQUIRED HINTS ${GRPC_INSTALL_PATH})
    find_package(gRPC CONFIG REQUIRED)
    message(STATUS "sdk_example: Using gRPC ${gRPC_VERSION}, include:${gRPC_INCLUDE_DIRS}")
  
    # gRPC CMake CONFIG gives the libraries slightly odd names, make them match
    # the conventional system-installed names.
    set_target_properties(protobuf::libprotobuf PROPERTIES IMPORTED_GLOBAL TRUE)
    add_library(grpc_protobuf ALIAS protobuf::libprotobuf)
  
    set_target_properties(gRPC::grpc++ PROPERTIES IMPORTED_GLOBAL TRUE)
    add_library(grpc++ ALIAS gRPC::grpc++)

    set(DEP_LIBS sdk 
        sdk
        grpc++
        grpc_protobuf
        ${FMT_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${GLOG_LIBRARIES}
        ${LIBUNWIND_LIBRARIES}
        dl 
        Threads::Threads
    )

    add_executable(sdk_rawkv_example
        sdk_rawkv_example.cc)
    target_link_libraries(sdk_rawkv_example
        ${DEP_LIBS}
    )

    add_executable(sdk_transaction_example
        sdk_transaction_example.cc)
    target_link_libraries(sdk_transaction_example
        ${DEP_LIBS}
    )

    add_executable(sdk_transaction_gc_example
        sdk_transaction_gc_example.cc)
    target_link_libraries(sdk_transaction_gc_example
        ${DEP_LIBS}
    )

    add_executable(sdk_transaction_gc_op_all_and_lock_example
        sdk_transaction_gc_op_all_and_lock_example.cc)
    target_link_libraries(sdk_transaction_gc_op_all_and_lock_example
        ${DEP_LIBS}
    )

    add_executable(sdk_vector_example
        sdk_vector_example.cc)
    target_link_libraries(sdk_vector_example
        ${DEP_LIBS}
    )

    add_executable(txn_scan_test
        txn_scan_test.cc)
    target_link_libraries(txn_scan_test
        ${DEP_LIBS}
    )
else()
    add_executable(sdk_rawkv_example
        sdk_rawkv_example.cc)
    target_link_libraries(sdk_rawkv_example
        sdk
    )

    add_executable(sdk_transaction_example
        sdk_transaction_example.cc)
    target_link_libraries(sdk_transaction_example
        sdk
    )

    add_executable(sdk_transaction_gc_example
        sdk_transaction_gc_example.cc)
    target_link_libraries(sdk_transaction_gc_example
        sdk
    )

    add_executable(sdk_transaction_gc_op_all_and_lock_example
        sdk_transaction_gc_op_all_and_lock_example.cc)
    target_link_libraries(sdk_transaction_gc_op_all_and_lock_example
        sdk
    )

    add_executable(sdk_vector_example
        sdk_vector_example.cc)
    target_link_libraries(sdk_vector_example
        sdk
    )

    add_executable(txn_scan_test
        txn_scan_test.cc)
    target_link_libraries(txn_scan_test
        sdk
    )

endif()
