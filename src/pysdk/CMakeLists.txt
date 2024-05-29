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
project(pysdk)

find_package(SWIG 4.2 REQUIRED)

if(SWIG_FOUND)
  message("SWIG found: ${SWIG_EXECUTABLE}, dir: ${SWIG_DIR}, version: ${SWIG_VERSION}")
endif()

# Find Python 3
find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)

include(UseSWIG)

set(CMAKE_SWIG_FLAGS)
set(SWIG_SOURCE_FILE_EXTENSIONS swg)

# list(APPEND CMAKE_SWIG_FLAGS "-DSWIGWORDSIZE64" "-c++")

list(APPEND CMAKE_SWIG_FLAGS "-DSWIGWORDSIZE64" "-c++" "-DSWIG_TYPE_TABLE=aabbccdd")

set_property(SOURCE dingosdk.swg PROPERTY CPLUSPLUS ON)

if(SDK_ENABLE_GRPC)
  message(STATUS "pysdk: include dir ${CMAKE_CURRENT_SOURCE_DIR}/.." )
  include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)

  message(STATUS "pysdk: link dir ${CMAKE_CURRENT_BINARY_DIR}/../sdk" )
  link_directories("${CMAKE_CURRENT_BINARY_DIR}/../sdk")
endif()

swig_add_library(dingosdk 
  TYPE SHARED 
  LANGUAGE python 
  SOURCES dingosdk.swg
)
# -DSWIG_TYPE_TABLE=myprojectname

target_include_directories(dingosdk 
  PRIVATE 
  ${Python3_INCLUDE_DIRS}
)

if(SDK_ENABLE_GRPC)
    message(STATUS "pysdk: CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}" )
    message(STATUS "pysdk: GRPC_INSTALL_DIR ${GRPC_INSTALL_DIR}, THIRD_PARTY_PATH:${THIRD_PARTY_PATH}" )

    add_definitions(-DGLOG_CUSTOM_PREFIX_SUPPORT)

    find_package(utf8_range CONFIG REQUIRED)
    # set(GRPC_INSTALL_PATH ${THIRD_PARTY_PATH}/install/grpc)
    # set(protobuf_MODULE_COMPATIBLE TRUE)
    # find_package(Protobuf CONFIG REQUIRED HINTS ${GRPC_INSTALL_PATH})
    find_package(Protobuf CONFIG REQUIRED)
    message(STATUS "pysdk: Using protobuf ${Protobuf_VERSION},dir:${Protobuf_DIR}, inlucde:${Protobuf_INCLUDE_DIRS}" )
    # find_package(gRPC CONFIG REQUIRED HINTS ${GRPC_INSTALL_PATH})
    find_package(gRPC CONFIG REQUIRED)
    message(STATUS "pysdk: Using gRPC ${gRPC_VERSION}, include:${gRPC_INCLUDE_DIRS}")
  
    # gRPC CMake CONFIG gives the libraries slightly odd names, make them match
    # the conventional system-installed names.
    set_target_properties(protobuf::libprotobuf PROPERTIES IMPORTED_GLOBAL TRUE)
    add_library(grpc_protobuf ALIAS protobuf::libprotobuf)
  
    set_target_properties(gRPC::grpc++ PROPERTIES IMPORTED_GLOBAL TRUE)
    add_library(grpc++ ALIAS gRPC::grpc++)

    target_link_libraries(dingosdk
      PRIVATE
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

else()
    target_link_libraries(dingosdk
      PRIVATE
      sdk
    )
endif()

configure_file(setup.py setup.py COPYONLY)
configure_file(__init__.py __init__.py COPYONLY)
configure_file(loader.py loader.py COPYONLY)

configure_file(examples/pysdk_rawkv_example.py pysdk_rawkv_example.py COPYONLY)
configure_file(examples/pysdk_vector_example.py pysdk_vector_example.py COPYONLY)

