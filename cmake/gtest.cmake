# Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

include(ExternalProject)
message(STATUS "Include gtest...")

set(GTEST_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/gtest)
set(GTEST_BINARY_DIR ${THIRD_PARTY_PATH}/build/gtest)
set(GTEST_INSTALL_DIR ${THIRD_PARTY_PATH}/install/gtest)
set(GTEST_INCLUDE_DIR
    "${GTEST_INSTALL_DIR}/include"
    CACHE PATH "gtest include directory." FORCE)
set(GMOCK_INCLUDE_DIR
    "${GTEST_INSTALL_DIR}/include/include"
    CACHE PATH "gmock include directory." FORCE)
set(GTEST_LIBRARIES
    "${GTEST_INSTALL_DIR}/lib/libgtest.a"
    CACHE FILEPATH "gtest library." FORCE)
set(GTEST_MAIN_LIBRARIES
    "${GTEST_INSTALL_DIR}/lib/libgtest_main.a"
    CACHE FILEPATH "gtest library." FORCE)
set(GMOCK_LIBRARIES
    "${GTEST_INSTALL_DIR}/lib/libgmock.a"
    CACHE FILEPATH "gmock library." FORCE)
set(GMOCK_MAIN_LIBRARIES
    "${GTEST_INSTALL_DIR}/lib/libgmock_main.a"
    CACHE FILEPATH "gmock library." FORCE)

ExternalProject_Add(
  extern_gtest
  ${EXTERNAL_PROJECT_LOG_ARGS}
  SOURCE_DIR ${GTEST_SOURCES_DIR}
  BINARY_DIR ${GTEST_BINARY_DIR}
  PREFIX ${GTEST_BINARY_DIR}
  UPDATE_COMMAND ""
  CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
             -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
             -DCMAKE_INSTALL_PREFIX=${GTEST_INSTALL_DIR}
             -DCMAKE_INSTALL_LIBDIR=${GTEST_INSTALL_DIR}/lib
             -DCMAKE_POSITION_INDEPENDENT_CODE=ON
             -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
             ${EXTERNAL_OPTIONAL_ARGS}
  LIST_SEPARATOR |
  CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${GTEST_INSTALL_DIR} -DCMAKE_INSTALL_LIBDIR:PATH=${GTEST_INSTALL_DIR}/lib
                   -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
  BUILD_COMMAND $(MAKE)
  INSTALL_COMMAND mkdir -p ${GTEST_INSTALL_DIR}/lib/
  COMMAND cp ${GTEST_BINARY_DIR}/lib/libgtest.a ${GTEST_LIBRARIES}
  COMMAND cp ${GTEST_BINARY_DIR}/lib/libgtest_main.a ${GTEST_MAIN_LIBRARIES}
  COMMAND cp ${GTEST_BINARY_DIR}/lib/libgmock.a ${GMOCK_LIBRARIES}
  COMMAND cp ${GTEST_BINARY_DIR}/lib/libgmock_main.a ${GMOCK_MAIN_LIBRARIES}
  COMMAND cp -r ${GTEST_SOURCES_DIR}/googletest/include ${GTEST_INCLUDE_DIR}
  COMMAND cp -r ${GTEST_SOURCES_DIR}/googlemock/include ${GTEST_INCLUDE_DIR}/)

add_library(gtest STATIC IMPORTED GLOBAL)
add_library(gtest_main STATIC IMPORTED GLOBAL)
add_library(gmock STATIC IMPORTED GLOBAL)
add_library(gmock_main STATIC IMPORTED GLOBAL)
set_property(TARGET gtest PROPERTY IMPORTED_LOCATION ${GTEST_LIBRARIES})
set_property(TARGET gtest_main PROPERTY IMPORTED_LOCATION ${GTEST_MAIN_LIBRARIES})
set_property(TARGET gmock PROPERTY IMPORTED_LOCATION ${GMOCK_LIBRARIES})
set_property(TARGET gmock_main PROPERTY IMPORTED_LOCATION ${GMOCK_MAIN_LIBRARIES})
add_dependencies(gtest extern_gtest)
add_dependencies(gtest_main extern_gtest)
add_dependencies(gmock extern_gtest)
add_dependencies(gmock_main extern_gtest)
