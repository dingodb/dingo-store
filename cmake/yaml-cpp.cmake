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

INCLUDE(ExternalProject)
message(STATUS "Include yaml-cpp...")

SET(YAMLCPP_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/yaml-cpp)
SET(YAMLCPP_BINARY_DIR ${THIRD_PARTY_PATH}/build/yaml-cpp)
SET(YAMLCPP_INSTALL_DIR ${THIRD_PARTY_PATH}/install/yamlcpp)
SET(YAMLCPP_INCLUDE_DIR "${YAMLCPP_INSTALL_DIR}/include" CACHE PATH "yamlcpp include directory." FORCE)
SET(YAMLCPP_LIBRARIES "${YAMLCPP_INSTALL_DIR}/lib/libyaml-cpp.a" CACHE FILEPATH "yamlcpp library." FORCE)

ExternalProject_Add(
    extern_yamlcpp
    ${EXTERNAL_PROJECT_LOG_ARGS}

    SOURCE_DIR ${YAMLCPP_SOURCES_DIR}
    BINARY_DIR ${YAMLCPP_BINARY_DIR}
    PREFIX ${YAMLCPP_BINARY_DIR}

    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_INSTALL_PREFIX=${YAMLCPP_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR=${YAMLCPP_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    -DCMAKE_PREFIX_PATH=${prefix_path}
    ${EXTERNAL_OPTIONAL_ARGS}
    LIST_SEPARATOR |
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${YAMLCPP_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${YAMLCPP_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
    BUILD_COMMAND $(MAKE)
    INSTALL_COMMAND mkdir -p ${YAMLCPP_INSTALL_DIR}/lib/ COMMAND cp ${YAMLCPP_BINARY_DIR}/libyaml-cpp${CMAKE_STATIC_LIBRARY_SUFFIX} ${YAMLCPP_LIBRARIES} COMMAND cp -r ${YAMLCPP_SOURCES_DIR}/include ${YAMLCPP_INCLUDE_DIR}/
)

ADD_LIBRARY(yamlcpp STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET yamlcpp PROPERTY IMPORTED_LOCATION ${YAMLCPP_LIBRARIES})
ADD_DEPENDENCIES(yamlcpp extern_yamlcpp)
