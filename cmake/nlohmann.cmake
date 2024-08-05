# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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
message(STATUS "Include nlohmann json...")

set(NLOHMANN_JSON_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/nlohmann-json)
set(NLOHMANN_JSON_BINARY_DIR ${THIRD_PARTY_PATH}/build/nlohmann-json)
set(NLOHMANN_JSON_INSTALL_DIR ${THIRD_PARTY_PATH}/install/nlohmann-json)
set(NLOHMANN_JSON_INCLUDE_DIR
    "${NLOHMANN_JSON_INSTALL_DIR}/include"
    CACHE PATH "nlohmann_json include directory." FORCE)

ExternalProject_Add(
  nlohmann-json
  ${EXTERNAL_PROJECT_LOG_ARGS}
  SOURCE_DIR ${NLOHMANN_JSON_SOURCES_DIR}
  BINARY_DIR ${NLOHMANN_JSON_BINARY_DIR}
  PREFIX ${NLOHMANN_JSON_BINARY_DIR}
  UPDATE_COMMAND ""
  CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_CXX_FLAGS=${GLOG_CMAKE_CXX_FLAGS}
             -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
             -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
             -DCMAKE_INSTALL_PREFIX=${NLOHMANN_JSON_INSTALL_DIR}
             -DCMAKE_POSITION_INDEPENDENT_CODE=ON
             -DJSON_BuildTests=OFF
             -DJSON_MultipleHeaders=ON
             -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE})
