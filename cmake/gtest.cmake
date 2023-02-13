# Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

INCLUDE(ExternalProject)

SET(GTEST_SOURCES_DIR ${THIRD_PARTY_PATH}/gtest)
SET(GTEST_INSTALL_DIR ${THIRD_PARTY_PATH}/install/gtest)
SET(GTEST_INCLUDE_DIR "${GTEST_INSTALL_DIR}/include" CACHE PATH "gtest include directory." FORCE)
SET(GTEST_LIBRARIES "${GTEST_INSTALL_DIR}/lib/libgtest.a" CACHE FILEPATH "gtest library." FORCE)
SET(GTEST_MAIN_LIBRARIES "${GTEST_INSTALL_DIR}/lib/libgtest_main.a" CACHE FILEPATH "gtest library." FORCE)
SET(GMOCK_LIBRARIES "${GTEST_INSTALL_DIR}/lib/libgmock.a" CACHE FILEPATH "gmock library." FORCE)
SET(GMOCK_MAIN_LIBRARIES "${GTEST_INSTALL_DIR}/lib/libgmock_main.a" CACHE FILEPATH "gmock library." FORCE)

ExternalProject_Add(
        extern_gtest
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${GTEST_SOURCES_DIR}
        GIT_REPOSITORY "https://github.com/google/googletest.git"
        GIT_TAG "v1.13.0"
        BUILD_IN_SOURCE 1
        BUILD_COMMAND $(MAKE) -j ${NUM_OF_PROCESSOR}
        INSTALL_COMMAND mkdir -p ${GTEST_INSTALL_DIR}/lib/ COMMAND cp ${GTEST_SOURCES_DIR}/src/extern_gtest/lib/libgtest.a ${GTEST_LIBRARIES} COMMAND cp ${GTEST_SOURCES_DIR}/src/extern_gtest/lib/libgtest_main.a ${GTEST_MAIN_LIBRARIES} COMMAND cp ${GTEST_SOURCES_DIR}/src/extern_gtest/lib/libgmock.a ${GMOCK_LIBRARIES} COMMAND cp ${GTEST_SOURCES_DIR}/src/extern_gtest/lib/libgmock_main.a ${GMOCK_MAIN_LIBRARIES} COMMAND cp -r ${GTEST_SOURCES_DIR}/src/extern_gtest/googletest/include ${GTEST_INSTALL_DIR} COMMAND cp -r ${GTEST_SOURCES_DIR}/src/extern_gtest/googlemock/include ${GTEST_INSTALL_DIR}/
)

ADD_LIBRARY(gtest STATIC IMPORTED GLOBAL)
ADD_LIBRARY(gtest_main STATIC IMPORTED GLOBAL)
ADD_LIBRARY(gmock STATIC IMPORTED GLOBAL)
ADD_LIBRARY(gmock_main STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET gtest PROPERTY IMPORTED_LOCATION ${GTEST_LIBRARIES})
SET_PROPERTY(TARGET gtest_main PROPERTY IMPORTED_LOCATION ${GTEST_MAIN_LIBRARIES})
SET_PROPERTY(TARGET gmock PROPERTY IMPORTED_LOCATION ${GMOCK_LIBRARIES})
SET_PROPERTY(TARGET gmock_main PROPERTY IMPORTED_LOCATION ${GMOCK_MAIN_LIBRARIES})
ADD_DEPENDENCIES(gtest extern_gtest)
ADD_DEPENDENCIES(gtest_main extern_gtest)
ADD_DEPENDENCIES(gmock extern_gtest)
ADD_DEPENDENCIES(gmock_main extern_gtest)
