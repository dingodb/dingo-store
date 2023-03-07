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

SET(LZ4_SOURCES_DIR ${THIRD_PARTY_PATH}/lz4)
SET(LZ4_INSTALL_DIR ${THIRD_PARTY_PATH}/install/lz4)
SET(LZ4_INCLUDE_DIR "${LZ4_INSTALL_DIR}/include" CACHE PATH "lz4 include directory." FORCE)
SET(LZ4_LIBRARIES "${LZ4_INSTALL_DIR}/lib/liblz4.a" CACHE FILEPATH "lz4 library." FORCE)

FILE(WRITE ${LZ4_SOURCES_DIR}/src/copy_repo.sh
        "mkdir -p ${LZ4_SOURCES_DIR}/src/extern_lz4/ && cp -rf ${CMAKE_SOURCE_DIR}/contrib/lz4/* ${LZ4_SOURCES_DIR}/src/extern_lz4/")

execute_process(COMMAND sh ${LZ4_SOURCES_DIR}/src/copy_repo.sh)

ExternalProject_Add(
        extern_lz4
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${LZ4_SOURCES_DIR}
        UPDATE_COMMAND ""
        SOURCE_DIR ${LZ4_SOURCES_DIR}/src/extern_lz4/
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND $(MAKE)
        INSTALL_COMMAND mkdir -p ${LZ4_INSTALL_DIR}/lib/ COMMAND cp ${LZ4_SOURCES_DIR}/src/extern_lz4/lib/liblz4.a ${LZ4_INSTALL_DIR}/lib/ COMMAND cp -r ${LZ4_SOURCES_DIR}/src/extern_lz4/lib/lz4.h ${LZ4_INSTALL_DIR}/ COMMAND cp -r ${LZ4_SOURCES_DIR}/src/extern_lz4/lib/lz4hc.h ${LZ4_INSTALL_DIR}
)

ADD_LIBRARY(lz4 STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET lz4 PROPERTY IMPORTED_LOCATION ${LZ4_LIBRARIES})
ADD_DEPENDENCIES(lz4 extern_lz4)
