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

SET(OPENSSL_SOURCES_DIR ${THIRD_PARTY_PATH}/openssl)
SET(OPENSSL_INSTALL_DIR ${THIRD_PARTY_PATH}/install/openssl)
SET(OPENSSL_INCLUDE_DIR "${OPENSSL_INSTALL_DIR}/include" CACHE PATH "openssl include directory." FORCE)
SET(OPENSSL_LIBRARIES "${OPENSSL_INSTALL_DIR}/lib/libssl.a" CACHE FILEPATH "openssl library." FORCE)
SET(CRYPTO_LIBRARIES  "${OPENSSL_INSTALL_DIR}/lib/libcrypto.a" CACHE FILEPATH "openssl library." FORCE)

FILE(WRITE ${OPENSSL_SOURCES_DIR}/src/copy_repo.sh
        "mkdir -p ${OPENSSL_SOURCES_DIR}/src/extern_openssl/ && cp -rf ${CMAKE_SOURCE_DIR}/contrib/openssl/* ${OPENSSL_SOURCES_DIR}/src/extern_openssl/")

execute_process(COMMAND sh ${OPENSSL_SOURCES_DIR}/src/copy_repo.sh)

ExternalProject_Add(
        extern_openssl
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${OPENSSL_SOURCES_DIR}
        UPDATE_COMMAND ""
        SOURCE_DIR ${OPENSSL_SOURCES_DIR}/src/extern_openssl/
        CONFIGURE_COMMAND sh config -DOPENSSL_NO_SCTP -DOPENSSL_NO_KTLS  -DOPENSSL_USE_NODELETE -DOPENSSL_PIC -no-shared
        BUILD_IN_SOURCE 1
        # BUILD_COMMAND $(MAKE) -j ${NUM_OF_PROCESSOR}
        BUILD_COMMAND $(MAKE)
        INSTALL_COMMAND mkdir -p ${OPENSSL_INSTALL_DIR}/lib/ COMMAND cp ${OPENSSL_SOURCES_DIR}/src/extern_openssl/libssl.a ${OPENSSL_INSTALL_DIR}/lib/ COMMAND cp ${OPENSSL_SOURCES_DIR}/src/extern_openssl/libcrypto.a ${OPENSSL_INSTALL_DIR}/lib/ COMMAND cp -r ${OPENSSL_SOURCES_DIR}/src/extern_openssl/include ${OPENSSL_INSTALL_DIR}/
)

ADD_LIBRARY(openssl STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET openssl PROPERTY IMPORTED_LOCATION ${OPENSSL_LIBRARIES} ${CRYPTO_LIBRARIES})
ADD_DEPENDENCIES(openssl extern_openssl)
