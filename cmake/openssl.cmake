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
message(STATUS "Include openssl...")

set(OPENSSL_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/openssl)
set(OPENSSL_BINARY_DIR ${THIRD_PARTY_PATH}/build/openssl)
set(OPENSSL_INSTALL_DIR ${THIRD_PARTY_PATH}/install/openssl)
set(OPENSSL_INCLUDE_DIR
    "${OPENSSL_INSTALL_DIR}/include"
    CACHE PATH "openssl include directory." FORCE)
set(OPENSSL_LIBRARIES
    "${OPENSSL_INSTALL_DIR}/lib/libssl.a"
    CACHE FILEPATH "openssl library." FORCE)
set(CRYPTO_LIBRARIES
    "${OPENSSL_INSTALL_DIR}/lib/libcrypto.a"
    CACHE FILEPATH "openssl library." FORCE)

ExternalProject_Add(
  extern_openssl
  ${EXTERNAL_PROJECT_LOG_ARGS}
  SOURCE_DIR ${OPENSSL_SOURCES_DIR}
  BINARY_DIR ${OPENSSL_BINARY_DIR}
  PREFIX ${OPENSSL_BINARY_DIR}
  UPDATE_COMMAND ""
  CONFIGURE_COMMAND sh ${OPENSSL_SOURCES_DIR}/config -DOPENSSL_NO_SCTP -DOPENSSL_NO_KTLS -DOPENSSL_USE_NODELETE
                    -DOPENSSL_PIC -no-shared
  BUILD_COMMAND $(MAKE)
  INSTALL_COMMAND mkdir -p ${OPENSSL_INSTALL_DIR}/lib
  COMMAND cp ${OPENSSL_BINARY_DIR}/libssl.a ${OPENSSL_INSTALL_DIR}/lib
  COMMAND cp ${OPENSSL_BINARY_DIR}/libcrypto.a ${OPENSSL_INSTALL_DIR}/lib
  COMMAND cp -rf ${OPENSSL_SOURCES_DIR}/include ${OPENSSL_INSTALL_DIR}/
  COMMAND cp -rf ${OPENSSL_BINARY_DIR}/include ${OPENSSL_INSTALL_DIR}/)

add_library(openssl STATIC IMPORTED GLOBAL)
set_property(TARGET openssl PROPERTY IMPORTED_LOCATION ${OPENSSL_LIBRARIES})
add_dependencies(openssl extern_openssl)

add_library(crypto STATIC IMPORTED GLOBAL)
set_property(TARGET crypto PROPERTY IMPORTED_LOCATION ${CRYPTO_LIBRARIES})
add_dependencies(crypto extern_openssl)
