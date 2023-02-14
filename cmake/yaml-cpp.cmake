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

SET(YAMLCPP_SOURCES_DIR ${THIRD_PARTY_PATH}/yamlcpp)
SET(YAMLCPP_INSTALL_DIR ${THIRD_PARTY_PATH}/install/yamlcpp)
SET(YAMLCPP_INCLUDE_DIR "${YAMLCPP_INSTALL_DIR}/include" CACHE PATH "yamlcpp include directory." FORCE)
SET(YAMLCPP_LIBRARIES "${YAMLCPP_INSTALL_DIR}/lib/libyaml-cpp.a" CACHE FILEPATH "yamlcpp library." FORCE)

ExternalProject_Add(
        extern_yamlcpp
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${YAMLCPP_SOURCES_DIR}
        # GIT_REPOSITORY "https://github.com/jbeder/yaml-cpp.git"
        # GIT_TAG "1b50109f7bea60bd382d8ea7befce3d2bd67da5f" # Just pick HEAD on 20230214
        URL "https://github.com/jbeder/yaml-cpp/archive/1b50109f7bea60bd382d8ea7befce3d2bd67da5f.tar.gz"
        BUILD_IN_SOURCE 1
        BUILD_COMMAND $(MAKE) -j ${NUM_OF_PROCESSOR}
        INSTALL_COMMAND mkdir -p ${YAMLCPP_INSTALL_DIR}/lib/ COMMAND cp ${YAMLCPP_SOURCES_DIR}/src/extern_yamlcpp/libyaml-cpp.a ${YAMLCPP_LIBRARIES} COMMAND cp -r ${YAMLCPP_SOURCES_DIR}/src/extern_yamlcpp/include ${YAMLCPP_INSTALL_DIR}/
)

ADD_LIBRARY(yamlcpp STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET yamlcpp PROPERTY IMPORTED_LOCATION ${YAMLCPP_LIBRARIES})
ADD_DEPENDENCIES(yamlcpp extern_yamlcpp)
