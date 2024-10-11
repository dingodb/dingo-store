// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "document/document_index_factory.h"

#include <cstdint>
#include <memory>

#include "common/helper.h"
#include "common/logging.h"
#include "document/document_index.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "tantivy_search.h"

namespace dingodb {

DocumentIndexPtr DocumentIndexFactory::LoadIndex(int64_t id, const std::string& index_path,
                                                 const pb::common::DocumentIndexParameter& parameter,
                                                 const pb::common::RegionEpoch& epoch, const pb::common::Range& range) {
  if (!Helper::IsExistPath(index_path)) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] index path not exist, path: {}.", id, index_path);
    return nullptr;
  }

  if (parameter.json_parameter().empty()) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] json_parameter is empty.", id);
    return nullptr;
  }

  auto bool_result = ffi_load_index_reader(index_path);
  if (!bool_result.result) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] load index reader failed, error: {} {}", id,
                                    bool_result.error_code, bool_result.error_msg.c_str());
    return nullptr;
  }

  bool_result = ffi_load_index_writer(index_path);
  if (!bool_result.result) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] load index writer failed, error: {} {}", id,
                                    bool_result.error_code, bool_result.error_msg.c_str());

    // index reader is loaded in ffi_load, need to free it before error return
    auto free_ret = ffi_free_index_reader(index_path);
    if (!free_ret.result) {
      DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] free index reader failed, error: {} {}", id,
                                      free_ret.error_code, free_ret.error_msg.c_str());
    }

    return nullptr;
  }

  auto document_index = std::make_shared<DocumentIndex>(id, index_path, parameter, epoch, range);
  CHECK(document_index != nullptr) << "alloc DocumentIndex fail.";

  return document_index;
}

DocumentIndexPtr DocumentIndexFactory::CreateIndex(int64_t id, const std::string& index_path,
                                                   const pb::common::DocumentIndexParameter& parameter,
                                                   const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                                   bool force_create) {
  if (Helper::IsExistPath(index_path)) {
    if (!force_create) {
      DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] index path already exist, path: {}", id,
                                      index_path);
      return nullptr;
    } else {
      if (!Helper::RemoveAllFileOrDirectory(index_path)) {
        DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] remove index path fail, path: {}", id,
                                        index_path);
        return nullptr;
      }
    }
  }

  if (parameter.json_parameter().empty()) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] json_parameter is empty", id);
    return nullptr;
  }

  std::vector<std::string> column_names;
  for (const auto& scalar_field : parameter.scalar_schema().fields()) {
    column_names.push_back(scalar_field.key());
  }

  if (column_names.empty()) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] column_names is empty", id);
    return nullptr;
  }

  auto bool_result = ffi_create_index_with_parameter(index_path, column_names, parameter.json_parameter());

  if (!bool_result.result) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] create index fail, error: {} {}", id,
                                    bool_result.error_code, bool_result.error_msg.c_str());
    return nullptr;
  }

  auto load_reader_ret = ffi_load_index_reader(index_path);
  if (!load_reader_ret.result) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] load index reader fail, error: {} {}", id,
                                    load_reader_ret.error_code, load_reader_ret.error_msg.c_str());

    // index write is loaded in ffi_create, need to free it before error return
    auto free_ret = ffi_free_index_writer(index_path);
    if (!free_ret.result) {
      DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] free index writer fail, error: {} {}", id,
                                      free_ret.error_code, free_ret.error_msg.c_str());
    }

    return nullptr;
  }

  auto document_index = std::make_shared<DocumentIndex>(id, index_path, parameter, epoch, range);
  CHECK(document_index != nullptr) << "alloc DocumentIndex fail.";

  return document_index;
}

}  // namespace dingodb
