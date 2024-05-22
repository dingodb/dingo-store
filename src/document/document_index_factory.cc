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
#include "proto/common.pb.h"
#include "tantivy_search.h"

namespace dingodb {

std::shared_ptr<DocumentIndex> DocumentIndexFactory::LoadIndex(
    int64_t id, const std::string& index_path, const pb::common::DocumentIndexParameter& document_index_parameter,
    const pb::common::RegionEpoch& epoch, const pb::common::Range& range, butil::Status& status) {
  if (!Helper::IsExistPath(index_path)) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] index path not exist, path: {}", id, index_path);
    status = butil::Status(pb::error::EINTERNAL, err_msg);
    return nullptr;
  }

  if (document_index_parameter.json_parameter().empty()) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] json_parameter is empty", id);
    status = butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
    return nullptr;
  }

  auto bool_result = ffi_load_index_reader(index_path);
  if (!bool_result.result) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] load index reader failed, error: {}, error_msg: {}",
                                      id, bool_result.error_code, bool_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    status = butil::Status(pb::error::EINTERNAL, err_msg);
    return nullptr;
  }

  bool_result = ffi_load_index_writer(index_path);
  if (!bool_result.result) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] load index writer failed, error: {}, error_msg: {}",
                                      id, bool_result.error_code, bool_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    status = butil::Status(pb::error::EINTERNAL, err_msg);

    // index reader is loaded in ffi_load, need to free it before error return
    auto free_ret = ffi_free_index_reader(index_path);
    if (!free_ret.result) {
      DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] free index reader failed, error: {}, error_msg: {}",
                                      id, free_ret.error_code, free_ret.error_msg.c_str());
    }

    return nullptr;
  }

  auto document_index = std::make_shared<DocumentIndex>(id, index_path, document_index_parameter, epoch, range);
  if (document_index == nullptr) {
    status = butil::Status(pb::error::EINTERNAL, "new DocumentIndex failed");
    return nullptr;
  }

  return document_index;
}

std::shared_ptr<DocumentIndex> DocumentIndexFactory::CreateIndex(
    int64_t id, const std::string& index_path, const pb::common::DocumentIndexParameter& document_index_parameter,
    const pb::common::RegionEpoch& epoch, const pb::common::Range& range, bool force_create, butil::Status& status) {
  if (Helper::IsExistPath(index_path)) {
    if (!force_create) {
      std::string err_msg =
          fmt::format("[document_index.raw][id({})] index path already exist, path: {}", id, index_path);
      status = butil::Status(pb::error::EINTERNAL, err_msg);
      return nullptr;
    } else {
      if (!Helper::RemoveAllFileOrDirectory(index_path)) {
        std::string err_msg =
            fmt::format("[document_index.raw][id({})] remove index path failed, path: {}", id, index_path);
        status = butil::Status(pb::error::EINTERNAL, err_msg);
        return nullptr;
      }
    }
  }

  if (document_index_parameter.json_parameter().empty()) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] json_parameter is empty", id);
    status = butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
    return nullptr;
  }

  std::vector<std::string> column_names;
  for (const auto& scalar_field : document_index_parameter.scalar_schema().fields()) {
    column_names.push_back(scalar_field.key());
  }

  if (column_names.empty()) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] column_names is empty", id);
    status = butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
    return nullptr;
  }

  auto bool_result =
      ffi_create_index_with_parameter(index_path, column_names, document_index_parameter.json_parameter());

  if (bool_result.result) {
    auto load_reader_ret = ffi_load_index_reader(index_path);
    if (!load_reader_ret.result) {
      std::string err_msg =
          fmt::format("[document_index.raw][id({})] load index reader failed, error: {}, error_msg: {}", id,
                      load_reader_ret.error_code, load_reader_ret.error_msg.c_str());
      DINGO_LOG(ERROR) << err_msg;
      status = butil::Status(pb::error::EINTERNAL, err_msg);

      // index write is loaded in ffi_create, need to free it before error return
      auto free_ret = ffi_free_index_writer(index_path);
      if (!free_ret.result) {
        DINGO_LOG(ERROR) << fmt::format(
            "[document_index.raw][id({})] free index writer failed, error: {}, error_msg: {}", id, free_ret.error_code,
            free_ret.error_msg.c_str());
      }

      return nullptr;
    }

    auto document_index = std::make_shared<DocumentIndex>(id, index_path, document_index_parameter, epoch, range);
    if (document_index == nullptr) {
      status = butil::Status(pb::error::EINTERNAL, "new DocumentIndex failed");
      return nullptr;
    }

    return document_index;
  } else {
    std::string err_msg = fmt::format("[document_index.raw][id({})] create index failed, error: {}, error_msg: {}", id,
                                      bool_result.error_code, bool_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    status = butil::Status(pb::error::EINTERNAL, err_msg);
    return nullptr;
  }
}

std::shared_ptr<DocumentIndex> DocumentIndexFactory::LoadOrCreateIndex(
    int64_t id, const std::string& index_path, const pb::common::DocumentIndexParameter& document_index_parameter,
    const pb::common::RegionEpoch& epoch, const pb::common::Range& range, butil::Status& status) {
  butil::Status tmp_status;
  auto document_index = LoadIndex(id, index_path, document_index_parameter, epoch, range, tmp_status);
  if (document_index != nullptr) {
    status = tmp_status;
    return document_index;
  }

  document_index = CreateIndex(id, index_path, document_index_parameter, epoch, range, true, tmp_status);
  if (document_index != nullptr) {
    return document_index;
  } else {
    status = tmp_status;
    return nullptr;
  }
}

}  // namespace dingodb
