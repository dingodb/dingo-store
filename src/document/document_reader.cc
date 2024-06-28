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

#include "document/document_reader.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "document/codec.h"
#include "document/document_index.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

butil::Status DocumentReader::QueryDocumentWithId(int64_t ts, const pb::common::Range& region_range,
                                                  int64_t partition_id, int64_t document_id, bool with_scalar_data,
                                                  std::vector<std::string>& selected_scalar_keys,
                                                  pb::common::DocumentWithId& document_with_id) {
  std::string plain_key;
  DocumentCodec::PackageDocumentKey(Helper::GetKeyPrefix(region_range), partition_id, document_id, plain_key);

  std::string value;
  auto status = reader_->KvGet(Constant::kStoreDataCF, ts, plain_key, value);
  if (!status.ok()) {
    return status;
  }

  pb::common::Document document;
  if (!document.ParseFromString(value)) {
    return butil::Status(pb::error::EINTERNAL, "Parse proto from string error");
  }
  document_with_id.set_id(document_id);

  if (with_scalar_data) {
    if (selected_scalar_keys.empty()) {
      document_with_id.mutable_document()->Swap(&document);
      return butil::Status();
    } else {
      for (auto& key : selected_scalar_keys) {
        auto scalar = document.document_data().find(key);
        if (scalar == document.document_data().end()) {
          continue;
        }

        (*document_with_id.mutable_document()->mutable_document_data())[key] = scalar->second;
      }

      return butil::Status();
    }
  }

  return butil::Status();
}

butil::Status DocumentReader::SearchDocument(int64_t ts, int64_t partition_id, DocumentIndexWrapperPtr document_index,
                                             pb::common::Range region_range,
                                             const pb::common::DocumentSearchParameter& parameter,
                                             std::vector<pb::common::DocumentWithScore>& document_with_score_results) {
  bool with_scalar_data = !(parameter.without_scalar_data());
  std::vector<std::string> selected_scalar_keys;

  if (with_scalar_data) {
    for (const auto& scalar_key : parameter.selected_keys()) {
      selected_scalar_keys.push_back(scalar_key);
    }
  }

  auto ret = document_index->Search(region_range, parameter, document_with_score_results);

  // document index does not support restruct document, we restruct it using kv store
  if (with_scalar_data) {
    for (auto& document_with_score : document_with_score_results) {
      pb::common::DocumentWithId document_with_id;
      auto status = QueryDocumentWithId(ts, region_range, partition_id, document_with_score.document_with_id().id(),
                                        with_scalar_data, selected_scalar_keys, document_with_id);
      if (!status.ok()) {
        return status;
      }

      document_with_score.mutable_document_with_id()->Swap(&document_with_id);
    }
  }

  return butil::Status();
}

butil::Status DocumentReader::DocumentSearch(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                             std::vector<pb::common::DocumentWithScore>& results) {
  // Search documents by documents
  auto status =
      SearchDocument(ctx->ts, ctx->partition_id, ctx->document_index, ctx->region_range, ctx->parameter, results);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status DocumentReader::DocumentBatchQuery(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                                 std::vector<pb::common::DocumentWithId>& document_with_ids) {
  for (auto document_id : ctx->document_ids) {
    pb::common::DocumentWithId document_with_id;
    auto status = QueryDocumentWithId(ctx->ts, ctx->region_range, ctx->partition_id, document_id, ctx->with_scalar_data,
                                      ctx->selected_scalar_keys, document_with_id);
    if ((!status.ok()) && status.error_code() != pb::error::EKEY_NOT_FOUND) {
      DINGO_LOG(WARNING) << fmt::format("Query document_with_id failed, document_id: {} error: {}", document_id,
                                        status.error_str());
    }

    // if the id is not exist, the document_with_id will be empty, sdk client will handle this
    document_with_ids.push_back(document_with_id);
  }

  return butil::Status::OK();
}

butil::Status DocumentReader::DocumentGetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min,
                                                  int64_t& document_id) {
  auto status = GetBorderId(ts, region_range, get_min, document_id);
  if (!status.ok()) {
    DINGO_LOG(INFO) << "Get border document id failed, error: " << status.error_str();
    return status;
  }

  return butil::Status();
}

butil::Status DocumentReader::DocumentScanQuery(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                                std::vector<pb::common::DocumentWithId>& document_with_ids) {
  DINGO_LOG(INFO) << fmt::format("Scan document id, region_id: {} start_id: {} is_reverse: {} limit: {}",
                                 ctx->region_id, ctx->start_id, ctx->is_reverse, ctx->limit);

  // scan for ids
  std::vector<int64_t> document_ids;
  auto status = ScanDocumentId(ctx, document_ids);
  if (!status.ok()) {
    DINGO_LOG(INFO) << "Scan document id failed, error: " << status.error_str();
    return status;
  }

  DINGO_LOG(INFO) << "scan document id count: " << document_ids.size();

  if (document_ids.empty()) {
    return butil::Status();
  }

  // query document_with id
  for (auto document_id : document_ids) {
    pb::common::DocumentWithId document_with_id;
    auto status = QueryDocumentWithId(ctx->ts, ctx->region_range, ctx->partition_id, document_id, ctx->with_scalar_data,
                                      ctx->selected_scalar_keys, document_with_id);
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format("Query document data failed, document_id {} error: {}", document_id,
                                        status.error_str());
    }

    // if the id is not exist, the document_with_id will be empty, sdk client will handle this
    document_with_ids.push_back(document_with_id);
  }

  return butil::Status::OK();
}

butil::Status DocumentReader::DocumentGetRegionMetrics(int64_t /*region_id*/, const pb::common::Range& region_range,
                                                       DocumentIndexWrapperPtr document_index,
                                                       pb::common::DocumentIndexMetrics& region_metrics) {
  int64_t total_doc_count = 0;
  int64_t total_token_count = 0;
  int64_t max_id = 0;
  int64_t min_id = 0;
  std::string meta_json, json_parameter;

  auto inner_document_index = document_index->GetOwnDocumentIndex();
  if (inner_document_index == nullptr) {
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.",
                         document_index->Id());
  }

  auto status = inner_document_index->GetDocCount(total_doc_count);
  if (!status.ok()) {
    return status;
  }

  status = inner_document_index->GetTokenCount(total_token_count);
  if (!status.ok()) {
    return status;
  }

  status = GetBorderId(0, region_range, true, min_id);
  if (!status.ok()) {
    return status;
  }

  status = GetBorderId(0, region_range, false, max_id);
  if (!status.ok()) {
    return status;
  }

  region_metrics.set_total_num_docs(total_doc_count);
  region_metrics.set_total_num_tokens(total_token_count);
  region_metrics.set_max_id(max_id);
  region_metrics.set_min_id(min_id);

  return butil::Status();
}

butil::Status DocumentReader::DocumentCount(int64_t ts, const pb::common::Range& range, int64_t& count) {
  const std::string& begin_key = range.start_key();
  const std::string& end_key = range.end_key();

  return reader_->KvCount(Constant::kStoreDataCF, ts, begin_key, end_key, count);
}

// GetBorderId
butil::Status DocumentReader::GetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min,
                                          int64_t& document_id) {
  const std::string& start_key = region_range.start_key();
  const std::string& end_key = region_range.end_key();

  std::string plain_key;
  if (get_min) {
    auto status = reader_->KvMinKey(Constant::kStoreDataCF, ts, start_key, end_key, plain_key);
    if (!status.ok()) {
      return status;
    }

  } else {
    auto status = reader_->KvMaxKey(Constant::kStoreDataCF, ts, start_key, end_key, plain_key);
    if (!status.ok()) {
      return status;
    }
  }

  document_id = plain_key.empty() ? 0 : DocumentCodec::UnPackageDocumentId(plain_key);

  return butil::Status::OK();
}

// ScanDocumentId
butil::Status DocumentReader::ScanDocumentId(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                             std::vector<int64_t>& document_ids) {
  const auto& range = ctx->region_range;
  std::string encode_seek_key;
  DocumentCodec::EncodeDocumentKey(Helper::GetKeyPrefix(range), ctx->partition_id, ctx->start_id, encode_seek_key);
  auto encode_range = mvcc::Codec::EncodeRange(range);

  IteratorOptions options;
  if (!ctx->is_reverse) {
    if (encode_seek_key < encode_range.start_key()) {
      encode_seek_key = encode_range.start_key();
    }

    if (encode_seek_key >= encode_range.end_key()) {
      return butil::Status::OK();
    }

    options.upper_bound = encode_range.end_key();
    auto iter = reader_->NewIterator(Constant::kStoreDataCF, ctx->ts, options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range {}", Helper::RangeToString(range));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }
    for (iter->Seek(encode_seek_key); iter->Valid(); iter->Next()) {
      pb::common::DocumentWithId document;

      std::string key(iter->Key());
      auto document_id = DocumentCodec::DecodeDocumentIdFromEncodeKeyWithTs(key);
      CHECK(document_id > 0) << fmt::format("document_id({}) is invaild", document_id);

      if (ctx->end_id != 0 && document_id > ctx->end_id) {
        break;
      }

      document_ids.push_back(document_id);
      if (document_ids.size() >= ctx->limit) {
        break;
      }
    }
  } else {
    if (encode_seek_key > encode_range.end_key()) {
      encode_seek_key = encode_range.end_key();
    }

    if (encode_seek_key < encode_range.start_key()) {
      return butil::Status::OK();
    }

    options.lower_bound = encode_range.start_key();
    auto iter = reader_->NewIterator(Constant::kStoreDataCF, ctx->ts, options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range {}", Helper::RangeToString(range));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }

    for (iter->SeekForPrev(encode_seek_key); iter->Valid(); iter->Prev()) {
      std::string key(iter->Key());
      auto document_id = DocumentCodec::DecodeDocumentIdFromEncodeKeyWithTs(key);
      CHECK(document_id > 0) << fmt::format("document_id({}) is invaild", document_id);

      if (ctx->end_id != 0 && document_id < ctx->end_id) {
        break;
      }

      document_ids.push_back(document_id);
      if (document_ids.size() >= ctx->limit) {
        break;
      }
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb
