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

#ifndef DINGODB_DOCUMENT_READER_H_
#define DINGODB_DOCUMENT_READER_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "butil/status.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "proto/common.pb.h"
#include "proto/document.pb.h"

namespace dingodb {

// Document reader
class DocumentReader {
 public:
  DocumentReader(RawEngine::ReaderPtr reader) : reader_(reader) {}

  static std::shared_ptr<DocumentReader> New(RawEngine::ReaderPtr reader) {
    return std::make_shared<DocumentReader>(reader);
  }

  butil::Status DocumentSearch(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                               std::vector<pb::common::DocumentWithScore>& results);

  butil::Status DocumentBatchQuery(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                   std::vector<pb::common::DocumentWithId>& document_with_ids);

  butil::Status DocumentGetBorderId(const pb::common::Range& region_range, bool get_min, int64_t& document_id);

  butil::Status DocumentScanQuery(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                  std::vector<pb::common::DocumentWithId>& document_with_ids);

  butil::Status DocumentGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                         DocumentIndexWrapperPtr document_index,
                                         pb::common::DocumentIndexMetrics& region_metrics);

  butil::Status DocumentCount(const pb::common::Range& range, int64_t& count);

 private:
  butil::Status QueryDocumentWithId(const pb::common::Range& region_range, int64_t partition_id, int64_t document_id,
                                    pb::common::DocumentWithId& document_with_id);
  butil::Status SearchDocument(int64_t partition_id, DocumentIndexWrapperPtr document_index,
                               pb::common::Range region_range, const pb::common::DocumentSearchParameter& parameter,
                               std::vector<pb::common::DocumentWithScore>& document_with_score_results);

  butil::Status GetBorderId(const pb::common::Range& region_range, bool get_min, int64_t& document_id);
  butil::Status ScanDocumentId(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                               std::vector<int64_t>& document_ids);

  RawEngine::ReaderPtr reader_;
};

}  // namespace dingodb

#endif  // DINGODB_DOCUMENT_READER_H_