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

#ifndef DINGODB_DOCUMENT_INDEX_FACTORY_H_
#define DINGODB_DOCUMENT_INDEX_FACTORY_H_

#include <cstdint>
#include <memory>

#include "document/document_index.h"
#include "proto/common.pb.h"

namespace dingodb {

class DocumentIndexFactory {
 public:
  DocumentIndexFactory() = delete;
  ~DocumentIndexFactory() = delete;

  DocumentIndexFactory(const DocumentIndexFactory& rhs) = delete;
  DocumentIndexFactory& operator=(const DocumentIndexFactory& rhs) = delete;
  DocumentIndexFactory(DocumentIndexFactory&& rhs) = delete;
  DocumentIndexFactory& operator=(DocumentIndexFactory&& rhs) = delete;

  static std::shared_ptr<DocumentIndex> LoadIndex(int64_t id, const std::string& index_path,
                                                  const pb::common::DocumentIndexParameter& document_index_parameter,
                                                  const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                                  butil::Status& status);
  static std::shared_ptr<DocumentIndex> CreateIndex(int64_t id, const std::string& index_path,
                                                    const pb::common::DocumentIndexParameter& document_index_parameter,
                                                    const pb::common::RegionEpoch& epoch,
                                                    const pb::common::Range& range, bool force_create,
                                                    butil::Status& status);
  static std::shared_ptr<DocumentIndex> LoadOrCreateIndex(
      int64_t id, const std::string& index_path, const pb::common::DocumentIndexParameter& document_index_parameter,
      const pb::common::RegionEpoch& epoch, const pb::common::Range& range, butil::Status& status);
};

}  // namespace dingodb

#endif  // DINGODB_DOCUMENT_INDEX_FACTORY_H_
