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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGODB_CLIENT_DOCUMENT_INDEX_H_
#define DINGODB_CLIENT_DOCUMENT_INDEX_H_

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "CLI/CLI.hpp"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"

namespace client_v2 {

void SetUpDocumentIndexSubCommands(CLI::App &app);

// document operation
struct DocumentDeleteOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t count;
};
void SetUpDocumentDelete(CLI::App &app);
void RunDocumentDelete(DocumentDeleteOptions const &opt);

struct DocumentAddOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t document_id;
  std::string document_text1;
  std::string document_text2;
  bool is_update;
};
void SetUpDocumentAdd(CLI::App &app);
void RunDocumentAdd(DocumentAddOptions const &opt);

struct DocumentSearchOptions {
  std::string coor_url;
  int64_t region_id;
  std::string query_string;
  int32_t topn;
  bool without_scalar;
};
void SetUpDocumentSearch(CLI::App &app);
void RunDocumentSearch(DocumentSearchOptions const &opt);

struct DocumentBatchQueryOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t document_id;
  bool without_scalar;
  std::string key;
};
void SetUpDocumentBatchQuery(CLI::App &app);
void RunDocumentBatchQuery(DocumentBatchQueryOptions const &opt);

struct DocumentScanQueryOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
  int64_t limit;
  bool is_reverse;
  bool without_scalar;
  std::string key;
};
void SetUpDocumentScanQuery(CLI::App &app);
void RunDocumentScanQuery(DocumentScanQueryOptions const &opt);

struct DocumentGetMaxIdOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpDocumentGetMaxId(CLI::App &app);
void RunDocumentGetMaxId(DocumentGetMaxIdOptions const &opt);

struct DocumentGetMinIdOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpDocumentGetMinId(CLI::App &app);
void RunDocumentGetMinId(DocumentGetMinIdOptions const &opt);

struct DocumentCountOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
};
void SetUpDocumentCount(CLI::App &app);
void RunDocumentCount(DocumentCountOptions const &opt);

struct DocumentGetRegionMetricsOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpDocumentGetRegionMetrics(CLI::App &app);
void RunDocumentGetRegionMetrics(DocumentGetRegionMetricsOptions const &opt);

// document
void SendDocumentAdd(DocumentAddOptions const &opt);
void SendDocumentDelete(DocumentDeleteOptions const &opt);
void SendDocumentSearch(DocumentSearchOptions const &opt);
void SendDocumentBatchQuery(DocumentBatchQueryOptions const &opt);
void SendDocumentGetMaxId(DocumentGetMaxIdOptions const &opt);
void SendDocumentGetMinId(DocumentGetMinIdOptions const &opt);
void SendDocumentScanQuery(DocumentScanQueryOptions const &opt);
int64_t SendDocumentCount(DocumentCountOptions const &opt);
void SendDocumentGetRegionMetrics(DocumentGetRegionMetricsOptions const &opt);

}  // namespace client_v2

#endif  // DINGODB_CLIENT_DOCUMENT_INDEX_H_