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

#ifndef DINGODB_META_META_READER_H_
#define DINGODB_META_META_READER_H_

#include <memory>
#include <vector>

#include "engine/raw_engine.h"
#include "proto/common.pb.h"

namespace dingodb {

class MetaReader {
 public:
  MetaReader(std::shared_ptr<RawEngine> engine) : engine_(engine) {}
  ~MetaReader() = default;

  MetaReader(const MetaReader&) = delete;
  const MetaReader& operator=(const MetaReader&) = delete;

  std::shared_ptr<pb::common::KeyValue> Get(const std::string& key);
  butil::Status Get(const std::string& key, pb::common::KeyValue& kv);
  bool Scan(const std::string& prefix, std::vector<pb::common::KeyValue>& kvs);

  // with Snapshot
  butil::Status Get(std::shared_ptr<Snapshot> snapshot, const std::string& key, pb::common::KeyValue& kv);
  std::shared_ptr<pb::common::KeyValue> Get(std::shared_ptr<Snapshot> snapshot, const std::string& key);
  bool Scan(std::shared_ptr<Snapshot>, const std::string& prefix, std::vector<pb::common::KeyValue>& kvs);

 private:
  std::shared_ptr<RawEngine> engine_;
};

using MetaReaderPtr = std::shared_ptr<MetaReader>;

}  // namespace dingodb

#endif  // DINGODB_META_META_READER_H_