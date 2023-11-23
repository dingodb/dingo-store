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

#ifndef DINGODB_META_META_WRITER_H_
#define DINGODB_META_META_WRITER_H_

#include <memory>
#include <vector>

#include "engine/raw_engine.h"
#include "proto/common.pb.h"

namespace dingodb {

class MetaWriter {
 public:
  MetaWriter(std::shared_ptr<RawEngine> engine) : engine_(engine) {}
  ~MetaWriter() = default;

  MetaWriter(const MetaWriter &) = delete;
  const MetaWriter &operator=(const MetaWriter &) = delete;

  bool Put(std::shared_ptr<pb::common::KeyValue> kv);
  bool Put(std::vector<pb::common::KeyValue> kvs);
  bool PutAndDelete(std::vector<pb::common::KeyValue> kvs_to_put, std::vector<std::string> keys_to_delete);
  bool Delete(const std::string &key);
  bool DeleteRange(const std::string &start_key, const std::string &end_key);
  bool DeletePrefix(const std::string &prefix);

 private:
  std::shared_ptr<RawEngine> engine_;
};

using MetaWriterPtr = std::shared_ptr<MetaWriter>;

}  // namespace dingodb

#endif  // DINGODB_META_META_WRITER_H_