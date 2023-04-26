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

#include "meta/meta_reader.h"

#include <cstddef>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/snapshot.h"

namespace dingodb {

std::shared_ptr<pb::common::KeyValue> MetaReader::Get(const std::string& key) { return Get(nullptr, key); }

bool MetaReader::Scan(const std::string& prefix, std::vector<pb::common::KeyValue>& kvs) {
  return Scan(nullptr, prefix, kvs);
}

// Get with specific snapshot
std::shared_ptr<pb::common::KeyValue> MetaReader::Get(std::shared_ptr<Snapshot> snapshot, const std::string& key) {
  auto reader = engine_->NewReader(Constant::kStoreMetaCF);
  std::string* value = new std::string();

  butil::Status status;
  if (snapshot) {
    status = reader->KvGet(snapshot, key, *value);
  } else {
    status = reader->KvGet(key, *value);
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta get failed, errcode: " << status.error_code() << " " << status.error_str();
    return nullptr;
  }

  std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(key);
  kv->set_allocated_value(value);

  return kv;
}

// Scan with specific snapshot
bool MetaReader::Scan(std::shared_ptr<Snapshot> snapshot, const std::string& prefix,
                      std::vector<pb::common::KeyValue>& kvs) {
  auto reader = engine_->NewReader(Constant::kStoreMetaCF);
  const std::string prefix_next = Helper::StringIncrement(prefix);
  butil::Status status;
  if (snapshot) {
    status = reader->KvScan(snapshot, prefix, prefix_next, kvs);
  } else {
    status = reader->KvScan(prefix, prefix_next, kvs);
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta scan failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }
  DINGO_LOG(INFO) << "Scan meta data, prefix: " << Helper::StringToHex(prefix) << "-"
                  << Helper::StringToHex(prefix_next) << ": " << kvs.size();

  return true;
}

}  // namespace dingodb
