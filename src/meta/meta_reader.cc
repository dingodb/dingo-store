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

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/snapshot.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace dingodb {

std::shared_ptr<pb::common::KeyValue> MetaReader::Get(const std::string& key) { return Get(nullptr, key); }

butil::Status MetaReader::Get(const std::string& key, pb::common::KeyValue& kv) { return Get(nullptr, key, kv); }

bool MetaReader::Scan(const std::string& prefix, std::vector<pb::common::KeyValue>& kvs) {
  return Scan(nullptr, prefix, kvs);
}

// Get with specific snapshot
std::shared_ptr<pb::common::KeyValue> MetaReader::Get(std::shared_ptr<Snapshot> snapshot, const std::string& key) {
  auto reader = engine_->Reader();
  std::string value;

  butil::Status status;
  if (snapshot) {
    status = reader->KvGet(Constant::kStoreMetaCF, snapshot, key, value);
  } else {
    status = reader->KvGet(Constant::kStoreMetaCF, key, value);
  }
  if (!status.ok() && status.error_code() != pb::error::EKEY_NOT_FOUND) {
    DINGO_LOG(ERROR) << fmt::format("Meta get key {} failed, errcode: {} {}", key, status.error_code(),
                                    status.error_str());
    return nullptr;
  }

  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(key);
  kv->set_value(value);

  return kv;
}

butil::Status MetaReader::Get(std::shared_ptr<Snapshot> snapshot, const std::string& key, pb::common::KeyValue& kv) {
  auto reader = engine_->Reader();
  std::string value;

  butil::Status status;
  if (snapshot) {
    status = reader->KvGet(Constant::kStoreMetaCF, snapshot, key, value);
  } else {
    status = reader->KvGet(Constant::kStoreMetaCF, key, value);
  }
  if (!status.ok() && status.error_code() != pb::error::EKEY_NOT_FOUND) {
    DINGO_LOG(ERROR) << fmt::format("Meta get key {} failed, errcode: {} {}", key, status.error_code(),
                                    status.error_str());
    return status;
  }

  kv.set_key(key);
  kv.set_value(value);

  return butil::Status::OK();
}

// Scan with specific snapshot
bool MetaReader::Scan(std::shared_ptr<Snapshot> snapshot, const std::string& prefix,
                      std::vector<pb::common::KeyValue>& kvs) {
  auto reader = engine_->Reader();
  const std::string prefix_next = Helper::PrefixNext(prefix);
  butil::Status status;
  if (snapshot) {
    status = reader->KvScan(Constant::kStoreMetaCF, snapshot, prefix, prefix_next, kvs);
  } else {
    status = reader->KvScan(Constant::kStoreMetaCF, prefix, prefix_next, kvs);
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta scan failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }
  DINGO_LOG(DEBUG) << "Scan meta data, prefix: " << prefix << " count: " << kvs.size();

  return true;
}

}  // namespace dingodb
