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

#include "common/helper.h"

namespace dingodb {

std::shared_ptr<pb::common::KeyValue> MetaReader::Get(const std::string& key) {
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->set_cf_name(kStoreMetaCF);

  std::string* value = new std::string();
  auto errcode = engine_->KvGet(ctx, key, *value);
  if (errcode != pb::error::OK) {
    LOG(ERROR) << "Meta get failed, errcode: " << errcode;
    return nullptr;
  }

  std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(key);
  kv->set_allocated_value(value);

  return kv;
}

bool MetaReader::Scan(const std::string& prefix, std::vector<pb::common::KeyValue>& kvs) {
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->set_cf_name(kStoreMetaCF);

  const std::string prefix_next = Helper::Increment(prefix);
  LOG(INFO) << "Scan meta data, prefix: " << Helper::StringToHex(prefix) << "-" << Helper::StringToHex(prefix_next);
  auto errcode = engine_->KvScan(ctx, prefix, prefix_next, kvs);
  if (errcode != pb::error::OK) {
    LOG(ERROR) << "Meta scan failed, errcode: " << errcode;
    return false;
  }
  LOG(INFO) << "Scan meta data, result kvs size: " << kvs.size();
  return true;
}

}  // namespace dingodb