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

#include "meta/meta_writer.h"

#include "common/context.h"
#include "common/helper.h"

namespace dingodb {

bool MetaWriter::Put(const std::shared_ptr<pb::common::KeyValue> kv) {
  LOG(INFO) << "Put meta data, key: " << Helper::StringToHex(kv->key());
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->set_cf_name(kStoreMetaCF);
  ctx->set_flush(true);
  auto errcode = engine_->KvPut(ctx, *kv);
  if (errcode != pb::error::OK) {
    LOG(ERROR) << "Meta write failed, errcode: " << errcode;
    return false;
  }

  return true;
}

bool MetaWriter::Put(const std::vector<pb::common::KeyValue> kvs) {
  LOG(INFO) << "Put meta data, key nums: " << kvs.size();
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->set_cf_name(kStoreMetaCF);
  ctx->set_flush(true);
  auto errcode = engine_->KvBatchPut(ctx, kvs);
  if (errcode != pb::error::OK) {
    LOG(ERROR) << "Meta batch write failed, errcode: " << errcode;
    return false;
  }

  return true;
}

bool MetaWriter::Delete(const std::string& key) {
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->set_cf_name(kStoreMetaCF);

  auto errcode = engine_->KvDelete(ctx, key);
  if (errcode != pb::error::OK) {
    LOG(ERROR) << "Meta delete failed, errcode: " << errcode;
    return false;
  }

  return true;
}

}  // namespace dingodb
