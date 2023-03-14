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

#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"

namespace dingodb {

bool MetaWriter::Put(const std::shared_ptr<pb::common::KeyValue> kv) {
  LOG(INFO) << "Put meta data, key: " << Helper::StringToHex(kv->key());
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);
  auto status = writer->KvPut(*kv.get());
  if (!status.ok()) {
    LOG(ERROR) << "Meta write failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::Put(const std::vector<pb::common::KeyValue> kvs) {
  LOG(INFO) << "Put meta data, key nums: " << kvs.size();
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);
  auto status = writer->KvBatchPut(kvs);
  if (!status.ok()) {
    LOG(ERROR) << "Meta batch write failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::PutAndDelete(std::vector<pb::common::KeyValue> kvs_put, std::vector<pb::common::KeyValue> kvs_delete) {
  LOG(INFO) << "PutAndDelete meta data, key_put nums: " << kvs_put.size() << " key_delete nums:" << kvs_delete.size();
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);
  auto status = writer->KvBatchPutAndDelete(kvs_put, kvs_delete);
  if (!status.ok()) {
    LOG(ERROR) << "Meta batch write and delete failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::Delete(const std::string& key) {
  LOG(INFO) << "Delete meta data, key: " << key;
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);
  auto status = writer->KvDelete(key);
  if (!status.ok()) {
    LOG(ERROR) << "Meta delete failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

}  // namespace dingodb
