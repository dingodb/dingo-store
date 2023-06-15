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
#include "common/helper.h"
#include "common/logging.h"
#include "proto/common.pb.h"

namespace dingodb {

bool MetaWriter::Put(const std::shared_ptr<pb::common::KeyValue> kv) {
  if (kv == nullptr) return true;
  DINGO_LOG(DEBUG) << "Put meta data, key: " << kv->key();
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);
  auto status = writer->KvPut(*kv);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta write failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::Put(const std::vector<pb::common::KeyValue> kvs) {
  DINGO_LOG(DEBUG) << "Put meta data, key nums: " << kvs.size();
  if (kvs.empty()) return true;
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);
  auto status = writer->KvBatchPut(kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta batch write failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::PutAndDelete(std::vector<pb::common::KeyValue> kvs_put, std::vector<pb::common::KeyValue> kvs_delete) {
  DINGO_LOG(DEBUG) << "PutAndDelete meta data, key_put nums: " << kvs_put.size()
                   << " key_delete nums:" << kvs_delete.size();
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);
  auto status = writer->KvBatchPutAndDelete(kvs_put, kvs_delete);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta batch write and delete failed, errcode: " << status.error_code() << " "
                     << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::Delete(const std::string& key) {
  DINGO_LOG(DEBUG) << "Delete meta data, key: " << key;
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);
  auto status = writer->KvDelete(key);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta delete failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::DeleteRange(const std::string& start_key, const std::string& end_key) {
  DINGO_LOG(DEBUG) << "DeleteRange meta data, start_key: " << start_key << " end_key: " << end_key;
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);

  pb::common::Range range;
  range.set_start_key(start_key);
  range.set_end_key(end_key);

  auto status = writer->KvDeleteRange(range);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta delete_range failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::DeletePrefix(const std::string& prefix) {
  DINGO_LOG(DEBUG) << "DeletePrefix meta data, prefix: " << prefix;
  auto writer = engine_->NewWriter(Constant::kStoreMetaCF);

  pb::common::Range range;
  range.set_start_key(prefix);

  std::string end_key = Helper::PrefixNext(prefix);  // end_key = prefix + 1
  range.set_end_key(end_key);

  DINGO_LOG(INFO) << "DeletePrefix meta data, start_key: " << Helper::StringToHex(range.start_key())
                  << " end_key: " << Helper::StringToHex(range.end_key());

  auto status = writer->KvDeleteRange(range);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta delete_range failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

}  // namespace dingodb
