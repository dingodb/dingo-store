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
  auto status = engine_->Writer()->KvPut(Constant::kStoreMetaCF, *kv);
  if (status.error_code() == pb::error::Errno::EINTERNAL) {
    DINGO_LOG(FATAL) << "KvPut failed, errcode: " << status.error_code() << " " << status.error_str()
                     << ", key(hex): " << Helper::StringToHex(kv->key())
                     << ", value(hex): " << Helper::StringToHex(kv->value());
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta write failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::Put(const std::vector<pb::common::KeyValue> kvs) {
  DINGO_LOG(DEBUG) << "Put meta data, key nums: " << kvs.size();
  if (kvs.empty()) return true;
  auto status = engine_->Writer()->KvBatchPutAndDelete(Constant::kStoreMetaCF, kvs, {});
  if (status.error_code() == pb::error::Errno::EINTERNAL) {
    DINGO_LOG(FATAL) << "KvBatchPut failed, errcode: " << status.error_code() << " " << status.error_str();
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta batch write failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::PutAndDelete(std::vector<pb::common::KeyValue> kvs_to_put, std::vector<std::string> keys_to_delete) {
  DINGO_LOG(DEBUG) << "PutAndDelete meta data, key_put nums: " << kvs_to_put.size()
                   << " key_delete nums:" << keys_to_delete.size();
  auto status = engine_->Writer()->KvBatchPutAndDelete(Constant::kStoreMetaCF, kvs_to_put, keys_to_delete);
  if (status.error_code() == pb::error::Errno::EINTERNAL) {
    DINGO_LOG(FATAL) << "KvBatchPutAndDelete failed, errcode: " << status.error_code() << " " << status.error_str()
                     << ", put_count: " << kvs_to_put.size() << ", delete_count: " << keys_to_delete.size()
                     << ", delete_count: " << keys_to_delete.size();
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta batch write and delete failed, errcode: " << status.error_code() << " "
                     << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::Delete(const std::string& key) {
  DINGO_LOG(DEBUG) << "Delete meta data, key: " << key;
  auto status = engine_->Writer()->KvDelete(Constant::kStoreMetaCF, key);
  if (status.error_code() == pb::error::Errno::EINTERNAL) {
    DINGO_LOG(FATAL) << "KvDelete failed, errcode: " << status.error_code() << " " << status.error_str()
                     << ", key(hex): " << Helper::StringToHex(key);
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta delete failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::DeleteRange(const std::string& start_key, const std::string& end_key) {
  DINGO_LOG(DEBUG) << "DeleteRange meta data, start_key: " << start_key << " end_key: " << end_key;
  pb::common::Range range;
  range.set_start_key(start_key);
  range.set_end_key(end_key);

  auto status = engine_->Writer()->KvDeleteRange(Constant::kStoreMetaCF, range);
  if (status.error_code() == pb::error::Errno::EINTERNAL) {
    DINGO_LOG(FATAL) << "KvDeleteRange failed, errcode: " << status.error_code() << " " << status.error_str()
                     << ", start_key(hex): " << Helper::StringToHex(start_key)
                     << ", end_key(hex): " << Helper::StringToHex(end_key);
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta delete_range failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

bool MetaWriter::DeletePrefix(const std::string& prefix) {
  DINGO_LOG(DEBUG) << "DeletePrefix meta data, prefix: " << prefix;
  pb::common::Range range;
  range.set_start_key(prefix);

  std::string end_key = Helper::PrefixNext(prefix);  // end_key = prefix + 1
  range.set_end_key(end_key);

  DINGO_LOG(INFO) << "DeletePrefix meta data, start_key: " << Helper::StringToHex(range.start_key())
                  << " end_key: " << Helper::StringToHex(range.end_key());

  auto status = engine_->Writer()->KvDeleteRange(Constant::kStoreMetaCF, range);
  if (status.error_code() == pb::error::Errno::EINTERNAL) {
    DINGO_LOG(FATAL) << "KvDeleteRange failed, errcode: " << status.error_code() << " " << status.error_str()
                     << ", start_key(hex): " << Helper::StringToHex(range.start_key())
                     << ", end_key(hex): " << Helper::StringToHex(range.end_key());
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Meta delete_range failed, errcode: " << status.error_code() << " " << status.error_str();
    return false;
  }

  return true;
}

}  // namespace dingodb
