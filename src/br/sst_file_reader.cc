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

#include "br/sst_file_reader.h"

#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"

namespace br {

SstFileReader::SstFileReader() {
  options_.env = rocksdb::Env::Default();
  if (options_.env != nullptr) {
    sst_reader_ = std::make_unique<rocksdb::SstFileReader>(options_);
  }
}

butil::Status SstFileReader::ReadFile(const std::string& filename, std::map<std::string, std::string>& kvs) {
  if (options_.env == nullptr || sst_reader_ == nullptr) {
    return butil::Status(dingodb::pb::error::EINTERNAL, "init sst options env error.");
  }

  auto status = sst_reader_->Open(filename);
  if (BAIDU_UNLIKELY(!status.ok())) {
    std::string s = fmt::format("open {} failed, error: {}.", filename, status.ToString());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  kvs.clear();

  std::unique_ptr<rocksdb::Iterator> iter(sst_reader_->NewIterator(rocksdb::ReadOptions()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string key(iter->key().data(), iter->key().size());
    std::string value(iter->value().data(), iter->value().size());
    kvs.emplace(std::move(key), std::move(value));
  }

  return butil::Status();
}

}  // namespace br