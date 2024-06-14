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

#ifndef DINGODB_MVCC_READER_H_
#define DINGODB_MVCC_READER_H_

#include "engine/engine.h"

namespace dingodb {

namespace mvcc {

class Reader : public Engine::Reader {
 public:
  Reader(RawEngine::ReaderPtr reader) : reader_(reader) {}

  // key is user key, not encode key
  // output value is user value, not include ext field
  butil::Status KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) override;

  // start_key and end_key is user key
  // output kvs is user key
  butil::Status KvScan(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                       std::vector<pb::common::KeyValue>& kvs) override;

  // start_key and end_key is user key
  butil::Status KvCount(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                        int64_t& count) override;

  dingodb::IteratorPtr NewIterator(const std::string& cf_name, int64_t ts, IteratorOptions options) override;

 private:
  RawEngine::ReaderPtr reader_;
};

}  // namespace mvcc

}  // namespace dingodb

#endif  // DINGODB_MVCC_READER_H_