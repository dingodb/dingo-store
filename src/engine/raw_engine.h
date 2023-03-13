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

#ifndef DINGODB_ENGINE_KV_ENGINE_H_
#define DINGODB_ENGINE_KV_ENGINE_H_

#include <memory>
#include <string>
#include <vector>

#include "common/context.h"
#include "config/config.h"
#include "engine/snapshot.h"
#include "engine/write_data.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

enum class EnumEngineIterator {
  kRocksIterator = 0,
  kMemoryIterator = 1,
  kXdpIterator = 2,
  kRaftStoreIterator = 3,
  kColumnarIterator = 4,
};

enum class EnumEngineReader {
  kRocksReader = 0,
  kMemoryReader = 1,
  kXdpReader = 2,
  kRaftStoreReader = 3,
  kColumnarReader = 4,
};

class EngineIterator : public std::enable_shared_from_this<EngineIterator> {
 public:
  EngineIterator() = default;
  virtual ~EngineIterator() = default;
  std::shared_ptr<EngineIterator> GetSelf() { return shared_from_this(); }

  virtual bool HasNext() = 0;
  virtual void Next() = 0;
  virtual void GetKV(std::string& key, std::string& value) = 0;  // NOLINT
  virtual const std::string& GetName() const = 0;
  virtual uint32_t GetID() = 0;
};

class RawEngine {
 public:
  virtual ~RawEngine() = default;

  class Reader {
   public:
    Reader() = default;
    virtual ~Reader() = default;
    virtual butil::Status KvGet(const std::string& key, std::string& value) = 0;
    virtual butil::Status KvGet(std::shared_ptr<dingodb::Snapshot> snapshot, const std::string& key,
                                std::string& value) = 0;

    virtual butil::Status KvScan(const std::string& start_key, const std::string& end_key,
                                 std::vector<pb::common::KeyValue>& kvs) = 0;
    virtual butil::Status KvScan(std::shared_ptr<dingodb::Snapshot> snapshot, const std::string& start_key,
                                 const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) = 0;

    virtual butil::Status KvCount(const std::string& start_key, const std::string& end_key, int64_t& count) = 0;
    virtual butil::Status KvCount(std::shared_ptr<dingodb::Snapshot> snapshot, const std::string& start_key,
                                  const std::string& end_key, int64_t& count) = 0;
  };

  class Writer {
   public:
    Writer() = default;
    virtual ~Writer() = default;
    virtual butil::Status KvPut(const pb::common::KeyValue& kv) = 0;
    virtual butil::Status KvBatchPut(const std::vector<pb::common::KeyValue>& kvs) = 0;

    virtual butil::Status KvPutIfAbsent(const pb::common::KeyValue& kv) = 0;
    virtual butil::Status KvBatchPutIfAbsent(const std::vector<pb::common::KeyValue>& kvs,
                                             std::vector<std::string>& put_keys, bool is_atomic) = 0;

    virtual butil::Status KvCompareAndSet(const pb::common::KeyValue& kv, const std::string& value) = 0;

    virtual butil::Status KvDelete(const std::string& key) = 0;

    virtual butil::Status KvDeleteRange(const pb::common::Range& range) = 0;
  };

  virtual bool Init(std::shared_ptr<Config> config) = 0;
  virtual bool Recover() { return true; }

  virtual std::string GetName() = 0;
  virtual pb::common::RawEngine GetID() = 0;

  virtual std::shared_ptr<Snapshot> GetSnapshot() = 0;
  virtual void ReleaseSnapshot(std::shared_ptr<Snapshot>) = 0;

  virtual void Flush(const std::string& cf_name) = 0;

  virtual std::shared_ptr<Reader> NewReader(const std::string& cf_name) = 0;
  virtual std::shared_ptr<RawEngine::Writer> NewWriter(const std::string& cf_name) = 0;

 protected:
  RawEngine() = default;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_KV_ENGINE_H_