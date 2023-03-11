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

#ifndef DINGODB_ENGINE_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_ENGINE_H_

#include <memory>
#include <string>
#include <vector>

#include "common/context.h"
#include "config/config.h"
#include "engine/snapshot.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

const std::string kStoreDataCF = "default";
const std::string kStoreMetaCF = "meta";

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

 protected:
 private:
};

class EngineReader : public std::enable_shared_from_this<EngineReader> {
 public:
  EngineReader() = default;
  virtual ~EngineReader() = default;
  std::shared_ptr<EngineReader> GetSelf() { return shared_from_this(); }
  virtual std::shared_ptr<EngineIterator> Scan(const std::string& begin_key, const std::string& end_key) = 0;
  virtual std::shared_ptr<std::string> KvGet(const std::string& key) = 0;
  virtual const std::string& GetName() const = 0;
  virtual uint32_t GetID() = 0;

 protected:
 private:
};

class Engine {
 public:
  virtual ~Engine() = default;

  virtual bool Init(std::shared_ptr<Config> config) = 0;
  virtual bool Recover() { return true; }

  virtual std::string GetName() = 0;
  virtual pb::common::Engine GetID() = 0;

  virtual Snapshot* GetSnapshot() { return nullptr; }
  virtual void ReleaseSnapshot() {}

  virtual pb::error::Errno KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) = 0;
  virtual pb::error::Errno KvBatchGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                                      std::vector<pb::common::KeyValue>& kvs) = 0;

  virtual pb::error::Errno KvPut(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) = 0;
  virtual pb::error::Errno KvAsyncPut(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) {
    return pb::error::OK;
  }

  virtual pb::error::Errno KvBatchPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs) = 0;
  virtual pb::error::Errno KvPutIfAbsent(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) = 0;

  virtual pb::error::Errno KvBatchPutIfAbsentAtomic(std::shared_ptr<Context> ctx,
                                                    const std::vector<pb::common::KeyValue>& kvs,
                                                    std::vector<std::string>& put_keys) {
    return pb::error::Errno::ENOT_SUPPORT;
  }

  virtual pb::error::Errno KvBatchPutIfAbsentNonAtomic(std::shared_ptr<Context> ctx,
                                                       const std::vector<pb::common::KeyValue>& kvs,
                                                       std::vector<std::string>& put_keys) {
    return pb::error::Errno::ENOT_SUPPORT;
  }

  // compare and replace. support does not exist
  virtual pb::error::Errno KvCompareAndSet(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv,
                                           const std::string& value) {
    return pb::error::Errno::ENOT_SUPPORT;
  }

  virtual pb::error::Errno KvDelete(std::shared_ptr<Context> ctx, const std::string& key) {
    return pb::error::Errno::ENOT_SUPPORT;
  }

  virtual pb::error::Errno KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) {
    return pb::error::Errno::ENOT_SUPPORT;
  }

  virtual std::shared_ptr<EngineReader> CreateReader(std::shared_ptr<Context> ctx) { return nullptr; }

  // [begin_key, end_key)
  virtual pb::error::Errno KvScan(std::shared_ptr<Context> ctx, const std::string& begin_key,
                                  const std::string& end_key,
                                  std::vector<pb::common::KeyValue>& kvs) {  // NOLINT
    return pb::error::Errno::ENOT_SUPPORT;
  }
  // [begin_key, end_key)
  virtual pb::error::Errno KvCount(std::shared_ptr<Context> ctx, const std::string& begin_key,
                                   const std::string& end_key, int64_t& count) {  // NOLINT
    return pb::error::Errno::ENOT_SUPPORT;
  }

  /**
   * This is used by RaftKvEngine to Persist Meta
   * This is a alternative method, will be replace by zihui new Interface.
   */
  virtual pb::error::Errno MetaPut(std::shared_ptr<Context> ctx, const pb::coordinator_internal::MetaIncrement& meta) {
    return pb::error::Errno::ENOT_SUPPORT;
  }

 protected:
  Engine() = default;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ENGINE_H_  // NOLINT
