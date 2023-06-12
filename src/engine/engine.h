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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/context.h"
#include "config/config.h"
#include "engine/raw_engine.h"
#include "engine/snapshot.h"
#include "engine/write_data.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"

namespace dingodb {

class Engine {
  using Errno = pb::error::Errno;

 public:
  virtual ~Engine() = default;

  virtual bool Init(std::shared_ptr<Config> config) = 0;
  virtual bool Recover() { return true; }

  virtual std::string GetName() = 0;
  virtual pb::common::Engine GetID() = 0;

  virtual std::shared_ptr<RawEngine> GetRawEngine() { return nullptr; }

  virtual std::shared_ptr<Snapshot> GetSnapshot() = 0;
  virtual butil::Status DoSnapshot(std::shared_ptr<Context> ctx, uint64_t region_id) = 0;

  virtual butil::Status Write(std::shared_ptr<Context> ctx, const WriteData& write_data) = 0;
  virtual butil::Status AsyncWrite(std::shared_ptr<Context> ctx, const WriteData& write_data, WriteCbFunc cb) = 0;

  class Reader {
   public:
    Reader() = default;
    virtual ~Reader() = default;

    virtual butil::Status KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) = 0;

    virtual butil::Status KvScan(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                                 std::vector<pb::common::KeyValue>& kvs) = 0;

    virtual butil::Status KvCount(std::shared_ptr<Context> ctx, const std::string& start_key,
                                  const std::string& end_key, uint64_t& count) = 0;

    virtual butil::Status VectorSearch(std::shared_ptr<Context> ctx, const pb::common::VectorWithId& vector,
                                       pb::common::VectorSearchParameter parameter,
                                       std::vector<pb::common::VectorWithDistance>& vectors) = 0;
  };

  virtual std::shared_ptr<Reader> NewReader(const std::string& cf_name) = 0;

  //  This is used by RaftKvEngine to Persist Meta
  //  This is a alternative method, will be replace by zihui new Interface.
  virtual butil::Status MetaPut(std::shared_ptr<Context> /*ctx*/,
                                const pb::coordinator_internal::MetaIncrement& /*meta*/) {
    return butil::Status(Errno::ENOT_SUPPORT, "Not support");
  }

 protected:
  Engine() = default;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ENGINE_H_  // NOLINT
