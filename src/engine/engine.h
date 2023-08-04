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
#include "common/logging.h"
#include "config/config.h"
#include "engine/raw_engine.h"
#include "engine/snapshot.h"
#include "engine/write_data.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
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

  virtual butil::Status Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) = 0;
  virtual butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) = 0;
  virtual butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
                                   WriteCbFunc cb) = 0;

  // KV reader
  class Reader {
   public:
    Reader() = default;
    virtual ~Reader() = default;

    virtual butil::Status KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) = 0;

    virtual butil::Status KvScan(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                                 std::vector<pb::common::KeyValue>& kvs) = 0;

    virtual butil::Status KvCount(std::shared_ptr<Context> ctx, const std::string& start_key,
                                  const std::string& end_key, uint64_t& count) = 0;
  };

  // Vector reader
  class VectorReader {
   public:
    VectorReader() = default;
    virtual ~VectorReader() = default;

    struct Context {
      Context()
          : partition_id(0),
            region_id(0),
            start_id(0),
            limit(0),
            with_vector_data(false),
            with_scalar_data(false),
            with_table_data(false),
            is_reverse(false),
            use_scalar_filter(false) {}
      uint64_t partition_id;
      uint64_t region_id;

      pb::common::Range region_range;

      std::vector<pb::common::VectorWithId> vector_with_ids;
      std::vector<uint64_t> vector_ids;
      pb::common::VectorSearchParameter parameter;
      std::vector<std::string> selected_scalar_keys;
      pb::common::VectorScalardata scalar_data_for_filter;

      uint64_t start_id;
      uint64_t limit;

      bool with_vector_data;
      bool with_scalar_data;
      bool with_table_data;
      bool is_reverse;
      bool use_scalar_filter;
    };

    virtual butil::Status VectorBatchSearch(std::shared_ptr<VectorReader::Context> ctx,
                                            std::vector<pb::index::VectorWithDistanceResult>& results) = 0;

    virtual butil::Status VectorBatchQuery(std::shared_ptr<VectorReader::Context> ctx,
                                           std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;

    virtual butil::Status VectorGetBorderId(const pb::common::Range& region_range, bool get_min,
                                            uint64_t& vector_id) = 0;
    virtual butil::Status VectorScanQuery(std::shared_ptr<VectorReader::Context> ctx,
                                          std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;
    virtual butil::Status VectorGetRegionMetrics(uint64_t region_id, const pb::common::Range& region_range,
                                                 pb::common::VectorIndexMetrics& region_metrics) = 0;
  };

  virtual std::shared_ptr<Reader> NewReader(const std::string& cf_name) = 0;
  virtual std::shared_ptr<VectorReader> NewVectorReader(const std::string&) {
    DINGO_LOG(ERROR) << "Not support NewVectorReader.";
    return nullptr;
  }

  //  This is used by RaftStoreEngine to Persist Meta
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
