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

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
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
#include "proto/store.pb.h"
#include "serial/buf.h"
#include "vector/vector_index.h"

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
  virtual butil::Status DoSnapshot(std::shared_ptr<Context> ctx, int64_t region_id) = 0;

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
                                  const std::string& end_key, int64_t& count) = 0;
  };

  // Vector reader
  class VectorReader {
   public:
    VectorReader() = default;
    virtual ~VectorReader() = default;

    struct Context {
      int64_t partition_id{};
      int64_t region_id{};

      pb::common::Range region_range;

      std::vector<pb::common::VectorWithId> vector_with_ids;
      std::vector<int64_t> vector_ids;
      pb::common::VectorSearchParameter parameter;
      std::vector<std::string> selected_scalar_keys;
      pb::common::VectorScalardata scalar_data_for_filter;

      int64_t start_id{};
      int64_t end_id{};
      int64_t limit{};

      bool with_vector_data{};
      bool with_scalar_data{};
      bool with_table_data{};
      bool is_reverse{};
      bool use_scalar_filter{};

      VectorIndexWrapperPtr vector_index;
    };

    virtual butil::Status VectorBatchSearch(std::shared_ptr<VectorReader::Context> ctx,
                                            std::vector<pb::index::VectorWithDistanceResult>& results) = 0;

    virtual butil::Status VectorBatchQuery(std::shared_ptr<VectorReader::Context> ctx,
                                           std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;

    virtual butil::Status VectorGetBorderId(const pb::common::Range& region_range, bool get_min,
                                            int64_t& vector_id) = 0;
    virtual butil::Status VectorScanQuery(std::shared_ptr<VectorReader::Context> ctx,
                                          std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;
    virtual butil::Status VectorGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                                 VectorIndexWrapperPtr vector_index,
                                                 pb::common::VectorIndexMetrics& region_metrics) = 0;

    virtual butil::Status VectorCount(const pb::common::Range& range, int64_t& count) = 0;

    // This function is for testing only
    virtual butil::Status VectorBatchSearchDebug(std::shared_ptr<VectorReader::Context> ctx,
                                                 std::vector<pb::index::VectorWithDistanceResult>& results,
                                                 int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                                 int64_t& search_time_us) = 0;
  };

  class TxnReader {
   public:
    TxnReader() = default;
    virtual ~TxnReader() = default;

    virtual butil::Status TxnBatchGet(std::shared_ptr<Context> ctx, uint64_t start_ts,
                                      const std::vector<std::string>& keys, std::vector<pb::common::KeyValue>& kvs,
                                      pb::store::TxnResultInfo& txn_result_info) = 0;
    virtual butil::Status TxnScan(std::shared_ptr<Context> ctx, uint64_t start_ts, const pb::common::Range& range,
                                  uint64_t limit, bool key_only, bool is_reverse, bool disable_coprocessor,
                                  const pb::store::Coprocessor& coprocessor, pb::store::TxnResultInfo& txn_result_info,
                                  std::vector<pb::common::KeyValue>& kvs, bool& has_more, std::string& end_key) = 0;
    virtual butil::Status TxnScanLock(std::shared_ptr<Context> ctx, uint64_t min_lock_ts, uint64_t max_lock_ts,
                                      const pb::common::Range& range, uint64_t limit,
                                      std::vector<pb::store::LockInfo>& lock_infos) = 0;
  };

  virtual std::shared_ptr<Reader> NewReader(const std::string& cf_name) = 0;
  virtual std::shared_ptr<VectorReader> NewVectorReader(const std::string&) {
    DINGO_LOG(ERROR) << "Not support NewVectorReader.";
    return nullptr;
  }

  virtual std::shared_ptr<TxnReader> NewTxnReader() = 0;

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
