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

#ifndef DINGODB_ENGINE_KV_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_KV_ENGINE_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "config/config.h"
#include "engine/iterator.h"
#include "engine/snapshot.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class RawEngine : public std::enable_shared_from_this<RawEngine> {
 public:
  virtual ~RawEngine() = default;

  class Reader {
   public:
    Reader() = default;
    virtual ~Reader() = default;

    virtual butil::Status KvGet(const std::string& cf_name, const std::string& key, std::string& value) = 0;
    virtual butil::Status KvGet(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                                const std::string& key, std::string& value) = 0;

    virtual butil::Status KvScan(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                                 std::vector<pb::common::KeyValue>& kvs) = 0;
    virtual butil::Status KvScan(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                                 const std::string& start_key, const std::string& end_key,
                                 std::vector<pb::common::KeyValue>& kvs) = 0;

    virtual butil::Status KvCount(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                                  int64_t& count) = 0;
    virtual butil::Status KvCount(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                                  const std::string& start_key, const std::string& end_key, int64_t& count) = 0;

    virtual std::shared_ptr<dingodb::Iterator> NewIterator(const std::string& cf_name, IteratorOptions options) = 0;
    virtual std::shared_ptr<dingodb::Iterator> NewIterator(const std::string& cf_name,
                                                           std::shared_ptr<Snapshot> snapshot,
                                                           IteratorOptions options) = 0;
  };
  using ReaderPtr = std::shared_ptr<Reader>;

  class Writer {
   public:
    Writer() = default;
    virtual ~Writer() = default;

    virtual butil::Status KvPut(const std::string& cf_name, const pb::common::KeyValue& kv) = 0;
    virtual butil::Status KvDelete(const std::string& cf_name, const std::string& key) = 0;

    virtual butil::Status KvBatchPutAndDelete(const std::string& cf_name,
                                              const std::vector<pb::common::KeyValue>& kvs_to_put,
                                              const std::vector<std::string>& keys_to_deletes) = 0;
    virtual butil::Status KvBatchPutAndDelete(
        const std::map<std::string, std::vector<pb::common::KeyValue>>& kv_put_with_cfs,
        const std::map<std::string, std::vector<std::string>>& kv_delete_with_cfs) = 0;

    virtual butil::Status KvDeleteRange(const std::string& cf_name, const pb::common::Range& range) = 0;
    virtual butil::Status KvBatchDeleteRange(
        const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs) = 0;

    butil::Status KvDeleteRange(const std::vector<std::string>& cf_names, const pb::common::Range& range) {
      std::map<std::string, std::vector<pb::common::Range>> delete_range_map;
      for (const auto& cf_name : cf_names) {
        delete_range_map[cf_name].push_back(range);
      }
      KvBatchDeleteRange(delete_range_map);
      return butil::Status::OK();
    }
  };

  using WriterPtr = std::shared_ptr<Writer>;

  class Checkpoint {
   public:
    Checkpoint() = default;
    virtual ~Checkpoint() = default;

    virtual butil::Status Create(const std::string& /*dirpath*/) {
      return butil::Status(pb::error::ENOT_SUPPORT, "Not support checkpoint.");
    }
    virtual butil::Status Create(const std::string& /*dirpath*/, const std::vector<std::string>& /*cf_names*/,
                                 std::vector<pb::store_internal::SstFileInfo>& /*sst_files*/) {
      return butil::Status(pb::error::ENOT_SUPPORT, "Not support checkpoint.");
    }
  };
  using CheckpointPtr = std::shared_ptr<Checkpoint>;

  virtual bool Init(std::shared_ptr<Config> config, const std::vector<std::string>& cf_names) = 0;
  virtual void Close() = 0;
  virtual void Destroy() = 0;
  virtual bool Recover() { return true; }

  virtual std::string GetName() = 0;
  virtual pb::common::RawEngine GetRawEngineType() = 0;

  virtual SnapshotPtr GetSnapshot() = 0;

  virtual ReaderPtr Reader() = 0;
  virtual WriterPtr Writer() = 0;

  virtual CheckpointPtr NewCheckpoint() = 0;

  virtual butil::Status MergeCheckpointFiles(const std::string& path, const pb::common::Range& range,
                                             const std::vector<std::string>& cf_names,
                                             std::vector<std::string>& merge_sst_paths) = 0;
  virtual butil::Status IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files) = 0;

  virtual std::vector<int64_t> GetApproximateSizes(const std::string& cf_name,
                                                   std::vector<pb::common::Range>& ranges) = 0;

  virtual void Flush(const std::string& cf_name) = 0;
  virtual butil::Status Compact(const std::string& cf_name) = 0;

 protected:
  RawEngine() = default;
};
using RawEnginePtr = std::shared_ptr<RawEngine>;

}  // namespace dingodb

#endif  // DINGODB_ENGINE_KV_ENGINE_H_  // NOLINT
