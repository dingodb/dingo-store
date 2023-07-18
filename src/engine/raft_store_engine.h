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

#ifndef DINGODB_ENGINE_RAFT_KV_ENGINE_H_
#define DINGODB_ENGINE_RAFT_KV_ENGINE_H_

#include <memory>

#include "butil/status.h"
#include "common/meta_control.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "event/event.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_metrics_manager.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/store.pb.h"
#include "proto/store_internal.pb.h"
#include "raft/raft_node_manager.h"

namespace dingodb {

class RaftControlAble {
 public:
  virtual ~RaftControlAble() = default;

  virtual butil::Status AddNode(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                std::shared_ptr<pb::store_internal::RaftMeta> raft_meta,
                                store::RegionMetricsPtr region_metrics,
                                std::shared_ptr<EventListenerCollection> listeners, bool is_restart) = 0;
  virtual butil::Status AddNode(std::shared_ptr<pb::common::RegionDefinition> region,
                                std::shared_ptr<MetaControl> meta_control, bool is_volatile) = 0;
  virtual butil::Status StopNode(std::shared_ptr<Context> ctx, uint64_t region_id) = 0;
  virtual butil::Status DestroyNode(std::shared_ptr<Context> ctx, uint64_t region_id) = 0;
  virtual butil::Status ChangeNode(std::shared_ptr<Context> ctx, uint64_t region_id,
                                   std::vector<pb::common::Peer> peers) = 0;
  virtual std::shared_ptr<RaftNode> GetNode(uint64_t region_id) = 0;

  virtual butil::Status TransferLeader(uint64_t region_id, const pb::common::Peer& peer) = 0;

 protected:
  RaftControlAble() = default;
};

class RaftStoreEngine : public Engine, public RaftControlAble {
 public:
  RaftStoreEngine(std::shared_ptr<RawEngine> engine);
  ~RaftStoreEngine() override;

  bool Init(std::shared_ptr<Config> config) override;
  bool Recover() override;

  std::string GetName() override;
  pb::common::Engine GetID() override;

  std::shared_ptr<RawEngine> GetRawEngine() override;

  butil::Status AddNode(std::shared_ptr<Context> ctx, store::RegionPtr region,
                        std::shared_ptr<pb::store_internal::RaftMeta> raft_meta, store::RegionMetricsPtr region_metrics,
                        std::shared_ptr<EventListenerCollection> listeners, bool is_restart) override;
  butil::Status AddNode(std::shared_ptr<pb::common::RegionDefinition> region, std::shared_ptr<MetaControl> meta_control,
                        bool is_volatile) override;
  butil::Status ChangeNode(std::shared_ptr<Context> ctx, uint64_t region_id,
                           std::vector<pb::common::Peer> peers) override;
  butil::Status StopNode(std::shared_ptr<Context> ctx, uint64_t region_id) override;
  butil::Status DestroyNode(std::shared_ptr<Context> ctx, uint64_t region_id) override;
  std::shared_ptr<RaftNode> GetNode(uint64_t region_id) override;

  butil::Status TransferLeader(uint64_t region_id, const pb::common::Peer& peer) override;

  std::shared_ptr<Snapshot> GetSnapshot() override { return nullptr; }
  butil::Status DoSnapshot(std::shared_ptr<Context> ctx, uint64_t region_id) override;

  butil::Status Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) override;
  butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) override;
  butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
                           WriteCbFunc cb) override;

  // KV reader
  class Reader : public Engine::Reader {
   public:
    Reader(std::shared_ptr<RawEngine::Reader> reader) : reader_(reader) {}
    butil::Status KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) override;

    butil::Status KvScan(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                         std::vector<pb::common::KeyValue>& kvs) override;

    butil::Status KvCount(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                          uint64_t& count) override;

   private:
    std::shared_ptr<RawEngine::Reader> reader_;
  };

  std::shared_ptr<Engine::Reader> NewReader(const std::string& cf_name) override;

  // Vector reader
  class VectorReader : public Engine::VectorReader {
   public:
    VectorReader(std::shared_ptr<RawEngine::Reader> reader) : reader_(reader) {}

    butil::Status VectorBatchSearch(std::shared_ptr<Context> ctx,
                                    const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                    pb::common::VectorSearchParameter parameter,
                                    std::vector<pb::index::VectorWithDistanceResult>& results) override;

    butil::Status VectorBatchQuery(std::shared_ptr<Context> ctx, std::vector<uint64_t> vector_ids,
                                   bool with_vector_data, bool with_scalar_data,
                                   std::vector<std::string> selected_scalar_keys,
                                   std::vector<pb::common::VectorWithId>& vector_with_ids) override;

    butil::Status VectorGetBorderId(std::shared_ptr<Context> ctx, uint64_t& id, bool get_min) override;

   private:
    butil::Status QueryVectorWithId(uint64_t region_id, uint64_t vector_id, pb::common::VectorWithId& vector_with_id,
                                    bool with_vector_data = true);
    butil::Status SearchVector(uint64_t region_id, const std::vector<pb::common::VectorWithId>& vector_with_ids,
                               const pb::common::VectorSearchParameter& parameter,
                               std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

    butil::Status QueryVectorScalarData(uint64_t region_id, std::vector<std::string> selected_scalar_keys,
                                        pb::common::VectorWithId& vector_with_id);
    butil::Status QueryVectorScalarData(uint64_t region_id, std::vector<std::string> selected_scalar_keys,
                                        std::vector<pb::common::VectorWithDistance>& vector_with_distances);
    butil::Status QueryVectorScalarData(uint64_t region_id, std::vector<std::string> selected_scalar_keys,
                                        std::vector<pb::index::VectorWithDistanceResult>& results);

    butil::Status CompareVectorScalarData(uint64_t region_id, uint64_t vector_id,
                                          const pb::common::VectorScalardata& source_scalar_data, bool& compare_result);

    std::shared_ptr<RawEngine::Reader> reader_;
  };

  std::shared_ptr<Engine::VectorReader> NewVectorReader(const std::string& cf_name) override;

 protected:
  std::shared_ptr<RawEngine> engine_;                   // NOLINT
  std::unique_ptr<RaftNodeManager> raft_node_manager_;  // NOLINT
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_RAFT_KV_ENGINE_H_H  // NOLINT
