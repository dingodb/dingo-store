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

#ifndef DINGODB_VECTOR_INDEX_DISKANN_H_  // NOLINT
#define DINGODB_VECTOR_INDEX_DISKANN_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "brpc/channel.h"
#include "butil/status.h"
#include "common/synchronization.h"
#include "proto/common.pb.h"
#include "proto/diskann.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

#ifndef TEST_VECTOR_INDEX_DISKANN_MOCK
#define TEST_VECTOR_INDEX_DISKANN_MOCK
#endif

#undef TEST_VECTOR_INDEX_DISKANN_MOCK

class VectorIndexDiskANN : public VectorIndex {
 public:
  explicit VectorIndexDiskANN(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                              const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                              ThreadPoolPtr thread_pool);

  ~VectorIndexDiskANN() override;

  VectorIndexDiskANN(const VectorIndexDiskANN& rhs) = delete;
  VectorIndexDiskANN& operator=(const VectorIndexDiskANN& rhs) = delete;
  VectorIndexDiskANN(VectorIndexDiskANN&& rhs) = delete;
  VectorIndexDiskANN& operator=(VectorIndexDiskANN&& rhs) = delete;

  static void Init();

  butil::Status Save(const std::string& path) override;
  butil::Status Load(const std::string& path) override;

  // in DiskANN index, add two vector with same id will cause data conflict
  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

  butil::Status AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_upsert);

  butil::Status AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_upsert);

  // not exist add. if exist update
  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

  butil::Status Delete(const std::vector<int64_t>& delete_ids) override;

  butil::Status Search(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                       const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool reconstruct,
                       const pb::common::VectorSearchParameter& parameter,
                       std::vector<pb::index::VectorWithDistanceResult>& results) override;

  butil::Status RangeSearch(const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
                            const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters, bool reconstruct,
                            const pb::common::VectorSearchParameter& parameter,
                            std::vector<pb::index::VectorWithDistanceResult>& results) override;

  void LockWrite() override;
  void UnlockWrite() override;
  bool SupportSave() override;

  butil::Status Build(const pb::common::Range& region_range, mvcc::ReaderPtr reader,
                      const pb::common::VectorBuildParameter& parameter, int64_t ts,
                      pb::common::VectorStateParameter& vector_state_parameter) override;

  butil::Status Load(const pb::common::VectorLoadParameter& parameter,
                     pb::common::VectorStateParameter& vector_state_parameter) override;

  butil::Status Status(pb::common::VectorStateParameter& vector_state_parameter) override;

  butil::Status Reset(bool delete_data_file, pb::common::VectorStateParameter& vector_state_parameter) override;

  butil::Status Drop() override;

  butil::Status Dump(bool dump_all, std::vector<std::string>& dump_datas) override;

  int32_t GetDimension() override;
  pb::common::MetricType GetMetricType() override;
  butil::Status GetCount(int64_t& count) override;
  butil::Status GetDeletedCount(int64_t& deleted_count) override;
  butil::Status GetMemorySize(int64_t& memory_size) override;
  bool IsExceedsMaxElements() override;
  butil::Status Train([[maybe_unused]] std::vector<float>& train_datas) override { return butil::Status::OK(); }
  butil::Status Train([[maybe_unused]] const std::vector<pb::common::VectorWithId>& vectors) override {
    return butil::Status::OK();
  }

  bool NeedToRebuild() override { return false; }

  bool NeedToSave(int64_t last_save_log_behind) override;

 private:
  static void InitDiskannServerAddr();
  static void InitWorkSet();
  butil::Status DoBuild(const pb::common::Range& region_range, mvcc::ReaderPtr reader,
                        const pb::common::VectorBuildParameter& parameter, int64_t ts,
                        pb::common::DiskANNCoreState& state);
  butil::Status DoDump(std::vector<std::string>& dump_datas);
  butil::Status DoDumpAll(std::vector<std::string>& dump_datas);
  bool InitChannel(const std::string& addr);
  butil::Status SendRequest(const std::string& service_name, const std::string& api_name,
                            const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorNewRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorNewRequestWrapper();
  butil::Status SendVectorStatusRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorStatusRequestWrapper(pb::common::DiskANNCoreState& state, pb::error::Error& last_error);
  butil::Status SendVectorCountRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorCountRequestWrapper(int64_t& count);
  butil::Status SendVectorBuildRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorBuildRequestWrapper(const pb::common::VectorBuildParameter& parameter,
                                              pb::common::DiskANNCoreState& state);
  butil::Status SendVectorLoadRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorLoadRequestWrapper(const pb::common::VectorLoadParameter& parameter,
                                             pb::common::DiskANNCoreState& state);
  butil::Status SendVectorTryLoadRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorTryLoadRequestWrapper(const pb::common::VectorLoadParameter& parameter,
                                                pb::common::DiskANNCoreState& state);
  butil::Status SendVectorPushDataRequest(const google::protobuf::Message& request,
                                          google::protobuf::Message& response);
  butil::Status SendVectorPushDataRequestWrapper(const pb::diskann::VectorPushDataRequest& vector_push_data_request);

  butil::Status SendVectorSearchRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorSearchRequestWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                               uint32_t topk, const pb::common::VectorSearchParameter& parameter,
                                               std::vector<pb::index::VectorWithDistanceResult>& results);
  butil::Status SendVectorResetRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorResetRequestWrapper(bool delete_data_file, pb::common::DiskANNCoreState& state);
  butil::Status SendVectorDestroyRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorDestroyRequestWrapper();
  butil::Status SendVectorDumpRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorDumpRequestWrapper(std::vector<std::string>& dump_datas);
  butil::Status SendVectorDumpAllRequest(const google::protobuf::Message& request, google::protobuf::Message& response);
  butil::Status SendVectorDumpAllRequestWrapper(std::vector<std::string>& dump_datas);

  faiss::idx_t dimension_;

  pb::common::MetricType metric_type_;

  std::unique_ptr<brpc::Channel> channel_;
  std::unique_ptr<brpc::ChannelOptions> options_;
  bool is_channel_init_;
  bool is_connected_;

  RWLock rw_lock_;

  static inline std::string diskann_server_addr;
  static inline WorkerSetPtr diskann_server_build_worker_set;
  static inline WorkerSetPtr diskann_server_load_worker_set;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_DISKANN_H_  // NOLINT
