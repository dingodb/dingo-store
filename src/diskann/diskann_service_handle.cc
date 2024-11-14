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

#include "diskann/diskann_service_handle.h"

#include "butil/status.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/diskann.pb.h"
#include "proto/error.pb.h"
#include "server/service_helper.h"

namespace dingodb {

butil::Status DiskAnnServiceHandle::VectorNew(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                              const pb::common::VectorIndexParameter& vector_index_parameter) {
  butil::Status status;
  auto item = item_manager.Create(ctx, vector_index_id, vector_index_parameter);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} already exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_EXISTS, s);
    return status;
  }

  return butil::Status::OK();
}

butil::Status DiskAnnServiceHandle::VectorPushData(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                                   const std::vector<pb::common::Vector>& vectors,
                                                   const std::vector<int64_t>& vector_ids, bool has_more,
                                                   pb::error::Errno /*error*/, bool force_to_load_data_if_exist,
                                                   int64_t ts, int64_t tso, int64_t already_send_vector_count) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  int64_t already_recv_vector_count = 0;
  status = item->Import(ctx, vectors, vector_ids, has_more, force_to_load_data_if_exist, already_send_vector_count, ts,
                        tso, already_recv_vector_count);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  ::dingodb::pb::diskann::VectorPushDataResponse& response =
      (dynamic_cast<pb::diskann::VectorPushDataResponse&>(*ctx->Response()));

  response.set_already_send_vector_count(already_recv_vector_count);

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorBuild(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                                bool force_to_build_if_exist) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  status = item->Build(ctx, force_to_build_if_exist, true);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  ::dingodb::pb::diskann::VectorBuildResponse& response =
      (dynamic_cast<pb::diskann::VectorBuildResponse&>(*ctx->Response()));

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorLoad(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                               const pb::common::LoadDiskAnnParam& load_param) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  status = item->Load(ctx, load_param, true);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  ::dingodb::pb::diskann::VectorLoadResponse& response =
      (dynamic_cast<pb::diskann::VectorLoadResponse&>(*ctx->Response()));

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorTryLoad(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                                  const pb::common::LoadDiskAnnParam& load_param) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  status = item->TryLoad(ctx, load_param, true);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  ::dingodb::pb::diskann::VectorTryLoadResponse& response =
      (dynamic_cast<pb::diskann::VectorTryLoadResponse&>(*ctx->Response()));

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorSearch(std::shared_ptr<Context> ctx, int64_t vector_index_id, uint32_t top_n,
                                                 const pb::common::SearchDiskAnnParam& search_param,
                                                 const std::vector<pb::common::Vector>& vectors) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  std::vector<pb::index::VectorWithDistanceResult> results;
  int64_t ts = 0;
  status = item->Search(ctx, top_n, search_param, vectors, results, ts);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  ::dingodb::pb::diskann::VectorSearchResponse& response =
      (dynamic_cast<pb::diskann::VectorSearchResponse&>(*ctx->Response()));

  if (status.ok()) {
    for (auto& result : results) {
      response.add_batch_results()->CopyFrom(result);
    }

    response.set_ts(ts);
  }

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorReset(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                                bool delete_data_file) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  if (delete_data_file) {
    status = item->Destroy(ctx);
  } else {
    status = item->Close(ctx);
  }
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  ::dingodb::pb::diskann::VectorResetResponse& response =
      (dynamic_cast<pb::diskann::VectorResetResponse&>(*ctx->Response()));

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorClose(std::shared_ptr<Context> ctx, int64_t vector_index_id) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  status = item->Close(ctx);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  ::dingodb::pb::diskann::VectorCloseResponse& response =
      (dynamic_cast<pb::diskann::VectorCloseResponse&>(*ctx->Response()));

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorDestroy(std::shared_ptr<Context> ctx, int64_t vector_index_id) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  status = item->Destroy(ctx);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  ::dingodb::pb::diskann::VectorDestroyResponse& response =
      (dynamic_cast<pb::diskann::VectorDestroyResponse&>(*ctx->Response()));

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  if (status.ok()) {
    item_manager.Delete(vector_index_id);
  }

  return status;
}

butil::Status DiskAnnServiceHandle::VectorStatus(std::shared_ptr<Context> ctx, int64_t vector_index_id) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  DiskANNCoreState state;
  state = item->Status(ctx);

  ::dingodb::pb::diskann::VectorStatusResponse& response =
      (dynamic_cast<pb::diskann::VectorStatusResponse&>(*ctx->Response()));

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(state));

  return butil::Status::OK();
}

butil::Status DiskAnnServiceHandle::VectorCount(std::shared_ptr<Context> ctx, int64_t vector_index_id) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  int64_t count = 0;
  status = item->Count(ctx, count);

  pb::diskann::VectorCountResponse& response = (dynamic_cast<pb::diskann::VectorCountResponse&>(*ctx->Response()));

  response.set_count(count);

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorSetNoData(std::shared_ptr<Context> ctx, int64_t vector_index_id) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  status = item->SetNoData(ctx);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  pb::diskann::VectorSetNoDataResponse& response =
      (dynamic_cast<pb::diskann::VectorSetNoDataResponse&>(*ctx->Response()));

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorSetImportTooMany(std::shared_ptr<Context> ctx, int64_t vector_index_id) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  status = item->SetImportTooMany(ctx);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  pb::diskann::VectorSetImportTooManyResponse& response =
      (dynamic_cast<pb::diskann::VectorSetImportTooManyResponse&>(*ctx->Response()));

  ServiceHelper::SetError(response.mutable_last_error(), ctx->Status().error_code(), ctx->Status().error_str());
  response.set_state(DiskANNUtils::DiskANNCoreStateToPb(ctx->DiskANNCoreStateX()));

  return status;
}

butil::Status DiskAnnServiceHandle::VectorDump(std::shared_ptr<Context> ctx, int64_t vector_index_id) {
  butil::Status status;
  auto item = item_manager.Find(vector_index_id);
  if (item == nullptr) {
    std::string s = fmt::format("vector_index_id : {} not  exists", vector_index_id);
    DINGO_LOG(ERROR) << s;
    status = butil::Status(pb::error::EINDEX_NOT_FOUND, s);
    return status;
  }

  std::string dump_data;
  dump_data = item->Dump(ctx);

  pb::diskann::VectorDumpResponse& response = (dynamic_cast<pb::diskann::VectorDumpResponse&>(*ctx->Response()));
  response.set_dump_data(dump_data);

  return butil::Status::OK();
}

butil::Status DiskAnnServiceHandle::VectorDumpAll(std::shared_ptr<Context> ctx) {
  butil::Status status;
  std::vector<std::shared_ptr<DiskANNItem>> items = item_manager.FindAll();

  pb::diskann::VectorDumpAllResponse& response = (dynamic_cast<pb::diskann::VectorDumpAllResponse&>(*ctx->Response()));
  for (auto& item : items) {
    std::string dump_data = item->Dump(ctx);
    response.add_dump_datas(std::move(dump_data));
  }

  return butil::Status::OK();
}

}  // namespace dingodb
