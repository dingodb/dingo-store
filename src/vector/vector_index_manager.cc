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

#include "vector/vector_index_manager.h"

#include <cstdint>
#include <memory>
#include <string>

#include "fmt/core.h"
#include "vector/codec.h"

namespace dingodb {

bool VectorIndexManager::Init(std::vector<store::RegionPtr> regions) {
  for (auto& region : regions) {
    // init vector index map
    const auto& definition = region->InnerRegion().definition();
    if (definition.index_parameter().index_type() == pb::common::IndexType::INDEX_TYPE_VECTOR) {
      DINGO_LOG(INFO) << fmt::format("Create region {} vector index", region->Id());

      auto status = LoadVectorIndex(region);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Load region {} vector index failed, ", region->Id());
        return false;
      }
    }
  }

  // Set apply log index
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    DINGO_LOG(ERROR) << "Scan vector index meta failed!";
    return false;
  }
  DINGO_LOG(INFO) << "Init vector index meta num: " << kvs.size();

  if (!kvs.empty()) {
    TransformFromKv(kvs);
  }

  return true;
}

bool VectorIndexManager::AddVectorIndex(uint64_t region_id, std::shared_ptr<VectorIndex> vector_index) {
  return vector_indexs_.Put(region_id, vector_index) > 0;
}

bool VectorIndexManager::AddVectorIndex(uint64_t region_id, const pb::common::IndexParameter& index_parameter) {
  auto vector_index = VectorIndex::New(region_id, index_parameter);
  if (vector_index == nullptr) {
    return false;
  }
  return AddVectorIndex(region_id, vector_index);
}

void VectorIndexManager::DeleteVectorIndex(uint64_t region_id) {
  vector_indexs_.Erase(region_id);
  meta_writer_->Delete(GenKey(region_id));
}

std::shared_ptr<VectorIndex> VectorIndexManager::GetVectorIndex(uint64_t region_id) {
  auto vector_index = vector_indexs_.Get(region_id);
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("Not found vector index {}", region_id);
  }

  return vector_index;
}

// Load vector index for already exist vector index at bootstrap.
butil::Status VectorIndexManager::LoadVectorIndex(store::RegionPtr region) {
  assert(region != nullptr);

  std::string start_key;
  std::string end_key;
  VectorCodec::EncodeVectorId(region->Id(), 0, start_key);
  VectorCodec::EncodeVectorId(region->Id(), UINT64_MAX, end_key);

  IteratorOptions options;
  options.lower_bound = start_key;
  options.upper_bound = end_key;
  auto iter = raw_engine_->NewIterator(Constant::kStoreDataCF, options);

  auto vector_index = VectorIndex::New(region->Id(), region->InnerRegion().definition().index_parameter());
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    pb::common::VectorWithId vector;

    std::string key(iter->Key());
    vector.set_id(VectorCodec::DecodeVectorId(key));

    std::string value(iter->Value());
    if (!vector.mutable_vector()->ParseFromString(value)) {
      DINGO_LOG(WARNING) << fmt::format("vector ParseFromString failed, id {}", vector.id());
      continue;
    }

    if (vector.vector().values_size() <= 0) {
      DINGO_LOG(WARNING) << fmt::format("vector values_size error, id {}", vector.id());
      continue;
    }

    vector_index->Add(vector);
  }

  vector_indexs_.Put(region->Id(), vector_index);

  return butil::Status::OK();
}

void VectorIndexManager::UpdateApplyLogIndex(std::shared_ptr<VectorIndex> vector_index, uint64_t apply_log_index) {
  assert(vector_index != nullptr);

  vector_index->SetApplyLogIndex(apply_log_index);
  meta_writer_->Put(TransformToKv(vector_index));
}

void VectorIndexManager::UpdateApplyLogIndex(uint64_t region_id, uint64_t apply_log_index) {
  auto vector_index = GetVectorIndex(region_id);
  if (vector_index != nullptr) {
    UpdateApplyLogIndex(vector_index, apply_log_index);
  }
}

std::shared_ptr<pb::common::KeyValue> VectorIndexManager::TransformToKv(std::any obj) {
  auto vector_index = std::any_cast<std::shared_ptr<VectorIndex>>(obj);
  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(vector_index->Id()));
  kv->set_value(
      VectorCodec::EncodeVecotrIndexLogIndex(vector_index->SnapshotLogIndex(), vector_index->ApplyLogIndex()));

  return kv;
}

void VectorIndexManager::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  for (const auto& kv : kvs) {
    uint64_t region_id = ParseRegionId(kv.key());
    uint64_t snapshot_log_index = VectorCodec::DecodeVectorSnapshotLogIndex(kv.value());
    uint64_t apply_log_index = VectorCodec::DecodeVectorApplyLogIndex(kv.value());

    auto vector_index = GetVectorIndex(region_id);
    if (vector_index != nullptr) {
      vector_index->SetSnapshotLogIndex(snapshot_log_index);
      vector_index->SetApplyLogIndex(apply_log_index);
    }
  }
}

}  // namespace dingodb