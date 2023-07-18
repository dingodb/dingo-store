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

#include "vector/vector_index_hnsw.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "bthread/mutex.h"
#include "bthread/types.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

DEFINE_uint64(hnsw_need_save_count, 10000, "hnsw need save count");

/*
 * replacement for the openmp '#pragma omp parallel for' directive
 * only handles a subset of functionality (no reductions etc)
 * Process ids from start (inclusive) to end (EXCLUSIVE)
 *
 * The method is borrowed from nmslib
 */
template <class Function>
inline void ParallelFor(size_t start, size_t end, size_t num_threads, Function fn) {
  if (num_threads <= 0) {
    num_threads = std::thread::hardware_concurrency();
  }

  if (num_threads == 1) {
    for (size_t id = start; id < end; id++) {
      fn(id, 0);
    }
  } else {
    std::vector<std::thread> threads;
    std::atomic<size_t> current(start);

    // keep track of exceptions in threads
    // https://stackoverflow.com/a/32428427/1713196
    std::exception_ptr last_exception = nullptr;
    std::mutex last_except_mutex;

    for (size_t thread_id = 0; thread_id < num_threads; ++thread_id) {
      threads.push_back(std::thread([&, thread_id] {
        while (true) {
          size_t id = current.fetch_add(1);

          if (id >= end) {
            break;
          }

          try {
            fn(id, thread_id);
          } catch (...) {
            std::unique_lock<std::mutex> last_excep_lock(last_except_mutex);
            last_exception = std::current_exception();
            /*
             * This will work even when current is the largest value that
             * size_t can fit, because fetch_add returns the previous value
             * before the increment (what will result in overflow
             * and produce 0 instead of current + 1).
             */
            current = end;
            break;
          }
        }
      }));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    if (last_exception) {
      std::rethrow_exception(last_exception);
    }
  }
}

VectorIndexHnsw::VectorIndexHnsw(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter)
    : VectorIndex(id, vector_index_parameter) {
  bthread_mutex_init(&mutex_, nullptr);
  is_online_.store(true);
  hnsw_num_threads_ = std::thread::hardware_concurrency();

  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    const auto& hnsw_parameter = vector_index_parameter.hnsw_parameter();
    assert(hnsw_parameter.dimension() > 0);
    assert(hnsw_parameter.metric_type() != pb::common::MetricType::METRIC_TYPE_NONE);
    assert(hnsw_parameter.efconstruction() > 0);
    assert(hnsw_parameter.max_elements() > 0);
    assert(hnsw_parameter.nlinks() > 0);

    this->dimension_ = hnsw_parameter.dimension();

    if (hnsw_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
      hnsw_space_ = new hnswlib::InnerProductSpace(hnsw_parameter.dimension());
    } else {
      hnsw_space_ = new hnswlib::L2Space(hnsw_parameter.dimension());
    }

    hnsw_index_ =
        new hnswlib::HierarchicalNSW<float>(hnsw_space_, hnsw_parameter.max_elements(), hnsw_parameter.nlinks(),
                                            hnsw_parameter.efconstruction(), 100, true);
  } else {
    hnsw_index_ = nullptr;
  }
}

VectorIndexHnsw::~VectorIndexHnsw() {
  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    delete hnsw_index_;
    delete hnsw_space_;
  }

  bthread_mutex_destroy(&mutex_);
}

butil::Status VectorIndexHnsw::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return Upsert(vector_with_ids);
}

butil::Status VectorIndexHnsw::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "upsert vector empty";
    return butil::Status::OK();
  }

  // check
  uint32_t input_dimension = vector_with_ids[0].vector().float_values_size();
  if (input_dimension != static_cast<size_t>(dimension_)) {
    std::string s =
        fmt::format("HNSW: float size : {} not equal to  dimension(create) : {}", input_dimension, dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
  }

  butil::Status ret;

  std::unique_ptr<float> data;
  try {
    data.reset(new float[this->dimension_ * vector_with_ids.size()]);
  } catch (std::bad_alloc& e) {
    DINGO_LOG(ERROR) << "upsert vector failed, error=" << e.what();
    ret = butil::Status(pb::error::Errno::EINTERNAL, "upsert vector failed, error=" + std::string(e.what()));
    return ret;
  }

  for (size_t row = 0; row < vector_with_ids.size(); ++row) {
    for (size_t col = 0; col < this->dimension_; ++col) {
      data.get()[row * this->dimension_ + col] = vector_with_ids[row].vector().float_values().at(col);
    }
  }

  BAIDU_SCOPED_LOCK(mutex_);

  // Add data to index
  try {
    ParallelFor(0, vector_with_ids.size(), hnsw_num_threads_, [&](size_t row, size_t /*thread_id*/) {
      this->hnsw_index_->addPoint((void*)(data.get() + dimension_ * row), vector_with_ids[row].id(), true);
    });
  } catch (std::runtime_error& e) {
    DINGO_LOG(ERROR) << "upsert vector failed, error=" << e.what();
    ret = butil::Status(pb::error::Errno::EINTERNAL, "upsert vector failed, error=" + std::string(e.what()));
  }

  return ret;
}

butil::Status VectorIndexHnsw::Delete(const std::vector<uint64_t>& delete_ids) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  if (delete_ids.empty()) {
    DINGO_LOG(WARNING) << "delete ids is empty";
    return butil::Status::OK();
  }

  butil::Status ret;

  BAIDU_SCOPED_LOCK(mutex_);

  // Add data to index
  try {
    ParallelFor(0, delete_ids.size(), hnsw_num_threads_,
                [&](size_t row, size_t /*thread_id*/) { hnsw_index_->markDelete(delete_ids[row]); });
  } catch (std::runtime_error& e) {
    DINGO_LOG(ERROR) << "delete vector failed, error=" << e.what();
    ret = butil::Status(pb::error::Errno::EINTERNAL, "delete vector failed, error=" + std::string(e.what()));
  }

  return ret;
}

butil::Status VectorIndexHnsw::Save(const std::string& path) {
  // Save need the caller to do LockWrite() and UnlockWrite()
  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    hnsw_index_->saveIndex(path);
    return butil::Status::OK();
  } else {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
  }
}

butil::Status VectorIndexHnsw::Load(const std::string& path) {
  BAIDU_SCOPED_LOCK(mutex_);

  // FIXME: need to prevent SEGV when delete old_hnsw_index
  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    auto* old_hnsw_index = hnsw_index_;
    hnsw_index_ = new hnswlib::HierarchicalNSW<float>(hnsw_space_, path, false,
                                                      vector_index_parameter.hnsw_parameter().max_elements(), true);
    delete old_hnsw_index;
    return butil::Status::OK();
  } else {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
  }
}

butil::Status VectorIndexHnsw::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                      std::vector<pb::index::VectorWithDistanceResult>& results, bool reconstruct) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  if (topk == 0) {
    DINGO_LOG(WARNING) << "topk is invalid";
    return butil::Status::OK();
  }

  if (vector_index_type != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
  }

  if (vector_with_ids.empty()) {
    return butil::Status::OK();
  }

  if (vector_with_ids[0].vector().float_values_size() != this->dimension_) {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector dimension is not match, input=%d, index=%d",
                         vector_with_ids[0].vector().float_values_size(), this->dimension_);
  }

  butil::Status ret;

  std::unique_ptr<float> data;
  try {
    data.reset(new float[this->dimension_ * vector_with_ids.size()]);
  } catch (std::bad_alloc& e) {
    DINGO_LOG(ERROR) << "upsert vector failed, error=" << e.what();
    ret = butil::Status(pb::error::Errno::EINTERNAL, "upsert vector failed, error=" + std::string(e.what()));
    return ret;
  }

  for (size_t row = 0; row < vector_with_ids.size(); ++row) {
    if (vector_with_ids[row].vector().float_values_size() != this->dimension_) {
      return butil::Status(pb::error::Errno::EVECTOR_INVALID, "vector dimension is not match, input=%d, index=%d",
                           vector_with_ids[row].vector().float_values_size(), this->dimension_);
    }
    for (size_t col = 0; col < this->dimension_; ++col) {
      data.get()[row * this->dimension_ + col] = vector_with_ids[row].vector().float_values().at(col);
    }
  }

  // Query the elements for themselves and measure recall
  std::vector<hnswlib::labeltype> neighbors(vector_with_ids.size());

  // Search by parallel
  results.resize(vector_with_ids.size());
  try {
    ParallelFor(0, vector_with_ids.size(), hnsw_num_threads_, [&](size_t row, size_t /*thread_id*/) {
      std::priority_queue<std::pair<float, hnswlib::labeltype>> result =
          hnsw_index_->searchKnn(data.get() + dimension_ * row, topk);

      while (!result.empty()) {
        auto* vector_with_distance = results[row].add_vector_with_distances();
        vector_with_distance->set_distance(result.top().first);
        vector_with_distance->set_metric_type(this->vector_index_parameter.hnsw_parameter().metric_type());

        auto* vector_with_id = vector_with_distance->mutable_vector_with_id();

        vector_with_id->set_id(result.top().second);
        vector_with_id->mutable_vector()->set_dimension(dimension_);
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);

        if (reconstruct) {
          try {
            std::vector<float> data = hnsw_index_->getDataByLabel<float>(result.top().second);
            for (auto& value : data) {
              vector_with_id->mutable_vector()->add_float_values(value);
            }
          } catch (std::exception& e) {
            LOG(ERROR) << "getDataByLabel failed, label: " << result.top().second << " err: " << e.what();
          }
        }

        result.pop();
      }
    });
  } catch (std::runtime_error& e) {
    DINGO_LOG(ERROR) << "parallel search vector failed, error=" << e.what();
    ret = butil::Status(pb::error::Errno::EINTERNAL, "parallel search vector failed, error=" + std::string(e.what()));
  }

  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::SetOnline() {
  is_online_.store(true);
  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::SetOffline() {
  is_online_.store(false);
  return butil::Status::OK();
}

bool VectorIndexHnsw::IsOnline() { return is_online_.load(); }

void VectorIndexHnsw::LockWrite() { bthread_mutex_lock(&mutex_); }

void VectorIndexHnsw::UnlockWrite() { bthread_mutex_unlock(&mutex_); }

butil::Status VectorIndexHnsw::GetCount([[maybe_unused]] uint64_t& count) {
  count = this->hnsw_index_->getCurrentElementCount();
  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::NeedToRebuild([[maybe_unused]] bool& need_to_rebuild,
                                             [[maybe_unused]] uint64_t last_save_log_behind) {
  auto element_count = this->hnsw_index_->getCurrentElementCount();
  auto deleted_count = this->hnsw_index_->getDeletedCount();

  if (element_count == 0 || deleted_count == 0) {
    need_to_rebuild = false;
    return butil::Status::OK();
  }

  if (deleted_count > 0 && deleted_count > element_count / 2) {
    need_to_rebuild = true;
    return butil::Status::OK();
  }

  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::NeedToSave([[maybe_unused]] bool& need_to_save,
                                          [[maybe_unused]] uint64_t last_save_log_behind) {
  if (this->status != pb::common::RegionVectorIndexStatus::VECTOR_INDEX_STATUS_NORMAL) {
    DINGO_LOG(INFO) << "need_to_save=false: vector index status is not normal, status="
                    << pb::common::RegionVectorIndexStatus_Name(this->status.load());
    need_to_save = false;
    return butil::Status::OK();
  }

  auto element_count = this->hnsw_index_->getCurrentElementCount();
  auto deleted_count = this->hnsw_index_->getDeletedCount();

  if (element_count == 0 && deleted_count == 0) {
    DINGO_LOG(INFO) << "need_to_save=false: element count is 0 and deleted count is 0, element_count=" << element_count
                    << ", deleted_count=" << deleted_count;
    need_to_save = false;
    return butil::Status::OK();
  }

  if (snapshot_log_index.load() == 0) {
    DINGO_LOG(INFO) << "need_to_save=true: snapshot_log_index is 0";
    need_to_save = true;
    return butil::Status::OK();
  }

  if (last_save_log_behind > FLAGS_hnsw_need_save_count) {
    DINGO_LOG(INFO) << "need_to_save=true: last_save_log_behind=" << last_save_log_behind
                    << ", FLAGS_hnsw_need_save_count=" << FLAGS_hnsw_need_save_count;
    need_to_save = true;
  }

  DINGO_LOG(INFO) << "need_to_save=false: last_save_log_behind=" << last_save_log_behind
                  << ", FLAGS_hnsw_need_save_count=" << FLAGS_hnsw_need_save_count;

  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::ResizeMaxElements(uint64_t new_max_elements) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    hnsw_index_->resizeIndex(new_max_elements);
    return butil::Status::OK();
  } else {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
  }

  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::GetMaxElements(uint64_t& max_elements) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    max_elements = hnsw_index_->getMaxElements();
    return butil::Status::OK();
  } else {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
  }

  return butil::Status::OK();
}

}  // namespace dingodb
