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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
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
#include "common/constant.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

// Filter vecotr id used by region range.
class HnswRangeFilterFunctor : public hnswlib::BaseFilterFunctor {
 public:
  HnswRangeFilterFunctor(std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters) : filters_(filters) {}
  virtual ~HnswRangeFilterFunctor() = default;
  bool operator()(hnswlib::labeltype id) override {
    if (filters_.empty()) {
      return true;
    }
    for (const auto& filter : filters_) {
      if (!filter->Check(id)) {
        return false;
      }
    }

    return true;
  }

 private:
  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters_;
};

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
          size_t id = current.fetch_add(1, std::memory_order_relaxed);

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

VectorIndexHnsw::VectorIndexHnsw(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                 const pb::common::Range& range)
    : VectorIndex(id, vector_index_parameter, range) {
  bthread_mutex_init(&mutex_, nullptr);
  hnsw_num_threads_ = std::thread::hardware_concurrency();

  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    const auto& hnsw_parameter = vector_index_parameter.hnsw_parameter();
    assert(hnsw_parameter.dimension() > 0);
    assert(hnsw_parameter.metric_type() != pb::common::MetricType::METRIC_TYPE_NONE);
    assert(hnsw_parameter.efconstruction() > 0);
    assert(hnsw_parameter.max_elements() > 0);
    assert(hnsw_parameter.nlinks() > 0);

    this->dimension_ = hnsw_parameter.dimension();

    normalize_ = false;

    if (hnsw_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
      hnsw_space_ = new hnswlib::InnerProductSpace(hnsw_parameter.dimension());
    } else if (hnsw_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_COSINE) {
      normalize_ = true;
      hnsw_space_ = new hnswlib::InnerProductSpace(hnsw_parameter.dimension());
    } else if (hnsw_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_L2) {
      hnsw_space_ = new hnswlib::L2Space(hnsw_parameter.dimension());
    }

    // avoid error write vector index failed cause leader and follower data not consistency.
    // let user_max_elements_<actual_max_elements.
    user_max_elements_ = hnsw_parameter.max_elements();
    uint32_t actual_max_elements = hnsw_parameter.max_elements() + Constant::kHnswMaxElementsExpandNum;

    hnsw_index_ = new hnswlib::HierarchicalNSW<float>(hnsw_space_, actual_max_elements, hnsw_parameter.nlinks(),
                                                      hnsw_parameter.efconstruction(), 100, false);
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

  BAIDU_SCOPED_LOCK(mutex_);

  // delete first
  // {
  //   std::vector<uint64_t> delete_ids;
  //   delete_ids.reserve(vector_with_ids.size());
  //   for (const auto& v : vector_with_ids) {
  //     delete_ids.push_back(v.id());
  //   }

  //   try {
  //     ParallelFor(0, delete_ids.size(), hnsw_num_threads_,
  //                 [&](size_t row, size_t /*thread_id*/) { hnsw_index_->markDelete(delete_ids[row]); });
  //     write_key_count += delete_ids.size();
  //   } catch (std::runtime_error& e) {
  //   }
  // }

  // Add data to index
  try {
    size_t real_threads = hnsw_num_threads_;

    if (BAIDU_UNLIKELY(hnsw_index_->M_ < hnsw_num_threads_ &&
                       hnsw_index_->cur_element_count.load(std::memory_order_relaxed) <
                           hnsw_num_threads_ * hnsw_index_->M_)) {
      real_threads = 1;
    }

    if (!normalize_) {
      ParallelFor(0, vector_with_ids.size(), real_threads, [&](size_t row, size_t /*thread_id*/) {
        this->hnsw_index_->addPoint((void*)vector_with_ids[row].vector().float_values().data(),
                                    vector_with_ids[row].id(), false);
      });
    } else {
      std::vector<float> norm_array(real_threads * dimension_);
      ParallelFor(0, vector_with_ids.size(), real_threads, [&](size_t row, size_t thread_id) {
        // normalize vector
        size_t start_idx = thread_id * dimension_;
        VectorIndexUtils::NormalizeVectorForHnsw((float*)vector_with_ids[row].vector().float_values().data(),
                                                 dimension_, (norm_array.data() + start_idx));

        this->hnsw_index_->addPoint((void*)(norm_array.data() + start_idx), vector_with_ids[row].id(), false);
      });
    }
    return butil::Status();
  } catch (std::runtime_error& e) {
    DINGO_LOG(ERROR) << "upsert vector failed, error=" << e.what();
    return butil::Status(pb::error::Errno::EINTERNAL, "upsert vector failed, error=" + std::string(e.what()));
  }
}

butil::Status VectorIndexHnsw::Delete(const std::vector<uint64_t>& delete_ids) {
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
                                      std::vector<std::shared_ptr<FilterFunctor>> filters,
                                      std::vector<pb::index::VectorWithDistanceResult>& results, bool reconstruct) {
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

  std::unique_ptr<float[]> data;
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
  std::vector<butil::Status> statuses;
  statuses.resize(vector_with_ids.size(), butil::Status::OK());

  std::vector<int> real_topks;
  real_topks.resize(vector_with_ids.size(), 0);

  auto rows = vector_with_ids.size();
  std::unique_ptr<hnswlib::labeltype[]> data_label_ptr = std::make_unique<hnswlib::labeltype[]>(rows * topk);
  hnswlib::labeltype* data_label = data_label_ptr.get();

  std::unique_ptr<float[]> data_distance_ptr = std::make_unique<float[]>(rows * topk);
  float* data_distance = data_distance_ptr.get();

  auto lambda_fill_results_function = [&results, this, data_label, data_distance, &real_topks](size_t row, int topk,
                                                                                               bool reconstruct) {
    for (int i = 0; i < topk && i < real_topks[row]; i++) {
      auto* vector_with_distance = results[row].add_vector_with_distances();
      vector_with_distance->set_distance(data_distance[row * topk + i]);
      vector_with_distance->set_metric_type(this->vector_index_parameter.hnsw_parameter().metric_type());

      auto* vector_with_id = vector_with_distance->mutable_vector_with_id();

      vector_with_id->set_id(data_label[row * topk + i]);
      vector_with_id->mutable_vector()->set_dimension(dimension_);
      vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);

      if (reconstruct) {
        try {
          std::vector<float> data = hnsw_index_->getDataByLabel<float>(data_label[row * topk + i]);
          for (auto& value : data) {
            vector_with_id->mutable_vector()->add_float_values(value);
          }
        } catch (std::exception& e) {
          std::string s =
              fmt::format("getDataByLabel failed, label: {}  err: {}", data_label[row * topk + i], e.what());
          LOG(ERROR) << s;
          return butil::Status(pb::error::Errno::EINTERNAL, s);
        }
      }
    }
    return butil::Status::OK();
  };

  auto lambda_reverse_rse_result_function = [data_label, data_distance, &real_topks](
                                                std::priority_queue<std::pair<float, hnswlib::labeltype>>& result,
                                                size_t row, int topk) {
    if (result.size() != topk) {
      std::string s = fmt::format(
          "Cannot return the results in a contigious 2D array. Probably ef or M is too small ignore.  topk : {} "
          "result.size() : "
          "{}",
          topk, result.size());
      LOG(WARNING) << s;
    }

    real_topks[row] = result.size();
    if (!result.empty()) {
      for (int i = std::min(topk, real_topks[row]) - 1; i >= 0; i--) {
        const auto& result_tuple = result.top();
        data_distance[row * topk + i] = result_tuple.first;
        data_label[row * topk + i] = result_tuple.second;
        result.pop();
      }
    }

    return butil::Status::OK();
  };

  std::unique_ptr<HnswRangeFilterFunctor> hnsw_filter_ptr;
  HnswRangeFilterFunctor* hnsw_filter =
      filters.empty() ? nullptr
                      : (hnsw_filter_ptr = std::make_unique<HnswRangeFilterFunctor>(filters), hnsw_filter_ptr.get());

  if (!normalize_) {
    ParallelFor(0, vector_with_ids.size(), hnsw_num_threads_, [&](size_t row, size_t /*thread_id*/) {
      std::priority_queue<std::pair<float, hnswlib::labeltype>> result;

      try {
        result = hnsw_index_->searchKnn(data.get() + dimension_ * row, topk, hnsw_filter);
      } catch (std::runtime_error& e) {
        std::string s = fmt::format("parallel search vector failed, error= {}", e.what());
        LOG(ERROR) << s;
        statuses[row] = butil::Status(pb::error::Errno::EINTERNAL, s);
        return;
      }

      statuses[row] = lambda_reverse_rse_result_function(result, row, topk);
      if (statuses[row].ok()) {
        statuses[row] = lambda_fill_results_function(row, topk, reconstruct);
      }
    });
  } else {  // normalize_
    std::vector<float> norm_array(hnsw_num_threads_ * dimension_);
    ParallelFor(0, vector_with_ids.size(), hnsw_num_threads_, [&](size_t row, size_t thread_id) {
      size_t start_idx = thread_id * dimension_;
      VectorIndexUtils::NormalizeVectorForHnsw((float*)(data.get() + dimension_ * row), dimension_,  // NOLINT
                                               (norm_array.data() + start_idx));

      std::priority_queue<std::pair<float, hnswlib::labeltype>> result;

      try {
        result = hnsw_index_->searchKnn(norm_array.data() + start_idx, topk, hnsw_filter);
      } catch (std::runtime_error& e) {
        std::string s = fmt::format("parallel search vector failed, error= {}", e.what());
        LOG(ERROR) << s;
        statuses[row] = butil::Status(pb::error::Errno::EINTERNAL, s);
        return;
      }

      statuses[row] = lambda_reverse_rse_result_function(result, row, topk);

      // force  reconstruct false
      if (statuses[row].ok()) {
        statuses[row] = lambda_fill_results_function(row, topk, false);
      }
    });
  }

  // check
  for (const auto& status : statuses) {
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

void VectorIndexHnsw::LockWrite() { bthread_mutex_lock(&mutex_); }

void VectorIndexHnsw::UnlockWrite() { bthread_mutex_unlock(&mutex_); }

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

bool VectorIndexHnsw::IsExceedsMaxElements() {
  if (hnsw_index_ == nullptr) {
    return true;
  }

  return hnsw_index_->getCurrentElementCount() > user_max_elements_;
}

hnswlib::HierarchicalNSW<float>* VectorIndexHnsw::GetHnswIndex() { return this->hnsw_index_; }

int32_t VectorIndexHnsw::GetDimension() { return this->dimension_; }

butil::Status VectorIndexHnsw::GetCount(uint64_t& count) {
  // std::unique_lock<std::mutex> lock_table(this->hnsw_index_->label_lookup_lock);
  // count = this->hnsw_index_->label_lookup_.size();
  count = this->hnsw_index_->getCurrentElementCount();
  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::GetDeletedCount(uint64_t& deleted_count) {
  // std::unique_lock<std::mutex> lock_deleted_elements(this->hnsw_index_->deleted_elements_lock);
  // deleted_count = this->hnsw_index_->deleted_elements.size();
  deleted_count = this->hnsw_index_->getDeletedCount();
  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::GetMemorySize(uint64_t& memory_size) {
  auto count = this->hnsw_index_->getCurrentElementCount();
  if (count == 0) {
    memory_size = 0;
    return butil::Status::OK();
  }
  auto deleted_count = this->hnsw_index_->getDeletedCount();
  auto memory_count = count + deleted_count;

  memory_size = memory_count * hnsw_index_->size_data_per_element_  // level 0 memory
                + hnsw_index_->size_links_level0_                   // level 0 links memory
                + memory_count * sizeof(void*)                      // linkLists_ memory
                + memory_count * sizeof(uint64_t)                   // element_levels_
                + memory_count * sizeof(uint64_t)                  // label_lookup_, translate user label to internal id
                + hnsw_index_->max_elements_ * sizeof(std::mutex)  // link_list_locks_
                + 65536 * sizeof(std::mutex)                       // label_op_locks_
                + memory_count * sizeof(uint64_t) * hnsw_index_->M_ * hnsw_index_->maxlevel_ /
                      2  // level 1-max_level nlinks, estimate echo vector exists in harf max_level_ count levels
      ;
  return butil::Status::OK();
}

// void VectorIndexHnsw::NormalizeVector(const float* data, float* norm_array) const {
//   float norm = 0.0f;
//   for (int i = 0; i < dimension_; i++) norm += data[i] * data[i];
//   norm = 1.0f / (sqrtf(norm) + 1e-30f);
//   for (int i = 0; i < dimension_; i++) norm_array[i] = data[i] * norm;
// }

}  // namespace dingodb
