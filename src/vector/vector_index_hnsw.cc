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
#include "butil/compiler_specific.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

DEFINE_int64(max_hnsw_memory_size_of_region, 1024L * 1024L * 1024L, "max memory size of region in HSNW");
DEFINE_int32(max_hnsw_nlinks_of_region, 4096, "max nlinks of region in HSNW");

DEFINE_int32(max_hnsw_parallel_thread_num, 1, "max hnsw parallel thread num");
DEFINE_int32(max_hnsw_parallel_thread_num_per_request, 32, "max hnsw parallel thread num to acquire in a request");
DEFINE_int64(hnsw_need_save_count, 10000, "hnsw need save count");
DEFINE_uint32(hnsw_max_init_max_elements, 100000, "hnsw max init max elements");

DECLARE_int64(vector_max_batch_count);

HnswThreadConig& HnswThreadConig::GetInstance() {
  static HnswThreadConig instance;
  return instance;
}

HnswThreadConig::HnswThreadConig() {
  bthread_mutex_init(&mutex_, nullptr);
  if (FLAGS_max_hnsw_parallel_thread_num > 0) {
    max_thread_num_ = FLAGS_max_hnsw_parallel_thread_num;
  } else {
    max_thread_num_ = std::thread::hardware_concurrency();
  }
  DINGO_LOG(INFO) << fmt::format("[vector_index.hnsw] max hnsw parallel thread num is set to {}", max_thread_num_);
}

uint32_t HnswThreadConig::AcquireThreads(uint32_t num) {
  CHECK(num > 0);
  BAIDU_SCOPED_LOCK(mutex_);
  if (running_thread_num_ >= max_thread_num_) {
    return 0;
  } else {
    uint32_t acquire_num = std::min(num, max_thread_num_ - running_thread_num_);
    running_thread_num_ += acquire_num;
    return acquire_num;
  }
}

void HnswThreadConig::ReleaseThreads(uint32_t num) {
  CHECK(num > 0);
  BAIDU_SCOPED_LOCK(mutex_);
  running_thread_num_ -= num;
  if (BAIDU_UNLIKELY(running_thread_num_ < 0)) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.hnsw] running_thread_num_ is illegal, running_thread_num_={} max_thread_num_={} num={}",
        running_thread_num_, max_thread_num_, num);
    running_thread_num_ = 0;
  }
}

// Filter vecotr id used by region range.
class HnswRangeFilterFunctor : public hnswlib::BaseFilterFunctor {
 public:
  HnswRangeFilterFunctor(std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters) : filters_(filters) {}
  ~HnswRangeFilterFunctor() override = default;
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
    // std::vector<Bthread> threads;
    std::atomic<size_t> current(start);

    // keep track of exceptions in threads
    // https://stackoverflow.com/a/32428427/1713196
    std::exception_ptr last_exception = nullptr;
    std::mutex last_except_mutex;

    for (size_t thread_id = 0; thread_id < num_threads; ++thread_id) {
      threads.push_back(std::thread([&, thread_id] {
        // threads.push_back(Bthread([&, thread_id] {
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
      // thread.Join();
    }
    if (last_exception) {
      std::rethrow_exception(last_exception);
    }
  }
}

VectorIndexHnsw::VectorIndexHnsw(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                 const pb::common::RegionEpoch& epoch, const pb::common::Range& range)
    : VectorIndex(id, vector_index_parameter, epoch, range), hnsw_space_(nullptr), hnsw_index_(nullptr) {
  bthread_mutex_init(&mutex_, nullptr);

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
    max_element_limit_ = hnsw_parameter.max_elements();

    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.hnsw][id({})] create index, init_max_elements={} max_element_limit={} nlinks={} "
        "efconstruction={} "
        "metric_type={} dimension={}",
        Id(), FLAGS_hnsw_max_init_max_elements, max_element_limit_, hnsw_parameter.nlinks(),
        hnsw_parameter.efconstruction(), pb::common::MetricType_Name(hnsw_parameter.metric_type()),
        hnsw_parameter.dimension());

    hnsw_index_ =
        new hnswlib::HierarchicalNSW<float>(hnsw_space_, FLAGS_hnsw_max_init_max_elements, hnsw_parameter.nlinks(),
                                            hnsw_parameter.efconstruction(), 100, false);
  }
}

VectorIndexHnsw::~VectorIndexHnsw() {
  delete hnsw_index_;
  delete hnsw_space_;

  bthread_mutex_destroy(&mutex_);
}

butil::Status VectorIndexHnsw::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return Upsert(vector_with_ids);
}

butil::Status VectorIndexHnsw::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.hnsw][id({})] upsert vector empty.", Id());
    return butil::Status::OK();
  }

  // check
  uint32_t input_dimension = vector_with_ids[0].vector().float_values_size();
  if (input_dimension != static_cast<size_t>(dimension_)) {
    std::string s = fmt::format("dimension is invalid, expect({}) input({})", dimension_, input_dimension);
    DINGO_LOG(ERROR) << fmt::format("[vector_index.hnsw][id({})] {}", s);
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
  }

  BAIDU_SCOPED_LOCK(mutex_);

  // Add data to index
  try {
    // check if we need to expand the max_elements
    auto batch_count = std::max(FLAGS_vector_max_batch_count, static_cast<int64_t>(vector_with_ids.size()));
    if (hnsw_index_->cur_element_count + batch_count * 2 > hnsw_index_->max_elements_) {
      auto new_max_elements = hnsw_index_->max_elements_ * 2;
      DINGO_LOG(INFO) << fmt::format("[vector_index.hnsw][id({})] expand max element, {} -> {}.", Id(),
                                     hnsw_index_->max_elements_, new_max_elements);

      hnsw_index_->resizeIndex(new_max_elements);
    }

    int32_t acquire_num = vector_with_ids.size() > FLAGS_max_hnsw_parallel_thread_num_per_request
                              ? FLAGS_max_hnsw_parallel_thread_num_per_request
                              : vector_with_ids.size();

    size_t available_threads = HnswThreadConig::GetInstance().AcquireThreads(acquire_num);
    DEFER(if (available_threads > 0) { HnswThreadConig::GetInstance().ReleaseThreads(available_threads); });

    if (available_threads > 0) {
      DINGO_LOG(DEBUG) << fmt::format(
          "[vector_index.hnsw][id({})] upsert, count({}) acquire_num({}) available_threads({})", id,
          vector_with_ids.size(), acquire_num, available_threads);
    }

    size_t real_threads = available_threads > 0 ? available_threads : 1;

    if (BAIDU_UNLIKELY(hnsw_index_->M_ < real_threads &&
                       hnsw_index_->cur_element_count.load(std::memory_order_relaxed) <
                           real_threads * hnsw_index_->M_)) {
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
    int64_t current_element_count = hnsw_index_->getCurrentElementCount();
    int64_t max_element_count = hnsw_index_->getMaxElements();
    std::string s = fmt::format("upsert failed, current_element_count({}) max_element_count({}) error: {}",
                                current_element_count, max_element_count, e.what());
    DINGO_LOG(ERROR) << fmt::format("[vector_index.hnsw][id({})] {}", Id(), s);
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }
}

butil::Status VectorIndexHnsw::Delete(const std::vector<int64_t>& delete_ids) {
  if (delete_ids.empty()) {
    return butil::Status::OK();
  }

  butil::Status ret;

  int32_t acquire_num = delete_ids.size() > FLAGS_max_hnsw_parallel_thread_num_per_request
                            ? FLAGS_max_hnsw_parallel_thread_num_per_request
                            : delete_ids.size();

  size_t available_threads = HnswThreadConig::GetInstance().AcquireThreads(acquire_num);
  DEFER(if (available_threads > 0) { HnswThreadConig::GetInstance().ReleaseThreads(available_threads); });

  if (available_threads > 0) {
    DINGO_LOG(DEBUG) << fmt::format(
        "[vector_index.hnsw][id({})] delete, count({}) acquire_num({}) available_threads({})", Id(), delete_ids.size(),
        acquire_num, available_threads);
  }

  size_t real_threads = available_threads > 0 ? available_threads : 1;

  BAIDU_SCOPED_LOCK(mutex_);

  // Add data to index
  try {
    ParallelFor(0, delete_ids.size(), real_threads,
                [&](size_t row, size_t /*thread_id*/) { hnsw_index_->markDelete(delete_ids[row]); });
  } catch (std::runtime_error& e) {
    std::string s = fmt::format("delete vector failed, error: {}", e.what());
    DINGO_LOG(ERROR) << fmt::format("[vector_index.hnsw][id({})] {}", Id(), s);
    ret = butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  return ret;
}

bool VectorIndexHnsw::SupportSave() { return true; }

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
    uint32_t actual_max_elements =
        vector_index_parameter.hnsw_parameter().max_elements() + Constant::kHnswMaxElementsExpandNum;
    hnsw_index_ = new hnswlib::HierarchicalNSW<float>(hnsw_space_, path, false, actual_max_elements, true);
    delete old_hnsw_index;
    return butil::Status::OK();
  } else {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
  }
}

butil::Status VectorIndexHnsw::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                      std::vector<std::shared_ptr<FilterFunctor>> filters, bool reconstruct,
                                      const pb::common::VectorSearchParameter& search_parameter,
                                      std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    return butil::Status::OK();
  }

  if (topk == 0) {
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

  if (search_parameter.hnsw().efsearch() < 0 || search_parameter.hnsw().efsearch() > 1024) {
    std::string s = fmt::format("efsearch is illegal, {}, must between 0 and 1024", search_parameter.hnsw().efsearch());
    DINGO_LOG(ERROR) << fmt::format("[vector_index.hnsw][id({})] {}", Id(), s);
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  butil::Status ret;

  std::unique_ptr<float[]> data;
  try {
    data.reset(new float[this->dimension_ * vector_with_ids.size()]);
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("upsert vector failed, error: {}", e.what());
    DINGO_LOG(ERROR) << fmt::format("[vector_index.hnsw][id({})] {}", Id(), s);
    ret = butil::Status(pb::error::Errno::EINTERNAL, s);
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
          LOG(ERROR) << fmt::format("[vector_index.hnsw][id({})] {}", Id(), s);
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
      LOG(WARNING) << fmt::format("[vector_index.hnsw] tok and result size not match, topk: {} result: {}", topk,
                                  result.size());
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

  auto hnsw_filter = filters.empty() ? nullptr : std::make_shared<HnswRangeFilterFunctor>(filters);

  int32_t acquire_num = vector_with_ids.size() > FLAGS_max_hnsw_parallel_thread_num_per_request
                            ? FLAGS_max_hnsw_parallel_thread_num_per_request
                            : vector_with_ids.size();

  size_t available_threads = HnswThreadConig::GetInstance().AcquireThreads(acquire_num);
  DEFER(if (available_threads > 0) { HnswThreadConig::GetInstance().ReleaseThreads(available_threads); });

  if (available_threads > 0) {
    DINGO_LOG(DEBUG) << fmt::format(
        "[vector_index.hnsw][id({})] search, count({}) acquire_num({}) available_threads({})", Id(),
        vector_with_ids.size(), acquire_num, available_threads);
  }

  size_t real_threads = available_threads > 0 ? available_threads : 1;

  BAIDU_SCOPED_LOCK(mutex_);

  if (search_parameter.hnsw().efsearch() > 0) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.hnsw][id({})] set ef_search({}).", Id(),
                                   search_parameter.hnsw().efsearch());
    hnsw_index_->setEf(search_parameter.hnsw().efsearch());
  }

  if (!normalize_) {
    ParallelFor(0, vector_with_ids.size(), real_threads, [&](size_t row, size_t /*thread_id*/) {
      std::priority_queue<std::pair<float, hnswlib::labeltype>> result;

      try {
        result = hnsw_index_->searchKnn(data.get() + dimension_ * row, topk, hnsw_filter.get());
      } catch (std::runtime_error& e) {
        std::string s = fmt::format("parallel search vector failed, error: {}", e.what());
        LOG(ERROR) << fmt::format("[vector_index.hnsw][id({})] {}", Id(), s);
        statuses[row] = butil::Status(pb::error::Errno::EINTERNAL, s);
        return;
      }

      statuses[row] = lambda_reverse_rse_result_function(result, row, topk);
      if (statuses[row].ok()) {
        statuses[row] = lambda_fill_results_function(row, topk, reconstruct);
      }
    });
  } else {  // normalize_
    std::vector<float> norm_array(real_threads * dimension_);
    ParallelFor(0, vector_with_ids.size(), real_threads, [&](size_t row, size_t thread_id) {
      size_t start_idx = thread_id * dimension_;
      VectorIndexUtils::NormalizeVectorForHnsw((float*)(data.get() + dimension_ * row), dimension_,  // NOLINT
                                               (norm_array.data() + start_idx));

      std::priority_queue<std::pair<float, hnswlib::labeltype>> result;

      try {
        result = hnsw_index_->searchKnn(norm_array.data() + start_idx, topk, hnsw_filter.get());
      } catch (std::runtime_error& e) {
        std::string s = fmt::format("parallel search vector failed, error: {}", e.what());
        LOG(ERROR) << fmt::format("[vector_index.hnsw][id({})] {}", Id(), s);
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

butil::Status VectorIndexHnsw::RangeSearch(std::vector<pb::common::VectorWithId> /*vector_with_ids*/, float /*radius*/,
                                           std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> /*filters*/,
                                           bool /*reconstruct*/, const pb::common::VectorSearchParameter& /*parameter*/,
                                           std::vector<pb::index::VectorWithDistanceResult>& /*results*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "RangeSearch not support in Hnsw!!!");
}

void VectorIndexHnsw::LockWrite() { bthread_mutex_lock(&mutex_); }

void VectorIndexHnsw::UnlockWrite() { bthread_mutex_unlock(&mutex_); }

butil::Status VectorIndexHnsw::ResizeMaxElements(int64_t new_max_elements) {
  BAIDU_SCOPED_LOCK(mutex_);

  try {
    if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      hnsw_index_->resizeIndex(new_max_elements);
      return butil::Status::OK();
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
    }
  } catch (std::runtime_error& e) {
    std::string s = fmt::format("resize index failed, error: {}", e.what());
    DINGO_LOG(ERROR) << fmt::format("[vector_index.hnsw][id({})] {}", Id(), s);
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::GetMaxElements(int64_t& max_elements) {
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

  return hnsw_index_->getCurrentElementCount() >= max_element_limit_;
}

hnswlib::HierarchicalNSW<float>* VectorIndexHnsw::GetHnswIndex() { return this->hnsw_index_; }

int32_t VectorIndexHnsw::GetDimension() { return this->dimension_; }

pb::common::MetricType VectorIndexHnsw::GetMetricType() {
  return this->vector_index_parameter.hnsw_parameter().metric_type();
}

butil::Status VectorIndexHnsw::GetCount(int64_t& count) {
  // std::unique_lock<std::mutex> lock_table(this->hnsw_index_->label_lookup_lock);
  // count = this->hnsw_index_->label_lookup_.size();
  count = this->hnsw_index_->getCurrentElementCount();
  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::GetDeletedCount(int64_t& deleted_count) {
  // std::unique_lock<std::mutex> lock_deleted_elements(this->hnsw_index_->deleted_elements_lock);
  // deleted_count = this->hnsw_index_->deleted_elements.size();
  deleted_count = this->hnsw_index_->getDeletedCount();
  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::GetMemorySize(int64_t& memory_size) {
  memory_size = hnsw_index_->indexFileSize();
  return butil::Status::OK();
}

bool VectorIndexHnsw::NeedToRebuild() {
  int64_t element_count = 0, deleted_count = 0;

  element_count = this->hnsw_index_->getCurrentElementCount();

  deleted_count = this->hnsw_index_->getDeletedCount();

  if (element_count == 0 || deleted_count == 0) {
    return false;
  }

  return (deleted_count > 0 && deleted_count > element_count / 2);
}

bool VectorIndexHnsw::NeedToSave(int64_t last_save_log_behind) {
  BAIDU_SCOPED_LOCK(mutex_);

  int64_t element_count = 0, deleted_count = 0;

  element_count = this->hnsw_index_->getCurrentElementCount();
  deleted_count = this->hnsw_index_->getDeletedCount();

  if (element_count == 0 && deleted_count == 0) {
    return false;
  }

  if (last_save_log_behind > FLAGS_hnsw_need_save_count) {
    return true;
  }

  return false;
}

// calc hnsw count from memory
uint32_t VectorIndexHnsw::CalcHnswCountFromMemory(int64_t memory_size_limit, int64_t dimension, int64_t nlinks) {
  // size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
  int64_t size_links_level0 = nlinks * 2 + sizeof(int64_t) + sizeof(int64_t);

  // int64_t size_data_per_element_ = size_links_level0_ + data_size_ + sizeof(labeltype);
  int64_t size_data_per_element = size_links_level0 + sizeof(float_t) * dimension + sizeof(int64_t);

  // int64_t size_link_list_per_element =  sizeof(void*);
  int64_t size_link_list_per_element = sizeof(int64_t);

  int64_t count = memory_size_limit / (size_data_per_element + size_link_list_per_element);

  if (count > UINT32_MAX) {
    count = UINT32_MAX;
  }

  return static_cast<uint32_t>(count);
}

butil::Status VectorIndexHnsw::CheckAndSetHnswParameter(pb::common::CreateHnswParam& hnsw_parameter) {
  if (hnsw_parameter.dimension() <= 0) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.hnsw] dimension is too small, dimension({}).",
                                    hnsw_parameter.dimension());
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "hnsw dimension is too small");
  }

  if (hnsw_parameter.nlinks() > FLAGS_max_hnsw_nlinks_of_region) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.hnsw] nlinks is too big, nlinks({}) max_nlinks({}).",
                                      hnsw_parameter.nlinks(), FLAGS_max_hnsw_nlinks_of_region);
    hnsw_parameter.set_nlinks(FLAGS_max_hnsw_nlinks_of_region);
  }

  auto max_element_limit = CalcHnswCountFromMemory(FLAGS_max_hnsw_memory_size_of_region, hnsw_parameter.dimension(),
                                                   hnsw_parameter.nlinks());
  hnsw_parameter.set_max_elements(max_element_limit);
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.hnsw] calc max element limit is {}, paramiter max_hnsw_memory_size_of_region({}) dimension({}) "
      "nlinks({}).",
      max_element_limit, FLAGS_max_hnsw_memory_size_of_region, hnsw_parameter.dimension(), hnsw_parameter.nlinks());

  return butil::Status::OK();
}

}  // namespace dingodb
