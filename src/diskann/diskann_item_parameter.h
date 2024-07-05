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

#ifndef DINGODB_DISKANN_DISKANN_ITEM_PARAMETER_H_  // NOLINT
#define DINGODB_DISKANN_DISKANN_ITEM_PARAMETER_H_

#include <sys/types.h>
#include <xmmintrin.h>

#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "butil/status.h"
#include "common/context.h"
#include "common/synchronization.h"
#include "diskann/diskann_core.h"
#include "diskann/diskann_utils.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

#define DISKANN_ITEM_PARAMETER_NOCOPY(self)  \
  self(const self& rhs) = delete;            \
  self& operator=(const self& rhs) = delete; \
  self(self&& rhs) = delete;                 \
  self& operator=(self&& rhs) = delete;

struct DiskANNItemConstructorParameter {
  DiskANNItemConstructorParameter(std::shared_ptr<Context> ctx, int64_t vector_index_id,
                                  const pb::common::VectorIndexParameter& vector_index_parameter, u_int32_t num_threads,
                                  float search_dram_budget_gb, float build_dram_budget_gb)
      : ctx(ctx),
        vector_index_id(vector_index_id),
        vector_index_parameter(vector_index_parameter),
        num_threads(num_threads),
        search_dram_budget_gb(search_dram_budget_gb),
        build_dram_budget_gb(build_dram_budget_gb) {}
  ~DiskANNItemConstructorParameter() = default;

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemConstructorParameter);

  std::shared_ptr<Context> ctx;
  int64_t vector_index_id;
  const pb::common::VectorIndexParameter& vector_index_parameter;
  uint32_t num_threads;
  float search_dram_budget_gb;
  float build_dram_budget_gb;
};

class DiskANNItemConstructorParameterBuilder {
 public:
  DiskANNItemConstructorParameterBuilder(const pb::common::VectorIndexParameter& vector_index_parameter)
      : vector_index_parameter_(vector_index_parameter) {}

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemConstructorParameterBuilder);

  DiskANNItemConstructorParameterBuilder& SetContext(const std::shared_ptr<Context>& ctx) {
    ctx_ = ctx;
    return *this;
  }
  DiskANNItemConstructorParameterBuilder& SetVectorIndexId(int64_t vector_index_id) {
    vector_index_id_ = vector_index_id;
    return *this;
  }
  DiskANNItemConstructorParameterBuilder& SetNumThreads(uint32_t num_threads) {
    num_threads_ = num_threads;
    return *this;
  }
  DiskANNItemConstructorParameterBuilder& SetSearchDramBudgetGb(float search_dram_budget_gb) {
    search_dram_budget_gb_ = search_dram_budget_gb;
    return *this;
  }
  DiskANNItemConstructorParameterBuilder& SetBuildDramBudgetGb(float build_dram_budget_gb) {
    build_dram_budget_gb_ = build_dram_budget_gb;
    return *this;
  }
  DiskANNItemConstructorParameter Build() {
    return DiskANNItemConstructorParameter(ctx_, vector_index_id_, vector_index_parameter_, num_threads_,
                                           search_dram_budget_gb_, build_dram_budget_gb_);
  }

 private:
  std::shared_ptr<Context> ctx_;
  int64_t vector_index_id_;
  const pb::common::VectorIndexParameter& vector_index_parameter_;
  uint32_t num_threads_;
  float search_dram_budget_gb_;
  float build_dram_budget_gb_;
};

struct DiskANNItemImportParameter {
  DiskANNItemImportParameter(std::shared_ptr<Context> ctx, const std::vector<pb::common::Vector>& vectors,
                             const std::vector<int64_t>& vector_ids, bool has_more, bool force_to_load_data_if_exist,
                             int64_t already_send_vector_count, int64_t ts, int64_t tso)
      : ctx(ctx),
        vectors(vectors),
        vector_ids(vector_ids),
        has_more(has_more),
        force_to_load_data_if_exist(force_to_load_data_if_exist),
        already_send_vector_count(already_send_vector_count),
        ts(ts),
        tso(tso) {}
  ~DiskANNItemImportParameter() = default;

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemImportParameter);

  std::shared_ptr<Context> ctx;
  const std::vector<pb::common::Vector>& vectors;
  const std::vector<int64_t>& vector_ids;
  bool has_more;
  bool force_to_load_data_if_exist;
  int64_t already_send_vector_count;
  int64_t ts;
  int64_t tso;
};

class DiskANNItemImportParameterBuilder {
 public:
  DiskANNItemImportParameterBuilder(const std::vector<pb::common::Vector>& vectors,
                                    const std::vector<int64_t>& vector_ids)
      : vectors_(vectors), vector_ids_(vector_ids) {}

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemImportParameterBuilder);

  DiskANNItemImportParameterBuilder& SetContext(const std::shared_ptr<Context>& ctx) {
    ctx_ = ctx;
    return *this;
  }

  DiskANNItemImportParameterBuilder& SetHasMore(bool has_more) {
    has_more_ = has_more;
    return *this;
  }
  DiskANNItemImportParameterBuilder& SetForceToLoadDataIfExist(bool force_to_load_data_if_exist) {
    force_to_load_data_if_exist_ = force_to_load_data_if_exist;
    return *this;
  }

  DiskANNItemImportParameterBuilder& SetAlreadySendVectorCount(int64_t already_send_vector_count) {
    already_send_vector_count_ = already_send_vector_count;
    return *this;
  }
  DiskANNItemImportParameterBuilder& SetTs(int64_t ts) {
    ts_ = ts;
    return *this;
  }
  DiskANNItemImportParameterBuilder& SetTso(int64_t tso) {
    tso_ = tso;
    return *this;
  }

  DiskANNItemImportParameter Build() {
    return DiskANNItemImportParameter(ctx_, vectors_, vector_ids_, has_more_, force_to_load_data_if_exist_,
                                      already_send_vector_count_, ts_, tso_);
  }

 private:
  std::shared_ptr<Context> ctx_;
  const std::vector<pb::common::Vector>& vectors_;
  const std::vector<int64_t>& vector_ids_;
  bool has_more_;
  bool force_to_load_data_if_exist_;
  int64_t already_send_vector_count_;
  int64_t ts_;
  int64_t tso_;
};

struct DiskANNItemBuildParameter {
  DiskANNItemBuildParameter(std::shared_ptr<Context> ctx, bool force_to_build, bool is_sync)
      : ctx(ctx), force_to_build(force_to_build), is_sync(is_sync) {}
  ~DiskANNItemBuildParameter() = default;

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemBuildParameter);

  std::shared_ptr<Context> ctx;
  bool force_to_build;
  bool is_sync;
};

class DiskANNItemBuildParameterBuilder {
 public:
  DiskANNItemBuildParameterBuilder() = default;

  ~DiskANNItemBuildParameterBuilder() = default;

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemBuildParameterBuilder);

  DiskANNItemBuildParameterBuilder& SetContext(const std::shared_ptr<Context>& ctx) {
    ctx_ = ctx;
    return *this;
  }

  DiskANNItemBuildParameterBuilder& SetForceToBuild(bool force_to_build) {
    force_to_build_ = force_to_build;
    return *this;
  }
  DiskANNItemBuildParameterBuilder& SetIsSync(bool is_sync) {
    is_sync_ = is_sync;
    return *this;
  }

  DiskANNItemBuildParameter Build() { return DiskANNItemBuildParameter(ctx_, force_to_build_, is_sync_); }

 private:
  std::shared_ptr<Context> ctx_;
  bool force_to_build_;
  bool is_sync_;
};

struct DiskANNItemLoadParameter {
  DiskANNItemLoadParameter(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param, bool is_sync)
      : ctx(ctx), load_param(load_param), is_sync(is_sync) {}
  ~DiskANNItemLoadParameter() = default;

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemLoadParameter);

  std::shared_ptr<Context> ctx;
  const pb::common::LoadDiskAnnParam& load_param;
  bool is_sync;
};

class DiskANNItemLoadParameterBuilder {
 public:
  DiskANNItemLoadParameterBuilder(const pb::common::LoadDiskAnnParam& load_param) : load_param_(load_param) {}

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemLoadParameterBuilder);
  DiskANNItemLoadParameterBuilder& SetContext(const std::shared_ptr<Context>& ctx) {
    ctx_ = ctx;
    return *this;
  }

  DiskANNItemLoadParameterBuilder& SetIsSync(bool is_sync) {
    is_sync_ = is_sync;
    return *this;
  }

  DiskANNItemLoadParameter Build() { return DiskANNItemLoadParameter(ctx_, load_param_, is_sync_); }

 private:
  std::shared_ptr<Context> ctx_;
  const pb::common::LoadDiskAnnParam& load_param_;
  bool is_sync_;
};

struct DiskANNItemSearchParameter {
  DiskANNItemSearchParameter(std::shared_ptr<Context> ctx, uint32_t top_n,
                             const pb::common::SearchDiskAnnParam& search_param,
                             const std::vector<pb::common::Vector>& vectors)
      : ctx(ctx), top_n(top_n), search_param(search_param), vectors(vectors) {}
  ~DiskANNItemSearchParameter() = default;

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemSearchParameter);

  std::shared_ptr<Context> ctx;
  uint32_t top_n;
  const pb::common::SearchDiskAnnParam& search_param;
  const std::vector<pb::common::Vector>& vectors;
};

class DiskANNItemSearchParameterBuilder {
 public:
  DiskANNItemSearchParameterBuilder(const pb::common::SearchDiskAnnParam& search_param,
                                    const std::vector<pb::common::Vector>& vectors)
      : search_param_(search_param), vectors_(vectors) {}

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemSearchParameterBuilder);
  DiskANNItemSearchParameterBuilder& SetContext(const std::shared_ptr<Context>& ctx) {
    ctx_ = ctx;
    return *this;
  }

  DiskANNItemSearchParameterBuilder& SetTopN(uint32_t top_n) {
    top_n_ = top_n;
    return *this;
  }

  DiskANNItemSearchParameter Build() { return DiskANNItemSearchParameter(ctx_, top_n_, search_param_, vectors_); }

 private:
  std::shared_ptr<Context> ctx_;
  uint32_t top_n_;
  const pb::common::SearchDiskAnnParam& search_param_;
  const std::vector<pb::common::Vector>& vectors_;
};

struct DiskANNItemTryLoadParameter {
  DiskANNItemTryLoadParameter(std::shared_ptr<Context> ctx, const pb::common::LoadDiskAnnParam& load_param,
                              bool is_sync)
      : ctx(ctx), load_param(load_param), is_sync(is_sync) {}
  ~DiskANNItemTryLoadParameter() = default;

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemTryLoadParameter);

  std::shared_ptr<Context> ctx;
  const pb::common::LoadDiskAnnParam& load_param;
  bool is_sync;
};

class DiskANNItemTryLoadParameterBuilder {
 public:
  DiskANNItemTryLoadParameterBuilder(const pb::common::LoadDiskAnnParam& load_param) : load_param_(load_param) {}

  DISKANN_ITEM_PARAMETER_NOCOPY(DiskANNItemTryLoadParameterBuilder);

  DiskANNItemTryLoadParameterBuilder& SetContext(const std::shared_ptr<Context>& ctx) {
    ctx_ = ctx;
    return *this;
  }

  DiskANNItemTryLoadParameterBuilder& SetIsSync(bool is_sync) {
    is_sync_ = is_sync;
    return *this;
  }

  DiskANNItemTryLoadParameter Build() { return DiskANNItemTryLoadParameter(ctx_, load_param_, is_sync_); }

 private:
  std::shared_ptr<Context> ctx_;
  const pb::common::LoadDiskAnnParam& load_param_;
  bool is_sync_;
};

}  // namespace dingodb

#endif  // DINGODB_DISKANN_DISKANN_ITEM_PARAMETER_H_  // NOLINT
