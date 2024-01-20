// Copyright(c) 2023 dingodb.com, Inc.All Rights Reserved
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

#ifndef DINGO_BVAR_MULTI_DIMENSION_H
#define DINGO_BVAR_MULTI_DIMENSION_H

#include "butil/containers/doubly_buffered_data.h"  // DBD
#include "butil/containers/flat_map.h"              // butil::FlatMap
#include "butil/logging.h"                          // LOG
#include "butil/macros.h"                           // BAIDU_CASSERT
#include "butil/scoped_lock.h"                      // BAIDU_SCOPE_LOCK
#include "bvar/latency_recorder.h"
#include "bvar/mvariable.h"
#include "bvar/variable.h"

namespace bvar {
DECLARE_int32(bvar_latency_p1);
DECLARE_int32(bvar_latency_p2);
DECLARE_int32(bvar_latency_p3);
DECLARE_int32(bvar_max_dump_multi_dimension_metric_number);
}  // namespace bvar

namespace dingodb {

constexpr int64_t MAX_MULTI_DIMENSION_STATS_COUNT = 100000;  // NOLINT

template <typename T>
class DingoMultiDimension : public bvar::MVariable {
 public:
  enum STATS_OP {    // NOLINT
    READ_ONLY,       // NOLINT
    READ_OR_INSERT,  // NOLINT
  };

  typedef MVariable Base;                   // NOLINT
  typedef std::list<std::string> key_type;  // NOLINT
  typedef T value_type;                     // NOLINT
  typedef T* value_ptr_type;                // NOLINT

  struct KeyHash {
    size_t operator()(const key_type& key) const {
      size_t hash_value = 0;
      for (const auto& k : key) {
        hash_value += std::hash<std::string>()(k);
      }
      return hash_value;
    }
  };

  typedef value_ptr_type op_value_type;                                         // NOLINT
  typedef typename butil::FlatMap<key_type, op_value_type, KeyHash> MetricMap;  // NOLINT

  typedef typename MetricMap::const_iterator MetricMapConstIterator;   // NOLINT
  typedef typename butil::DoublyBufferedData<MetricMap> MetricMapDBD;  // NOLINT
  typedef typename MetricMapDBD::ScopedPtr MetricMapScopedPtr;         // NOLINT

  explicit DingoMultiDimension(const key_type& labels);

  DingoMultiDimension(const butil::StringPiece& name, const key_type& labels);

  DingoMultiDimension(const butil::StringPiece& prefix, const butil::StringPiece& name, const key_type& labels);

  ~DingoMultiDimension() override;

  // Implement this method to print the variable into ostream.
  void describe(std::ostream& os) override;

  // Dump real bvar pointer
  size_t dump(bvar::Dumper* dumper, const bvar::DumpOptions* options) override;

  // Get real bvar pointer object
  // Return real bvar pointer on success, NULL otherwise.
  T* get_stats(const key_type& labels_value) { return get_stats_impl(labels_value, READ_OR_INSERT); }  // NOLINT

  // Remove stat so those not count and dump
  void delete_stats(const key_type& labels_value);  // NOLINT

  // True if bvar pointer exists
  bool has_stats(const key_type& labels_value);  // NOLINT

  // Get number of stats
  size_t count_stats();  // NOLINT

  // Put name of all stats label into `names'
  void list_stats(std::vector<key_type>* names);  // NOLINT

  // Remove all stats so those not count and dump
  void delete_stats();  // NOLINT

 private:
  T* get_stats_impl(const key_type& labels_value);  // NOLINT

  T* get_stats_impl(const key_type& labels_value, STATS_OP stats_op, bool* do_write = NULL);  // NOLINT

  void make_dump_key(std::ostream& os, const key_type& labels_value, const std::string& suffix = "",  // NOLINT
                     const int quantile = 0);                                                         // NOLINT

  void make_labels_kvpair_string(std::ostream& os, const key_type& labels_value, const int quantile);  // NOLINT

  bool is_valid_lables_value(const key_type& labels_value) const;  // NOLINT

  static size_t init_flatmap(MetricMap& bg);  // NOLINT

  MetricMapDBD _metric_map;  // NOLINT
};

static const std::string ALLOW_UNUSED METRIC_TYPE_COUNTER = "counter";      // NOLINT
static const std::string ALLOW_UNUSED METRIC_TYPE_SUMMARY = "summary";      // NOLINT
static const std::string ALLOW_UNUSED METRIC_TYPE_HISTOGRAM = "histogram";  // NOLINT
static const std::string ALLOW_UNUSED METRIC_TYPE_GAUGE = "gauge";          // NOLINT

template <typename T>
inline DingoMultiDimension<T>::DingoMultiDimension(const key_type& labels) : Base(labels) {
  _metric_map.Modify(init_flatmap);
}

template <typename T>
inline DingoMultiDimension<T>::DingoMultiDimension(const butil::StringPiece& name, const key_type& labels)
    : Base(labels) {
  _metric_map.Modify(init_flatmap);
  this->expose(name);
}

template <typename T>
inline DingoMultiDimension<T>::DingoMultiDimension(const butil::StringPiece& prefix, const butil::StringPiece& name,
                                                   const key_type& labels)
    : Base(labels) {
  _metric_map.Modify(init_flatmap);
  this->expose_as(prefix, name);
}

template <typename T>
DingoMultiDimension<T>::~DingoMultiDimension() {
  hide();
  delete_stats();
}

template <typename T>
inline size_t DingoMultiDimension<T>::init_flatmap(MetricMap& bg) {
  // size = 1 << 13
  CHECK_EQ(0, bg.init(8192, 80));
  return (size_t)1;
}

template <typename T>
inline size_t DingoMultiDimension<T>::count_stats() {
  MetricMapScopedPtr metric_map_ptr;
  if (_metric_map.Read(&metric_map_ptr) != 0) {
    LOG(ERROR) << "Fail to read dbd";
    return 0;
  }
  return metric_map_ptr->size();
}

template <typename T>
inline void DingoMultiDimension<T>::delete_stats(const key_type& labels_value) {
  if (is_valid_lables_value(labels_value)) {
    // Because there are two copies(foreground and background) in DBD, we need to use an empty tmp_metric,
    // get the deleted value of second copy into tmp_metric, which can prevent the bvar object from being deleted
    // twice.
    op_value_type tmp_metric = NULL;
    auto erase_fn = [&labels_value, &tmp_metric](MetricMap& bg) {
      auto it = bg.seek(labels_value);
      if (it != NULL) {
        tmp_metric = *it;
        bg.erase(labels_value);
        return 1;
      }
      return 0;
    };
    _metric_map.Modify(erase_fn);

    delete tmp_metric;
  }
}

template <typename T>
inline void DingoMultiDimension<T>::delete_stats() {
  // Because there are two copies(foreground and background) in DBD, we need to use an empty tmp_map,
  // swap two copies with empty, and get the value of second copy into tmp_map,
  // then traversal tmp_map and delete bvar object,
  // which can prevent the bvar object from being deleted twice.
  MetricMap tmp_map;
  CHECK_EQ(0, tmp_map.init(8192, 80));
  auto clear_fn = [&tmp_map](MetricMap& map) {
    if (!tmp_map.empty()) {
      tmp_map.clear();
    }
    tmp_map.swap(map);
    return (size_t)1;
  };
  int ret = _metric_map.Modify(clear_fn);
  CHECK_EQ(1, ret);
  for (auto& kv : tmp_map) {
    delete kv.second;
  }
}

template <typename T>
inline void DingoMultiDimension<T>::list_stats(std::vector<key_type>* names) {
  if (names == nullptr) {
    return;
  }
  names->clear();
  MetricMapScopedPtr metric_map_ptr;
  if (_metric_map.Read(&metric_map_ptr) != 0) {
    LOG(ERROR) << "Fail to read dbd";
    return;
  }
  names->reserve(metric_map_ptr->size());
  for (auto it = metric_map_ptr->begin(); it != metric_map_ptr->end(); ++it) {
    names->emplace_back(it->first);
  }
}

template <typename T>
inline T* DingoMultiDimension<T>::get_stats_impl(const key_type& labels_value) {
  if (!is_valid_lables_value(labels_value)) {
    return nullptr;
  }
  MetricMapScopedPtr metric_map_ptr;
  if (_metric_map.Read(&metric_map_ptr) != 0) {
    LOG(ERROR) << "Fail to read dbd";
    return nullptr;
  }

  auto it = metric_map_ptr->seek(labels_value);
  if (it == nullptr) {
    return nullptr;
  }
  return (*it);
}

template <typename T>
inline T* DingoMultiDimension<T>::get_stats_impl(const key_type& labels_value, STATS_OP stats_op, bool* do_write) {
  if (!is_valid_lables_value(labels_value)) {
    return nullptr;
  }
  {
    MetricMapScopedPtr metric_map_ptr;
    if (_metric_map.Read(&metric_map_ptr) != 0) {
      LOG(ERROR) << "Fail to read dbd";
      return nullptr;
    }

    auto it = metric_map_ptr->seek(labels_value);
    if (it != NULL) {
      return (*it);
    } else if (READ_ONLY == stats_op) {
      return nullptr;
    }

    if (metric_map_ptr->size() > MAX_MULTI_DIMENSION_STATS_COUNT) {
      LOG(ERROR) << "Too many stats seen, overflow detected, max stats count:" << MAX_MULTI_DIMENSION_STATS_COUNT;
      return nullptr;
    }
  }

  // Because DBD has two copies(foreground and background) MetricMap, both copies need to be modify,
  // In order to avoid new duplicate bvar object, need use cache_metric to cache the new bvar object,
  // In this way, when modifying the second copy, can directly use the cache_metric bvar object.
  op_value_type cache_metric = NULL;
  auto insert_fn = [&labels_value, &cache_metric, &do_write](MetricMap& bg) {
    auto bg_metric = bg.seek(labels_value);
    if (NULL != bg_metric) {
      cache_metric = *bg_metric;
      return 0;
    }
    if (do_write) {
      *do_write = true;
    }
    if (NULL != cache_metric) {
      bg.insert(labels_value, cache_metric);
    } else {
      T* add_metric = new T();
      bg.insert(labels_value, add_metric);
      cache_metric = add_metric;
    }
    return 1;
  };
  _metric_map.Modify(insert_fn);
  return cache_metric;
}

template <typename T>
inline bool DingoMultiDimension<T>::has_stats(const key_type& labels_value) {
  return get_stats_impl(labels_value) != nullptr;
}

template <typename T>
inline size_t DingoMultiDimension<T>::dump(bvar::Dumper* dumper, const bvar::DumpOptions* options) {
  std::vector<key_type> label_names;
  list_stats(&label_names);
  if (label_names.empty() || !dumper->dump_comment(name(), METRIC_TYPE_GAUGE)) {
    return 0;
  }
  size_t n = 0;
  for (auto& label_name : label_names) {
    T* bvar = get_stats_impl(label_name);
    if (!bvar) {
      continue;
    }
    std::ostringstream oss;
    bvar->describe(oss, options->quote_string);
    std::ostringstream oss_key;
    make_dump_key(oss_key, label_name);
    if (!dumper->dump(oss_key.str(), oss.str())) {
      continue;
    }
    n++;
  }
  return n;
}

template <>
inline size_t DingoMultiDimension<bvar::LatencyRecorder>::dump(bvar::Dumper* dumper, const bvar::DumpOptions*) {
  std::vector<key_type> label_names;
  list_stats(&label_names);
  if (label_names.empty()) {
    return 0;
  }
  size_t n = 0;
  for (auto& label_name : label_names) {
    bvar::LatencyRecorder* bvar = get_stats_impl(label_name);
    if (!bvar) {
      continue;
    }

    // latency comment
    if (!dumper->dump_comment(name() + "_latency", METRIC_TYPE_GAUGE)) {
      continue;
    }
    // latency
    std::ostringstream oss_latency_key;
    make_dump_key(oss_latency_key, label_name, "_latency");
    if (dumper->dump(oss_latency_key.str(), std::to_string(bvar->latency()))) {
      n++;
    }
    // latency_percentiles
    // p1/p2/p3
    int latency_percentiles[3]{bvar::FLAGS_bvar_latency_p1, bvar::FLAGS_bvar_latency_p2, bvar::FLAGS_bvar_latency_p3};
    for (auto lp : latency_percentiles) {
      std::ostringstream oss_lp_key;
      make_dump_key(oss_lp_key, label_name, "_latency", lp);
      if (dumper->dump(oss_lp_key.str(), std::to_string(bvar->latency_percentile(lp / 100.0)))) {
        n++;
      }
    }
    // 999
    std::ostringstream oss_p999_key;
    make_dump_key(oss_p999_key, label_name, "_latency", 999);
    if (dumper->dump(oss_p999_key.str(), std::to_string(bvar->latency_percentile(0.999)))) {
      n++;
    }
    // 9999
    std::ostringstream oss_p9999_key;
    make_dump_key(oss_p9999_key, label_name, "_latency", 9999);
    if (dumper->dump(oss_p9999_key.str(), std::to_string(bvar->latency_percentile(0.9999)))) {
      n++;
    }

    // max_latency comment
    if (!dumper->dump_comment(name() + "_max_latency", METRIC_TYPE_GAUGE)) {
      continue;
    }
    // max_latency
    std::ostringstream oss_max_latency_key;
    make_dump_key(oss_max_latency_key, label_name, "_max_latency");
    if (dumper->dump(oss_max_latency_key.str(), std::to_string(bvar->max_latency()))) {
      n++;
    }

    // qps comment
    if (!dumper->dump_comment(name() + "_qps", METRIC_TYPE_GAUGE)) {
      continue;
    }
    // qps
    std::ostringstream oss_qps_key;
    make_dump_key(oss_qps_key, label_name, "_qps");
    if (dumper->dump(oss_qps_key.str(), std::to_string(bvar->qps()))) {
      n++;
    }

    // qps comment
    if (!dumper->dump_comment(name() + "_count", METRIC_TYPE_COUNTER)) {
      continue;
    }
    // count
    std::ostringstream oss_count_key;
    make_dump_key(oss_count_key, label_name, "_count");
    if (dumper->dump(oss_count_key.str(), std::to_string(bvar->count()))) {
      n++;
    }
  }
  return n;
}

template <typename T>
inline void DingoMultiDimension<T>::make_dump_key(std::ostream& os, const key_type& labels_value,
                                                  const std::string& suffix, const int quantile) {
  os << name();
  if (!suffix.empty()) {
    os << suffix;
  }
  make_labels_kvpair_string(os, labels_value, quantile);
}

template <typename T>
inline void DingoMultiDimension<T>::make_labels_kvpair_string(std::ostream& os, const key_type& labels_value,
                                                              const int quantile) {
  os << "{";
  auto label_key = _labels.cbegin();
  auto label_value = labels_value.cbegin();
  char comma[2] = {'\0', '\0'};
  for (; label_key != _labels.cend() && label_value != labels_value.cend(); label_key++, label_value++) {
    os << comma << label_key->c_str() << "=\"" << label_value->c_str() << "\"";
    comma[0] = ',';
  }
  if (quantile > 0) {
    os << ",quantile=\"" << quantile << "\"";
  }
  os << "}";
}

template <typename T>
inline bool DingoMultiDimension<T>::is_valid_lables_value(const key_type& labels_value) const {
  if (count_labels() != labels_value.size()) {
    LOG(ERROR) << "Invalid labels count";
    return false;
  }
  return true;
}

template <typename T>
inline void DingoMultiDimension<T>::describe(std::ostream& os) {
  os << R"({"name" : ")" << _name << R"(", "labels" : [)";
  char comma[3] = {'\0', ' ', '\0'};
  for (auto& label : _labels) {
    os << comma << "\"" << label << "\"";
    comma[0] = ',';
  }
  os << "], \"stats_count\" : " << count_stats() << "}";
}

}  // namespace dingodb

#endif  // BVAR_MULTI_DIMENSION_H