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

#include "benchmark/dataset.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "H5Cpp.h"
#include "H5PredType.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/threadpool.h"
#include "fmt/core.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "sdk/vector.h"

DECLARE_uint32(vector_put_batch_size);
DECLARE_uint32(vector_search_topk);
DECLARE_uint32(vector_dimension);
DECLARE_bool(vector_search_not_insert);

DEFINE_uint32(load_vector_dataset_concurrency, 4, "load vector dataset concurrency");

DEFINE_uint32(batch_vector_entry_cache_size, 1024, "batch vector entry cache");

namespace dingodb {
namespace benchmark {

std::shared_ptr<Dataset> Dataset::New(std::string filepath) {
  if (filepath.find("sift") != std::string::npos) {
    return std::make_shared<SiftDataset>(filepath);

  } else if (filepath.find("glove") != std::string::npos) {
    return std::make_shared<GloveDataset>(filepath);

  } else if (filepath.find("gist") != std::string::npos) {
    return std::make_shared<GistDataset>(filepath);

  } else if (filepath.find("kosarak") != std::string::npos) {
  } else if (filepath.find("lastfm") != std::string::npos) {
  } else if (filepath.find("mnist") != std::string::npos) {
    return std::make_shared<MnistDataset>(filepath);

  } else if (filepath.find("movielens10m") != std::string::npos) {
  } else if (filepath.find("wikipedia") != std::string::npos) {
    return std::make_shared<Wikipedia2212Dataset>(filepath);
  } else if (filepath.find("beir-bioasq") != std::string::npos) {
    return std::make_shared<BeirBioasqDataset>(filepath);
  }

  std::cout << "Not support dataset, path: " << filepath << std::endl;

  return nullptr;
}

BaseDataset::BaseDataset(std::string filepath) : filepath_(filepath) {}

BaseDataset::~BaseDataset() { h5file_->close(); }

bool BaseDataset::Init() {
  std::lock_guard lock(mutex_);

  try {
    h5file_ = std::make_shared<H5::H5File>(filepath_, H5F_ACC_RDONLY);
    {
      H5::DataSet dataset = h5file_->openDataSet("train");
      H5::DataSpace dataspace = dataset.getSpace();

      hsize_t dims_out[2] = {0};
      dataspace.getSimpleExtentDims(dims_out, nullptr);
      train_row_count_ = dims_out[0];
      dimension_ = dims_out[1];
      std::cout << fmt::format("dataset train data_type({}) rank({}) dimensions({}x{})",
                               static_cast<int>(dataset.getTypeClass()), dataspace.getSimpleExtentNdims(), dims_out[0],
                               dims_out[1])
                << std::endl;
    }

    {
      H5::DataSet dataset = h5file_->openDataSet("test");
      H5::DataSpace dataspace = dataset.getSpace();

      hsize_t dims_out[2] = {0};
      dataspace.getSimpleExtentDims(dims_out, nullptr);
      test_row_count_ = dims_out[0];
      std::cout << fmt::format("dataset test data_type({}) rank({}) dimensions({}x{})",
                               static_cast<int>(dataset.getTypeClass()), dataspace.getSimpleExtentNdims(), dims_out[0],
                               dims_out[1])
                << std::endl;
    }

    {
      H5::DataSet dataset = h5file_->openDataSet("neighbors");
      H5::DataSpace dataspace = dataset.getSpace();

      hsize_t dims_out[2] = {0};
      dataspace.getSimpleExtentDims(dims_out, nullptr);
      std::cout << fmt::format("dataset neighbors data_type({}) rank({}) dimensions({}x{})",
                               static_cast<int>(dataset.getTypeClass()), dataspace.getSimpleExtentNdims(), dims_out[0],
                               dims_out[1])
                << std::endl;
    }

    {
      H5::DataSet dataset = h5file_->openDataSet("distances");
      H5::DataSpace dataspace = dataset.getSpace();

      hsize_t dims_out[2] = {0};
      dataspace.getSimpleExtentDims(dims_out, nullptr);
      std::cout << fmt::format("dataset distances data_type({}) rank({}) dimensions({}x{})",
                               static_cast<int>(dataset.getTypeClass()), dataspace.getSimpleExtentNdims(), dims_out[0],
                               dims_out[1])
                << std::endl;
    }

    return true;
  } catch (H5::FileIException& error) {
    error.printErrorStack();
  } catch (H5::DataSetIException& error) {
    error.printErrorStack();
  } catch (H5::DataSpaceIException& error) {
    error.printErrorStack();
  } catch (H5::DataTypeIException& error) {
    error.printErrorStack();
  } catch (std::exception& e) {
    std::cerr << "dataset init failed, " << e.what();
  }

  return false;
}

uint32_t BaseDataset::GetDimension() const { return dimension_; }
uint32_t BaseDataset::GetTrainDataCount() const { return train_row_count_; }
uint32_t BaseDataset::GetTestDataCount() const { return test_row_count_; }

void BaseDataset::GetBatchTrainData(uint32_t batch_num, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) {
  std::lock_guard lock(mutex_);

  uint64_t start_time = dingodb::Helper::TimestampUs();

  is_eof = false;
  H5::DataSet dataset = h5file_->openDataSet("train");
  H5::DataSpace dataspace = dataset.getSpace();

  int rank = dataspace.getSimpleExtentNdims();
  hsize_t dims_out[2] = {0};
  dataspace.getSimpleExtentDims(dims_out, nullptr);
  uint32_t row_count = dims_out[0];
  uint32_t dimension = dims_out[1];
  uint32_t row_offset = batch_num * FLAGS_vector_put_batch_size;
  if (row_offset >= row_count) {
    dataspace.close();
    dataset.close();
    is_eof = true;
    return;
  }

  uint32_t batch_size =
      (row_offset + FLAGS_vector_put_batch_size) <= row_count ? FLAGS_vector_put_batch_size : (row_count - row_offset);
  is_eof = batch_size < FLAGS_vector_put_batch_size;

  hsize_t file_offset[2] = {row_offset, 0};
  hsize_t file_count[2] = {batch_size, dimension};
  dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

  H5::DataSpace memspace(rank, dims_out);
  hsize_t mem_offset[2] = {0, 0};
  hsize_t mem_count[2] = {batch_size, dimension};
  memspace.selectHyperslab(H5S_SELECT_SET, mem_count, mem_offset);

  std::vector<float> buf;
  buf.resize(batch_size * dimension);
  dataset.read(buf.data(), H5::PredType::NATIVE_FLOAT, memspace, dataspace);

  // generate VectorWithId
  for (int i = 0; i < batch_size; ++i) {
    sdk::VectorWithId vector_with_id;
    vector_with_id.id = row_offset + i + 1;
    vector_with_id.vector.dimension = dimension;
    vector_with_id.vector.value_type = sdk::ValueType::kFloat;

    std::vector<float> vec;
    vec.resize(dimension);
    memcpy(vec.data(), buf.data() + i * dimension, dimension * sizeof(float));
    vector_with_id.vector.float_values.swap(vec);

    vector_with_ids.push_back(std::move(vector_with_id));
  }

  memspace.close();
  dataspace.close();
  dataset.close();

  std::cout << fmt::format("elapsed time: {} us", dingodb::Helper::TimestampUs() - start_time) << std::endl;
}

template <typename T>
static void PrintVector(const std::vector<T>& vec) {
  for (const auto& v : vec) {
    std::cout << v << " ";
  }

  std::cout << std::endl;
}

std::vector<BaseDataset::TestEntryPtr> BaseDataset::GetTestData() {
  std::lock_guard lock(mutex_);

  H5::DataSet dataset = h5file_->openDataSet("test");
  H5::DataSpace dataspace = dataset.getSpace();

  int rank = dataspace.getSimpleExtentNdims();
  hsize_t dims_out[2] = {0};
  dataspace.getSimpleExtentDims(dims_out, nullptr);
  uint32_t row_count = dims_out[0];
  uint32_t dimension = dims_out[1];

  hsize_t file_offset[2] = {0, 0};
  hsize_t file_count[2] = {row_count, dimension};
  dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

  H5::DataSpace memspace(rank, dims_out);
  hsize_t mem_offset[2] = {0, 0};
  hsize_t mem_count[2] = {row_count, dimension};
  memspace.selectHyperslab(H5S_SELECT_SET, mem_count, mem_offset);

  std::vector<float> buf;
  buf.resize(row_count * dimension);
  dataset.read(buf.data(), H5::PredType::NATIVE_FLOAT, memspace, dataspace);

  // gernerate test entries
  std::vector<BaseDataset::TestEntryPtr> test_entries;
  for (int i = 0; i < row_count; ++i) {
    auto test_entry = std::make_shared<BaseDataset::TestEntry>();
    test_entry->vector_with_id.id = 0;
    test_entry->vector_with_id.vector.dimension = dimension;
    test_entry->vector_with_id.vector.value_type = sdk::ValueType::kFloat;

    std::vector<float> vec;
    vec.resize(dimension);
    memcpy(vec.data(), buf.data() + i * dimension, dimension * sizeof(float));
    test_entry->vector_with_id.vector.float_values.swap(vec);

    test_entry->neighbors = GetTestVectorNeighbors(i);

    test_entries.push_back(test_entry);
  }

  memspace.close();
  dataspace.close();
  dataset.close();

  return test_entries;
}

std::unordered_map<int64_t, float> BaseDataset::GetTestVectorNeighbors(uint32_t index) {
  auto vector_ids = GetNeighbors(index);
  auto distances = GetDistances(index);
  assert(vector_ids.size() == distances.size());

  uint32_t size = std::min(static_cast<uint32_t>(vector_ids.size()), FLAGS_vector_search_topk);
  std::unordered_map<int64_t, float> neighbors;
  for (uint32_t i = 0; i < size; ++i) {
    neighbors.insert(std::make_pair(vector_ids[i] + 1, distances[i]));
  }

  return neighbors;
}

std::vector<int> BaseDataset::GetNeighbors(uint32_t index) {
  H5::DataSet dataset = h5file_->openDataSet("neighbors");
  H5::DataSpace dataspace = dataset.getSpace();

  int rank = dataspace.getSimpleExtentNdims();
  hsize_t dims_out[2] = {0};
  dataspace.getSimpleExtentDims(dims_out, nullptr);
  uint32_t row_count = dims_out[0];
  uint32_t dimension = dims_out[1];

  hsize_t file_offset[2] = {index, 0};
  hsize_t file_count[2] = {1, dimension};
  dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

  H5::DataSpace memspace(rank, dims_out);
  hsize_t mem_offset[2] = {0, 0};
  hsize_t mem_count[2] = {1, dimension};
  memspace.selectHyperslab(H5S_SELECT_SET, mem_count, mem_offset);

  std::vector<int> buf;
  buf.resize(dimension);
  dataset.read(buf.data(), H5::PredType::NATIVE_INT32, memspace, dataspace);

  memspace.close();
  dataspace.close();
  dataset.close();

  return buf;
}

std::vector<float> BaseDataset::GetDistances(uint32_t index) {
  H5::DataSet dataset = h5file_->openDataSet("distances");
  H5::DataSpace dataspace = dataset.getSpace();

  int rank = dataspace.getSimpleExtentNdims();
  hsize_t dims_out[2] = {0};
  dataspace.getSimpleExtentDims(dims_out, nullptr);
  uint32_t row_count = dims_out[0];
  uint32_t dimension = dims_out[1];

  hsize_t file_offset[2] = {index, 0};
  hsize_t file_count[2] = {1, dimension};
  dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

  H5::DataSpace memspace(rank, dims_out);
  hsize_t mem_offset[2] = {0, 0};
  hsize_t mem_count[2] = {1, dimension};
  memspace.selectHyperslab(H5S_SELECT_SET, mem_count, mem_offset);

  std::vector<float> buf;
  buf.resize(dimension);
  dataset.read(buf.data(), H5::PredType::NATIVE_FLOAT, memspace, dataspace);

  memspace.close();
  dataspace.close();
  dataset.close();

  return buf;
}

void DatasetUtils::SplitDataset(const std::string& filepath, uint32_t data_num) {
  std::ifstream ifs(filepath);
  rapidjson::IStreamWrapper isw(ifs);
  rapidjson::Document doc;
  doc.ParseStream(isw);
  if (doc.HasParseError()) {
    DINGO_LOG(ERROR) << fmt::format("parse json file {} failed, error: {}", filepath,
                                    static_cast<int>(doc.GetParseError()));
    return;
  }

  rapidjson::Document left_doc, right_doc;
  left_doc.SetArray();
  right_doc.SetArray();
  rapidjson::Document::AllocatorType& left_allocator = left_doc.GetAllocator();
  rapidjson::Document::AllocatorType& right_allocator = right_doc.GetAllocator();

  const auto& array = doc.GetArray();
  std::cout << fmt::format("filepath: {} count: {}", filepath, array.Size());
  for (int i = 0; i < array.Size(); ++i) {
    const auto& item = array[i].GetObject();

    if (i < data_num) {
      left_doc.PushBack(item, left_allocator);
    } else {
      right_doc.PushBack(item, right_allocator);
    }
  }

  {
    rapidjson::StringBuffer str_buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);
    left_doc.Accept(writer);
    dingodb::Helper::SaveFile(filepath + ".left", str_buf.GetString());
    left_doc.Clear();
  }

  {
    rapidjson::StringBuffer str_buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);
    right_doc.Accept(writer);
    dingodb::Helper::SaveFile(filepath + ".right", str_buf.GetString());
    right_doc.Clear();
  }
}

struct VectorEntry {
  VectorEntry() = default;
  VectorEntry(VectorEntry& entry) noexcept {
    this->id = entry.id;
    this->emb = entry.emb;
  }
  VectorEntry(VectorEntry&& entry) noexcept {
    this->id = entry.id;
    this->emb.swap(entry.emb);
  }

  struct Neighbor {
    int64_t id;
    float distance;

    bool operator()(const Neighbor& lhs, const Neighbor& rhs) { return lhs.distance < rhs.distance; }
  };
  int64_t id;
  std::vector<float> emb;

  // max heap, remain top k min distance
  std::priority_queue<Neighbor, std::vector<Neighbor>, Neighbor> max_heap;
  std::mutex mutex;

  std::vector<Neighbor> neighbors;

  void MakeNeighbors(const VectorEntry& vector_entry) {
    CHECK(emb.size() == vector_entry.emb.size());

    float distance = dingodb::Helper::DingoHnswL2Sqr(emb.data(), vector_entry.emb.data(), emb.size());

    Neighbor neighbor;
    neighbor.id = vector_entry.id;
    neighbor.distance = distance;
    InsertHeap(neighbor);
  }

  void InsertHeap(const Neighbor& neighbor) {
    std::lock_guard lock(mutex);

    const int nearest_neighbor_num = 100;
    if (max_heap.size() < nearest_neighbor_num) {
      max_heap.push(neighbor);
    } else {
      const auto& max_neighbor = max_heap.top();
      if (neighbor.distance < max_neighbor.distance) {
        max_heap.pop();
        max_heap.push(neighbor);
      }
    }
  }

  void SaveNeighbors() {
    std::lock_guard lock(mutex);

    while (!max_heap.empty()) {
      const auto& max_neighbor = max_heap.top();
      neighbors.push_back(max_neighbor);
      max_heap.pop();
    }

    std::sort(neighbors.begin(), neighbors.end(), Neighbor());
  }

  void PrintNeighbors() {
    for (auto& neighbor : neighbors) {
      std::cout << fmt::format("{} {}", neighbor.id, neighbor.distance) << std::endl;
    }
  }
};

static void SaveTestDatasetNeighbor(std::shared_ptr<rapidjson::Document> doc, std::vector<VectorEntry>& test_entries,
                                    const std::string& out_filepath) {
  rapidjson::Document out_doc;
  out_doc.SetArray();
  rapidjson::Document::AllocatorType& allocator = out_doc.GetAllocator();

  const auto& array = doc->GetArray();
  for (int i = 0; i < array.Size(); ++i) {
    auto out_obj = array[i].GetObject();
    auto& test_entry = test_entries[i];
    test_entry.SaveNeighbors();

    rapidjson::Value neighbors(rapidjson::kArrayType);
    for (const auto& neighbor : test_entry.neighbors) {
      rapidjson::Value obj(rapidjson::kObjectType);
      obj.AddMember("id", neighbor.id, allocator);
      obj.AddMember("distance", neighbor.distance, allocator);

      neighbors.PushBack(obj, allocator);
    }

    if (out_obj.HasMember("neighbors")) {
      out_obj.EraseMember("neighbors");
    }
    out_obj.AddMember("neighbors", neighbors, allocator);
    out_doc.PushBack(out_obj, allocator);
  }

  rapidjson::StringBuffer str_buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);
  out_doc.Accept(writer);
  dingodb::Helper::SaveFile(out_filepath, str_buf.GetString());
}

void DatasetUtils::GenTestDataset(const std::string& dataset_name, const std::string& test_dataset_filepath,
                                  const std::string& train_dataset_dirpath) {
  std::vector<VectorEntry> test_entries;

  auto get_id_func = [&](const rapidjson::Value& obj) -> int64_t {
    if (dataset_name == "wikipedia") {
      return obj["id"].GetInt64();
    } else if (dataset_name == "beir-bioasq") {
      return std::stoll(obj["_id"].GetString());
    }

    return 0;
  };

  // bootstrap thread pool
  dingodb::ThreadPool thread_pool("distance", 8);

  // load test data
  auto test_doc = std::make_shared<rapidjson::Document>();
  {
    std::ifstream ifs(test_dataset_filepath);
    rapidjson::IStreamWrapper isw(ifs);
    test_doc->ParseStream(isw);

    const auto& array = test_doc->GetArray();
    for (int i = 0; i < array.Size(); ++i) {
      const auto& item = array[i].GetObject();

      VectorEntry entry;
      entry.id = get_id_func(item);

      if (item["emb"].IsArray()) {
        entry.emb.reserve(item["emb"].GetArray().Size());
        for (auto& f : item["emb"].GetArray()) {
          entry.emb.push_back(f.GetFloat());
        }
      }

      CHECK(entry.emb.size() == FLAGS_vector_dimension)
          << fmt::format("dataset dimension({}) is not uniformity.", entry.emb.size());
      test_entries.push_back(std::move(entry));
    }

    std::cout << fmt::format("test data count: {}", test_entries.size()) << std::endl;
  }

  // load train data
  {
    std::vector<std::string> train_filepaths;
    auto train_filenames = dingodb::Helper::TraverseDirectory(train_dataset_dirpath, std::string("train"));
    train_filepaths.reserve(train_filenames.size());
    for (auto& filelname : train_filenames) {
      train_filepaths.push_back(fmt::format("{}/{}", train_dataset_dirpath, filelname));
    }
    std::cout << fmt::format("file count: {}", train_filepaths.size()) << std::endl;

    for (auto& train_filepath : train_filepaths) {
      std::ifstream ifs(train_filepath);
      rapidjson::IStreamWrapper isw(ifs);
      rapidjson::Document doc;
      doc.ParseStream(isw);
      if (doc.HasParseError()) {
        DINGO_LOG(ERROR) << fmt::format("parse json file {} failed, error: {}", train_filepath,
                                        static_cast<int>(doc.GetParseError()));
      }

      CHECK(doc.IsArray());
      const auto& array = doc.GetArray();
      std::cout << fmt::format("train file: {} count: {}", train_filepath, array.Size()) << std::endl;
      for (int i = 0; i < array.Size(); ++i) {
        const auto& item = array[i].GetObject();
        if (!item.HasMember("id") || !item.HasMember("emb")) {
          continue;
        }

        auto* entry = new VectorEntry();
        entry->id = get_id_func(item);

        if (!item["emb"].IsArray()) {
          continue;
        }

        entry->emb.reserve(item["emb"].GetArray().Size());
        for (auto& f : item["emb"].GetArray()) {
          entry->emb.push_back(f.GetFloat());
        }
        CHECK(entry->emb.size() == FLAGS_vector_dimension)
            << fmt::format("dataset dimension({}) is not uniformity.", entry->emb.size());

        thread_pool.ExecuteTask(
            [&test_entries](void* arg) {
              VectorEntry* train_entry = static_cast<VectorEntry*>(arg);

              for (auto& entry : test_entries) {
                entry.MakeNeighbors(*train_entry);
              }
            },
            entry);

        while (thread_pool.PendingTaskCount() > 1000) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      }
    }
  }

  // waiting finish
  while (thread_pool.PendingTaskCount() > 1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // handle result
  SaveTestDatasetNeighbor(test_doc, test_entries, test_dataset_filepath + ".neighbor");
}

void DatasetUtils::Test() {
  // Wikipedia2212Dataset dataset("/root/work/dingodb/dataset/wikipedia-22-12-zh");
  // if (!dataset.Init()) {
  //   return;
  // }

  // uint64_t count = 0;
  // std::vector<sdk::VectorWithId> vector_with_ids;
  // vector_with_ids.reserve(1000);
  // for (int i = 0; i < 100000000; ++i) {
  //   vector_with_ids.clear();
  //   bool is_eof = false;
  //   dataset.GetBatchTrainData(i, vector_with_ids, is_eof);
  //   if (!vector_with_ids.empty()) {
  //     if (++count % 1000 == 0) {
  //       std::cout << fmt::format("count: {}", count) << std::endl;
  //     }
  //   }
  //   if (is_eof) {
  //     break;
  //   }
  // }

  // std::this_thread::sleep_for(std::chrono::seconds(100000));

  // wikipedia dataset
  // DatasetUtils::SplitDataset("/root/work/dingodb/dataset/wikipedia-22-12-zh/train-00000-of-00017-20a9866562d051fb.json",
  //                            1000);

  DatasetUtils::GenTestDataset(
      "wikipedia", "/root/work/dingodb/dataset/wikipedia-22-12-zh/test-00000-of-00017-20a9866562d051fb.json",
      "/root/work/dingodb/dataset/wikipedia-22-12-zh/");

  // beir bioasq
  // DatasetUtils::SplitDataset("/root/work/dingodb/dataset/beir-bioasq/0000-0.json", 1000);

  // DatasetUtils::GenTestDataset("wikipedia", "/root/work/dingodb/dataset/beir-bioasq/test-0000-0.json",
  //                              "/root/work/dingodb/dataset/beir-bioasq");
}

bool Wikipedia2212Dataset::Init() {
  std::lock_guard lock(mutex_);

  if (!FLAGS_vector_search_not_insert) {
    auto train_filenames = dingodb::Helper::TraverseDirectory(dirpath_, std::string("train"));
    for (auto& filelname : train_filenames) {
      train_filepaths_.push_back(fmt::format("{}/{}", dirpath_, filelname));
    }

    if (train_filepaths_.empty()) {
      return false;
    }

    batch_vector_entry_cache_.resize(FLAGS_batch_vector_entry_cache_size);
    auto train_thread = std::thread([&] { ParallelLoadTrainData(train_filepaths_); });
    train_thread.detach();
  }

  auto test_filenames = dingodb::Helper::TraverseDirectory(dirpath_, std::string("test"));
  for (auto& filelname : test_filenames) {
    test_filepaths_.push_back(fmt::format("{}/{}", dirpath_, filelname));
  }

  return !test_filepaths_.empty();
}

uint32_t Wikipedia2212Dataset::GetDimension() const { return FLAGS_vector_dimension; }

uint32_t Wikipedia2212Dataset::GetTrainDataCount() const { return 0; }

uint32_t Wikipedia2212Dataset::GetTestDataCount() const { return test_row_count_; }

void Wikipedia2212Dataset::ParallelLoadTrainData(const std::vector<std::string>& filepaths) {
  uint64_t start_time = dingodb::Helper::TimestampMs();

  std::atomic_int curr_file_pos = 0;
  std::vector<std::thread> threads;
  for (size_t thread_id = 0; thread_id < FLAGS_load_vector_dataset_concurrency; ++thread_id) {
    threads.push_back(std::thread([&, thread_id] {
      for (;;) {
        int file_pos = curr_file_pos.fetch_add(1);
        if (file_pos >= filepaths.size()) {
          return;
        }

        std::ifstream ifs(filepaths[file_pos]);
        rapidjson::IStreamWrapper isw(ifs);
        auto doc = std::make_shared<rapidjson::Document>();
        doc->ParseStream(isw);
        LOG_IF(ERROR, doc->HasParseError()) << fmt::format("parse json file {} failed, error: {}", filepaths[file_pos],
                                                           static_cast<int>(doc->GetParseError()));

        uint32_t offset = 0;
        int64_t batch_vector_count = 0;
        int64_t vector_count = 0;
        for (;;) {
          ++batch_vector_count;
          auto batch_vector_entry = std::make_shared<BatchVectorEntry>();
          batch_vector_entry->vector_with_ids.reserve(FLAGS_vector_put_batch_size);
          offset = LoadTrainData(doc, offset, FLAGS_vector_put_batch_size, batch_vector_entry->vector_with_ids);
          vector_count += batch_vector_entry->vector_with_ids.size();
          if (batch_vector_entry->vector_with_ids.empty()) {
            break;
          }

          for (;;) {
            bool is_full = false;
            {
              std::lock_guard lock(mutex_);
              // check is full
              if ((head_pos_ + 1) % FLAGS_batch_vector_entry_cache_size == tail_pos_) {
                is_full = true;
              } else {
                batch_vector_entry_cache_[tail_pos_] = batch_vector_entry;
                tail_pos_ = (tail_pos_ + 1) % FLAGS_batch_vector_entry_cache_size;
                break;
              }
            }

            if (is_full) {
              std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
          }
        }

        LOG(INFO) << fmt::format("filepath: {} file_pos: {} batch_vector_count: {} vector_count: {}",
                                 filepaths[file_pos], file_pos, batch_vector_count, vector_count);
      }
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  train_load_finish_.store(true);

  LOG(INFO) << fmt::format("Parallel load train data elapsed time: {} ms", dingodb::Helper::TimestampMs() - start_time);
}

uint32_t Wikipedia2212Dataset::LoadTrainData(std::shared_ptr<rapidjson::Document> doc, uint32_t offset, uint32_t size,
                                             std::vector<sdk::VectorWithId>& vector_with_ids) const {
  CHECK(doc != nullptr);
  CHECK(doc->IsArray());

  const auto& array = doc->GetArray();
  while (offset < array.Size()) {
    const auto& item = array[offset++].GetObject();
    if (!item.HasMember("id") || !item.HasMember("title") || !item.HasMember("text") || !item.HasMember("url") ||
        !item.HasMember("wiki_id") || !item.HasMember("views") || !item.HasMember("paragraph_id") ||
        !item.HasMember("langs") || !item.HasMember("emb")) {
      continue;
    }

    std::vector<float> embedding;
    if (item["emb"].IsArray()) {
      uint32_t dimension = item["emb"].GetArray().Size();
      CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.", dimension);
      embedding.reserve(dimension);
      for (auto& f : item["emb"].GetArray()) {
        embedding.push_back(f.GetFloat());
      }
    }

    sdk::VectorWithId vector_with_id;
    vector_with_id.id = item["id"].GetInt64() + 1;

    vector_with_id.vector.value_type = sdk::ValueType::kFloat;
    vector_with_id.vector.float_values.swap(embedding);

    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kInt64;
      sdk::ScalarField field;
      field.long_data = item["id"].GetInt64() + 1;
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["id"] = scalar_value;
    }

    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kInt64;
      sdk::ScalarField field;
      field.long_data = item["wiki_id"].GetInt64();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["wiki_id"] = scalar_value;
    }

    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kInt64;
      sdk::ScalarField field;
      field.long_data = item["paragraph_id"].GetInt64();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["paragraph_id"] = scalar_value;
    }

    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kInt64;
      sdk::ScalarField field;
      field.long_data = item["langs"].GetInt64();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["langs"] = scalar_value;
    }

    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kString;
      sdk::ScalarField field;
      field.string_data = item["title"].GetString();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["title"] = scalar_value;
    }
    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kString;
      sdk::ScalarField field;
      field.string_data = item["url"].GetString();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["url"] = scalar_value;
    }
    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kString;
      sdk::ScalarField field;
      field.string_data = item["text"].GetString();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["text"] = scalar_value;
    }

    vector_with_ids.push_back(vector_with_id);
    if (vector_with_ids.size() >= size) {
      break;
    }
  }

  return offset;
}

void Wikipedia2212Dataset::GetBatchTrainData(uint32_t, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) {
  is_eof = false;
  if (train_filepaths_.empty()) {
    return;
  }

  for (;;) {
    bool is_empty = false;

    {
      std::lock_guard lock(mutex_);

      if (head_pos_ == tail_pos_) {
        is_empty = true;
        is_eof = train_load_finish_.load();
        if (is_eof) {
          return;
        }
      } else {
        auto batch_vector_entry = batch_vector_entry_cache_[head_pos_];
        head_pos_ = (head_pos_ + 1) % FLAGS_batch_vector_entry_cache_size;

        for (auto& vector_with_id : batch_vector_entry->vector_with_ids) {
          vector_with_ids.push_back(std::move(vector_with_id));
        }
        return;
      }
    }

    if (is_empty) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
}

std::vector<Dataset::TestEntryPtr> Wikipedia2212Dataset::GetTestData() {
  std::vector<Dataset::TestEntryPtr> test_entries;
  for (auto& test_filepath : test_filepaths_) {
    std::ifstream ifs(test_filepath);
    rapidjson::IStreamWrapper isw(ifs);
    rapidjson::Document doc;
    doc.ParseStream(isw);
    if (doc.HasParseError()) {
      DINGO_LOG(ERROR) << fmt::format("parse json file {} failed, error: {}", test_filepath,
                                      static_cast<int>(doc.GetParseError()));
      continue;
    }

    const auto& array = doc.GetArray();
    test_row_count_ += array.Size();
    for (int i = 0; i < array.Size(); ++i) {
      const auto& item = array[i].GetObject();

      sdk::VectorWithId vector_with_id;
      vector_with_id.id = item["id"].GetInt64() + 1;

      std::vector<float> embedding;
      if (item["emb"].IsArray()) {
        uint32_t dimension = item["emb"].GetArray().Size();
        CHECK(dimension == FLAGS_vector_dimension)
            << fmt::format("dataset dimension({}) is not uniformity.", dimension);
        embedding.reserve(dimension);
        for (auto& f : item["emb"].GetArray()) {
          embedding.push_back(f.GetFloat());
        }
      }

      vector_with_id.vector.value_type = sdk::ValueType::kFloat;
      vector_with_id.vector.float_values.swap(embedding);

      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kInt64;
      //   sdk::ScalarField field;
      //   field.long_data = item["id"].GetInt64() + 1;
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["id"] = scalar_value;
      // }

      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kInt64;
      //   sdk::ScalarField field;
      //   field.long_data = item["wiki_id"].GetInt64();
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["wiki_id"] = scalar_value;
      // }

      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kInt64;
      //   sdk::ScalarField field;
      //   field.long_data = item["paragraph_id"].GetInt64();
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["paragraph_id"] = scalar_value;
      // }

      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kInt64;
      //   sdk::ScalarField field;
      //   field.long_data = item["langs"].GetInt64();
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["langs"] = scalar_value;
      // }

      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kString;
      //   sdk::ScalarField field;
      //   field.string_data = item["title"].GetString();
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["title"] = scalar_value;
      // }
      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kString;
      //   sdk::ScalarField field;
      //   field.string_data = item["url"].GetString();
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["url"] = scalar_value;
      // }
      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kString;
      //   sdk::ScalarField field;
      //   field.string_data = item["text"].GetString();
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["text"] = scalar_value;
      // }

      Dataset::TestEntryPtr entry = std::make_shared<Dataset::TestEntry>();
      entry->vector_with_id = vector_with_id;

      const auto& neighbors = item["neighbors"].GetArray();
      uint32_t size = std::min(static_cast<uint32_t>(neighbors.Size()), FLAGS_vector_search_topk);
      for (uint32_t i = 0; i < size; ++i) {
        const auto& neighbor_obj = neighbors[i].GetObject();

        CHECK(neighbor_obj["id"].IsInt64()) << "id type is not int64_t.";
        CHECK(neighbor_obj["distance"].IsFloat()) << "distance type is not float.";
        entry->neighbors[neighbor_obj["id"].GetInt64() + 1] = neighbor_obj["distance"].GetFloat();
      }

      test_entries.push_back(entry);
    }
  }

  return test_entries;
}

bool BeirBioasqDataset::Init() {
  std::lock_guard lock(mutex_);

  if (!FLAGS_vector_search_not_insert) {
    auto train_filenames = dingodb::Helper::TraverseDirectory(dirpath_, std::string("train"));
    for (auto& filelname : train_filenames) {
      train_filepaths_.push_back(fmt::format("{}/{}", dirpath_, filelname));
    }

    if (train_filepaths_.empty()) {
      return false;
    }

    batch_vector_entry_cache_.resize(FLAGS_batch_vector_entry_cache_size);
    auto train_thread = std::thread([&] { ParallelLoadTrainData(train_filepaths_); });
    train_thread.detach();
  }

  auto test_filenames = dingodb::Helper::TraverseDirectory(dirpath_, std::string("test"));
  for (auto& filelname : test_filenames) {
    test_filepaths_.push_back(fmt::format("{}/{}", dirpath_, filelname));
  }

  return !test_filepaths_.empty();
}

uint32_t BeirBioasqDataset::GetDimension() const { return FLAGS_vector_dimension; }

uint32_t BeirBioasqDataset::GetTrainDataCount() const { return 0; }

uint32_t BeirBioasqDataset::GetTestDataCount() const { return test_row_count_; }

void BeirBioasqDataset::ParallelLoadTrainData(const std::vector<std::string>& filepaths) {
  uint64_t start_time = dingodb::Helper::TimestampMs();

  std::atomic_int curr_file_pos = 0;
  std::vector<std::thread> threads;
  for (size_t thread_id = 0; thread_id < FLAGS_load_vector_dataset_concurrency; ++thread_id) {
    threads.push_back(std::thread([&, thread_id] {
      for (;;) {
        int file_pos = curr_file_pos.fetch_add(1);
        if (file_pos >= filepaths.size()) {
          return;
        }

        std::ifstream ifs(filepaths[file_pos]);
        rapidjson::IStreamWrapper isw(ifs);
        auto doc = std::make_shared<rapidjson::Document>();
        doc->ParseStream(isw);
        LOG_IF(ERROR, doc->HasParseError()) << fmt::format("parse json file {} failed, error: {}", filepaths[file_pos],
                                                           static_cast<int>(doc->GetParseError()));

        uint32_t offset = 0;
        int64_t batch_vector_count = 0;
        int64_t vector_count = 0;
        for (;;) {
          ++batch_vector_count;
          auto batch_vector_entry = std::make_shared<BatchVectorEntry>();
          batch_vector_entry->vector_with_ids.reserve(FLAGS_vector_put_batch_size);
          offset = LoadTrainData(doc, offset, FLAGS_vector_put_batch_size, batch_vector_entry->vector_with_ids);
          vector_count += batch_vector_entry->vector_with_ids.size();
          if (batch_vector_entry->vector_with_ids.empty()) {
            break;
          }

          for (;;) {
            bool is_full = false;
            {
              std::lock_guard lock(mutex_);
              // check is full
              if ((head_pos_ + 1) % FLAGS_batch_vector_entry_cache_size == tail_pos_) {
                is_full = true;
              } else {
                batch_vector_entry_cache_[tail_pos_] = batch_vector_entry;
                tail_pos_ = (tail_pos_ + 1) % FLAGS_batch_vector_entry_cache_size;
                break;
              }
            }

            if (is_full) {
              std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
          }
        }

        LOG(INFO) << fmt::format("filepath: {} file_pos: {} batch_vector_count: {} vector_count: {}",
                                 filepaths[file_pos], file_pos, batch_vector_count, vector_count);
      }
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  train_load_finish_.store(true);

  LOG(INFO) << fmt::format("Parallel load train data elapsed time: {} ms", dingodb::Helper::TimestampMs() - start_time);
}

uint32_t BeirBioasqDataset::LoadTrainData(std::shared_ptr<rapidjson::Document> doc, uint32_t offset, uint32_t size,
                                          std::vector<sdk::VectorWithId>& vector_with_ids) const {
  CHECK(doc != nullptr);
  CHECK(doc->IsArray());

  const auto& array = doc->GetArray();
  while (offset < array.Size()) {
    const auto& item = array[offset++].GetObject();
    if (!item.HasMember("_id") || !item.HasMember("title") || !item.HasMember("text") || !item.HasMember("emb")) {
      continue;
    }

    std::vector<float> embedding;
    if (item["emb"].IsArray()) {
      uint32_t dimension = item["emb"].GetArray().Size();
      CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.", dimension);
      embedding.reserve(dimension);
      for (auto& f : item["emb"].GetArray()) {
        embedding.push_back(f.GetFloat());
      }
    }

    sdk::VectorWithId vector_with_id;
    vector_with_id.id = std::stoll(item["_id"].GetString()) + 1;

    vector_with_id.vector.value_type = sdk::ValueType::kFloat;
    vector_with_id.vector.float_values.swap(embedding);

    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kInt64;
      sdk::ScalarField field;
      field.long_data = std::stoll(item["_id"].GetString()) + 1;
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["id"] = scalar_value;
    }

    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kString;
      sdk::ScalarField field;
      field.string_data = item["title"].GetString();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["title"] = scalar_value;
    }

    {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::ScalarFieldType::kString;
      sdk::ScalarField field;
      field.string_data = item["text"].GetString();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["text"] = scalar_value;
    }

    vector_with_ids.push_back(vector_with_id);
    if (vector_with_ids.size() >= size) {
      break;
    }
  }

  return offset;
}

void BeirBioasqDataset::GetBatchTrainData(uint32_t, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) {
  is_eof = false;
  if (train_filepaths_.empty()) {
    return;
  }

  for (;;) {
    bool is_empty = false;

    {
      std::lock_guard lock(mutex_);

      if (head_pos_ == tail_pos_) {
        is_empty = true;
        is_eof = train_load_finish_.load();
        if (is_eof) {
          return;
        }
      } else {
        auto batch_vector_entry = batch_vector_entry_cache_[head_pos_];
        head_pos_ = (head_pos_ + 1) % FLAGS_batch_vector_entry_cache_size;

        for (auto& vector_with_id : batch_vector_entry->vector_with_ids) {
          vector_with_ids.push_back(std::move(vector_with_id));
        }
        return;
      }
    }

    if (is_empty) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
}

std::vector<Dataset::TestEntryPtr> BeirBioasqDataset::GetTestData() {
  std::lock_guard lock(mutex_);

  std::vector<Dataset::TestEntryPtr> test_entries;
  for (auto& test_filepath : test_filepaths_) {
    std::ifstream ifs(test_filepath);
    rapidjson::IStreamWrapper isw(ifs);
    rapidjson::Document doc;
    doc.ParseStream(isw);
    if (doc.HasParseError()) {
      DINGO_LOG(ERROR) << fmt::format("parse json file {} failed, error: {}", test_filepath,
                                      static_cast<int>(doc.GetParseError()));
      continue;
    }

    const auto& array = doc.GetArray();
    test_row_count_ += array.Size();
    for (int i = 0; i < array.Size(); ++i) {
      const auto& item = array[i].GetObject();

      sdk::VectorWithId vector_with_id;
      vector_with_id.id = std::stoll(item["_id"].GetString()) + 1;

      std::vector<float> embedding;
      if (item["emb"].IsArray()) {
        uint32_t dimension = item["emb"].GetArray().Size();
        CHECK(dimension == FLAGS_vector_dimension)
            << fmt::format("dataset dimension({}) is not uniformity.", dimension);
        embedding.reserve(dimension);
        for (auto& f : item["emb"].GetArray()) {
          embedding.push_back(f.GetFloat());
        }
      }

      vector_with_id.vector.value_type = sdk::ValueType::kFloat;
      vector_with_id.vector.float_values.swap(embedding);

      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kInt64;
      //   sdk::ScalarField field;
      //   field.long_data = std::stoll(item["_id"].GetString()) + 1;
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["id"] = scalar_value;
      // }

      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kString;
      //   sdk::ScalarField field;
      //   field.string_data = item["title"].GetString();
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["title"] = scalar_value;
      // }

      // {
      //   sdk::ScalarValue scalar_value;
      //   scalar_value.type = sdk::ScalarFieldType::kString;
      //   sdk::ScalarField field;
      //   field.string_data = item["text"].GetString();
      //   scalar_value.fields.push_back(field);
      //   vector_with_id.scalar_data["text"] = scalar_value;
      // }

      Dataset::TestEntryPtr entry = std::make_shared<Dataset::TestEntry>();
      entry->vector_with_id = vector_with_id;

      const auto& neighbors = item["neighbors"].GetArray();
      uint32_t size = std::min(static_cast<uint32_t>(neighbors.Size()), FLAGS_vector_search_topk);
      for (uint32_t i = 0; i < size; ++i) {
        const auto& neighbor_obj = neighbors[i].GetObject();

        CHECK(neighbor_obj["id"].IsInt64()) << "id type is not int64_t.";
        CHECK(neighbor_obj["distance"].IsFloat()) << "distance type is not float.";
        entry->neighbors[neighbor_obj["id"].GetInt64() + 1] = neighbor_obj["distance"].GetFloat();
      }

      test_entries.push_back(entry);
    }
  }

  return test_entries;
}

}  // namespace benchmark
}  // namespace dingodb