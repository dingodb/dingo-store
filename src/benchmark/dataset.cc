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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "H5Cpp.h"
#include "H5PredType.h"
#include "common/helper.h"
#include "common/logging.h"
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

void Wikipedia2212Dataset::Test() {
  Wikipedia2212Dataset dataset("/root/work/dingodb/dataset/wikipedia-22-12-zh");
  if (!dataset.Init()) {
    return;
  }

  std::vector<sdk::VectorWithId> vector_with_ids;
  vector_with_ids.reserve(1000);
  for (int i = 0; i < 10000; ++i) {
    vector_with_ids.clear();
    bool is_eof = false;
    dataset.GetBatchTrainData(i, vector_with_ids, is_eof);
    if (is_eof) {
      break;
    }
  }
}

bool Wikipedia2212Dataset::Init() {
  std::lock_guard lock(mutex_);
  auto train_filenames = dingodb::Helper::TraverseDirectory(train_dataset_dir_, "train");
  for (auto& filelname : train_filenames) {
    train_filepaths_.push_back(fmt::format("{}/{}", train_dataset_dir_, filelname));
  }

  if (train_filepaths_.empty()) {
    return false;
  }

  MakeDocument(train_filepaths_[train_curr_file_pos_]);

  return InitDimension();
}

uint32_t Wikipedia2212Dataset::GetDimension() const { return dimension_; }

uint32_t Wikipedia2212Dataset::GetTrainDataCount() const { return 0; }

uint32_t Wikipedia2212Dataset::GetTestDataCount() const { return 100000; }

void Wikipedia2212Dataset::GetBatchTrainData(uint32_t, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) {
  std::lock_guard lock(mutex_);
  if (train_filepaths_.empty()) {
    return;
  }

  train_curr_offset_ = LoadTrainData(doc_, train_curr_offset_, FLAGS_vector_put_batch_size, vector_with_ids);
  // std::cout << fmt::format("{} {} {}", train_curr_file_pos_, train_curr_offset_, vector_with_ids.size()) <<
  // std::endl;
  if (vector_with_ids.size() == FLAGS_vector_put_batch_size) {
    return;
  }

  if (train_curr_file_pos_ + 1 >= train_filepaths_.size()) {
    // all file finish
    is_eof = true;
    return;
  }

  // next file
  MakeDocument(train_filepaths_[++train_curr_file_pos_]);
}

std::vector<Dataset::TestEntryPtr> Wikipedia2212Dataset::GetTestData() {
  std::lock_guard lock(mutex_);
  return {};
}

void Wikipedia2212Dataset::MakeDocument(const std::string& filepath) {
  std::ifstream ifs(filepath);
  rapidjson::IStreamWrapper isw(ifs);

  if (doc_ == nullptr) {
    doc_ = std::make_shared<rapidjson::Document>();
  } else {
    doc_.reset();
    doc_ = std::make_shared<rapidjson::Document>();
  }

  doc_->ParseStream(isw);
  if (doc_->HasParseError()) {
    DINGO_LOG(ERROR) << fmt::format("parse json file {} failed, error: {}", filepath,
                                    static_cast<int>(doc_->GetParseError()));
  }
  train_curr_offset_ = 0;
}

bool Wikipedia2212Dataset::InitDimension() {
  CHECK(doc_ != nullptr);

  const auto& array = doc_->GetArray();
  if (array.Empty()) {
    return false;
  }

  const auto& item = array[0].GetObject();
  dimension_ = item["emb"].GetArray().Size();
  return true;
}

uint32_t Wikipedia2212Dataset::LoadTrainData(std::shared_ptr<rapidjson::Document> doc, uint32_t offset, uint32_t size,
                                             std::vector<sdk::VectorWithId>& vector_with_ids) {
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
      CHECK(dimension == dimension_) << "dataset dimension is not uniformity.";
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

}  // namespace benchmark
}  // namespace dingodb