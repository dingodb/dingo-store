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
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "H5Cpp.h"
#include "H5PredType.h"
#include "fmt/core.h"
#include "gflags/gflags_declare.h"

DECLARE_uint32(vector_put_batch_size);

namespace dingodb {
namespace benchmark {

std::shared_ptr<Dataset> Dataset::New(std::string filepath) {
  if (filepath.find("sift") == std::string::npos) {
    return std::make_shared<SiftDataset>(filepath);

  } else if (filepath.find("glove") == std::string::npos) {
    return std::make_shared<GloveDataset>(filepath);

  } else if (filepath.find("gist") == std::string::npos) {
    return std::make_shared<GistDataset>(filepath);

  } else if (filepath.find("kosarak") == std::string::npos) {
    return std::make_shared<KosarakDataset>(filepath);

  } else if (filepath.find("lastfm") == std::string::npos) {
    return std::make_shared<LastfmDataset>(filepath);

  } else if (filepath.find("mnist") == std::string::npos) {
    return std::make_shared<MnistDataset>(filepath);

  } else if (filepath.find("movielens10m") == std::string::npos) {
    return std::make_shared<Movielens10mDataset>(filepath);
  }

  std::cout << "Not support dataset, path: " << filepath << std::endl;

  return nullptr;
}

BaseDataset::BaseDataset(std::string filepath) : filepath_(filepath) {}

BaseDataset::~BaseDataset() { h5file_->close(); }

bool BaseDataset::Init() {
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
  for (int i = 0; i < 3; ++i) {
    auto test_entry = std::make_shared<BaseDataset::TestEntry>();
    test_entry->vector_with_id.id = 0;
    test_entry->vector_with_id.vector.dimension = dimension;
    test_entry->vector_with_id.vector.value_type = sdk::ValueType::kFloat;

    std::vector<float> vec;
    vec.resize(dimension);
    memcpy(vec.data(), buf.data() + i * dimension, dimension * sizeof(float));
    test_entry->vector_with_id.vector.float_values.swap(vec);
    // PrintVector(test_entry->vector_with_id.vector.float_values);

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

  std::unordered_map<int64_t, float> neighbors;
  for (int i = 0; i < vector_ids.size(); ++i) {
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

}  // namespace benchmark
}  // namespace dingodb