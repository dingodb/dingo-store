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

#ifndef DINGODB_BENCHMARK_DATASET_H_
#define DINGODB_BENCHMARK_DATASET_H_

#include <sys/types.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "H5Cpp.h"
#include "google/protobuf/stubs/port.h"
#include "rapidjson/document.h"
#include "sdk/vector.h"

namespace dingodb {
namespace benchmark {

// dataset abstraction class
class Dataset {
 public:
  Dataset() = default;
  virtual ~Dataset() = default;

  struct TestEntry {
    sdk::VectorWithId vector_with_id;
    // std::vector<Neighbor> neighbors;
    std::unordered_map<int64_t, float> neighbors;
  };
  using TestEntryPtr = std::shared_ptr<TestEntry>;

  static std::shared_ptr<Dataset> New(std::string filepath);

  virtual bool Init() = 0;

  virtual uint32_t GetDimension() const = 0;
  virtual uint32_t GetTrainDataCount() const = 0;
  virtual uint32_t GetTestDataCount() const = 0;

  // Get train data by batch
  virtual void GetBatchTrainData(uint32_t batch_num, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) = 0;

  // Get all test data
  virtual std::vector<TestEntryPtr> GetTestData() = 0;
};
using DatasetPtr = std::shared_ptr<Dataset>;

class BaseDataset : public Dataset {
 public:
  BaseDataset(std::string filepath);
  ~BaseDataset() override;

  bool Init() override;

  uint32_t GetDimension() const override;
  uint32_t GetTrainDataCount() const override;
  uint32_t GetTestDataCount() const override;

  // Get train data by batch
  void GetBatchTrainData(uint32_t batch_num, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) override;

  // Get all test data
  std::vector<TestEntryPtr> GetTestData() override;

 private:
  std::vector<int> GetNeighbors(uint32_t index);
  std::vector<float> GetDistances(uint32_t index);
  std::unordered_map<int64_t, float> GetTestVectorNeighbors(uint32_t index);

  std::string filepath_;
  std::shared_ptr<H5::H5File> h5file_;

  uint32_t train_row_count_{0};
  uint32_t test_row_count_{0};

  uint32_t dimension_{0};
  std::mutex mutex_;
};

// sift/glove/gist/mnist is same
class SiftDataset : public BaseDataset {
 public:
  SiftDataset(std::string filepath) : BaseDataset(filepath) {}
  ~SiftDataset() override = default;
};

class GloveDataset : public BaseDataset {
 public:
  GloveDataset(std::string filepath) : BaseDataset(filepath) {}
  ~GloveDataset() override = default;
};

class GistDataset : public BaseDataset {
 public:
  GistDataset(std::string filepath) : BaseDataset(filepath) {}
  ~GistDataset() override = default;
};

class KosarakDataset : public BaseDataset {
 public:
  KosarakDataset(std::string filepath) : BaseDataset(filepath) {}
  ~KosarakDataset() override = default;
};

class LastfmDataset : public BaseDataset {
 public:
  LastfmDataset(std::string filepath) : BaseDataset(filepath) {}
  ~LastfmDataset() override = default;
};

class MnistDataset : public BaseDataset {
 public:
  MnistDataset(std::string filepath) : BaseDataset(filepath) {}
  ~MnistDataset() override = default;
};

class Movielens10mDataset : public BaseDataset {
 public:
  Movielens10mDataset(std::string filepath) : BaseDataset(filepath) {}
  ~Movielens10mDataset() override = default;
};

class Wikipedia2212Dataset : public Dataset {
 public:
  Wikipedia2212Dataset(const std::string& train_dir) : train_dataset_dir_(train_dir) {}
  ~Wikipedia2212Dataset() override = default;

  bool Init() override;

  static void Test();

  uint32_t GetDimension() const override;
  uint32_t GetTrainDataCount() const override;
  uint32_t GetTestDataCount() const override;

  // Get train data by batch
  void GetBatchTrainData(uint32_t batch_num, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) override;

  // Get all test data
  std::vector<TestEntryPtr> GetTestData() override;

 private:
  void MakeDocument(const std::string& filepath);
  bool InitDimension();
  uint32_t LoadTrainData(std::shared_ptr<rapidjson::Document> doc, uint32_t offset, uint32_t size,
                         std::vector<sdk::VectorWithId>& vector_with_ids);

  std::string train_dataset_dir_;
  std::vector<std::string> train_filepaths_;
  // current using train file pos
  int train_curr_file_pos_{0};
  // current offset in file
  int train_curr_offset_{0};

  std::shared_ptr<rapidjson::Document> doc_;

  std::vector<std::string> test_filepaths_;

  uint32_t dimension_{0};
  std::mutex mutex_;
};

}  // namespace benchmark
}  // namespace dingodb

#endif  // DINGODB_BENCHMARK_DATASET_H_
