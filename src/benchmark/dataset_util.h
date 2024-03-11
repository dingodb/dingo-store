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

#ifndef DINGODB_BENCHMARK_DATASET_UTIL_H_
#define DINGODB_BENCHMARK_DATASET_UTIL_H_

#include <cstdint>
#include <string>
#include <vector>

namespace dingodb {
namespace benchmark {

// dataset utils
class DatasetUtils {
 public:
  static void Main();

  // make statistics distribution
  static void GetStatisticsDistribution(const std::string& dataset_name, const std::string& train_dataset_dirpath,
                                        const std::string& field, const std::string& out_filepath);
  // split dataset for generate test dataset
  static void SplitDataset(const std::string& filepath, uint32_t data_num);
  // generate test dataset neighbors
  static void GenNeighbor(const std::string& dataset_name, const std::string& test_dataset_filepath,
                          const std::string& train_dataset_dirpath, const std::string& out_filepath);
};

}  // namespace benchmark
}  // namespace dingodb

#endif  // DINGODB_BENCHMARK_DATASET_UTIL_H_
