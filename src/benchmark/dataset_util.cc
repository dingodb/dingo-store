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

#include "benchmark/dataset_util.h"

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "common/helper.h"
#include "common/logging.h"
#include "common/threadpool.h"
#include "fmt/core.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

DECLARE_string(vector_dataset);
DECLARE_uint32(vector_dimension);

DEFINE_string(sub_command, "", "sub command");
DEFINE_string(filter_field, "", "filter field");
DEFINE_string(filter_field_value, "", "filter field value");
DEFINE_bool(filter_field_reverse, false, "reverse filter field");

DEFINE_string(test_dataset_filepath, "", "test dataset filepath");

DEFINE_uint32(split_num, 1000, "spilt num");

namespace dingodb {
namespace benchmark {

static bool IsDigitString(const std::string& str) {
  for (const auto& c : str) {
    if (!std::isdigit(c)) {
      return false;
    }
  }

  return true;
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

    if (!FLAGS_filter_field.empty()) {
      rapidjson::Value obj(rapidjson::kObjectType);
      if (IsDigitString(FLAGS_filter_field_value)) {
        int64_t v = std::strtoll(FLAGS_filter_field_value.c_str(), nullptr, 10);
        obj.AddMember(rapidjson::StringRef(FLAGS_filter_field.c_str()), v, allocator);
      } else {
        obj.AddMember(rapidjson::StringRef(FLAGS_filter_field.c_str()),
                      rapidjson::StringRef(FLAGS_filter_field_value.c_str()), allocator);
      }
      out_obj.AddMember("filter", obj, allocator);
    }

    out_doc.PushBack(out_obj, allocator);
  }

  rapidjson::StringBuffer str_buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);
  out_doc.Accept(writer);
  dingodb::Helper::SaveFile(out_filepath, str_buf.GetString());
}

static bool FilterValue(const rapidjson::Value& obj) {
  if (FLAGS_filter_field.empty()) {
    return false;
  }
  if (obj[FLAGS_filter_field.c_str()].IsString()) {
    if (FLAGS_filter_field_reverse) {
      if (obj[FLAGS_filter_field.c_str()].GetString() != FLAGS_filter_field_value) {
        LOG(INFO) << fmt::format("filter value reverse: {} {}", obj[FLAGS_filter_field.c_str()].GetInt64(),
                                 FLAGS_filter_field_value);
        return true;
      }
    } else {
      if (obj[FLAGS_filter_field.c_str()].GetString() == FLAGS_filter_field_value) {
        LOG(INFO) << fmt::format("filter value: {} {}", obj[FLAGS_filter_field.c_str()].GetInt64(),
                                 FLAGS_filter_field_value);
        return true;
      }
    }
  } else if (obj[FLAGS_filter_field.c_str()].IsInt64()) {
    if (FLAGS_filter_field_reverse) {
      if (fmt::format("{}", obj[FLAGS_filter_field.c_str()].GetInt64()) != FLAGS_filter_field_value) {
        LOG(INFO) << fmt::format("filter value reverse: {} {}", obj[FLAGS_filter_field.c_str()].GetInt64(),
                                 FLAGS_filter_field_value);
        return true;
      }
    } else {
      if (fmt::format("{}", obj[FLAGS_filter_field.c_str()].GetInt64()) == FLAGS_filter_field_value) {
        LOG(INFO) << fmt::format("filter value: {} {}", obj[FLAGS_filter_field.c_str()].GetInt64(),
                                 FLAGS_filter_field_value);
        return true;
      }
    }
  }

  return false;
}

void DatasetUtils::GenNeighbor(const std::string& dataset_name, const std::string& test_dataset_filepath,
                               const std::string& train_dataset_dirpath, const std::string& out_filepath) {
  std::vector<VectorEntry> test_entries;

  auto get_id_func = [&](const rapidjson::Value& obj) -> int64_t {
    if (dataset_name == "wikipedia") {
      return obj["id"].GetInt64();
    } else if (dataset_name == "beir-bioasq") {
      return std::stoll(obj["_id"].GetString());
    }

    return -1;
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
        if (!item.HasMember("emb")) {
          continue;
        }

        auto* entry = new VectorEntry();
        entry->id = get_id_func(item);
        CHECK(entry->id != -1) << fmt::format("vector id({}) is invalid", entry->id);

        if (!item["emb"].IsArray()) {
          continue;
        }

        entry->emb.reserve(item["emb"].GetArray().Size());
        for (auto& f : item["emb"].GetArray()) {
          entry->emb.push_back(f.GetFloat());
        }
        CHECK(entry->emb.size() == FLAGS_vector_dimension)
            << fmt::format("dataset dimension({}) is not uniformity.", entry->emb.size());

        if (!FLAGS_filter_field.empty() && FilterValue(item)) {
          continue;
        }

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
  SaveTestDatasetNeighbor(test_doc, test_entries, out_filepath);
}

void DatasetUtils::GetStatisticsDistribution(const std::string& dataset_name, const std::string& train_dataset_dirpath,
                                             const std::string& field, const std::string& out_filepath) {
  auto get_id_func = [&](const rapidjson::Value& obj) -> int64_t {
    if (dataset_name == "wikipedia") {
      return obj["id"].GetInt64();
    } else if (dataset_name == "beir-bioasq") {
      return std::stoll(obj["_id"].GetString());
    }

    return -1;
  };

  std::vector<std::string> train_filepaths;
  auto train_filenames = dingodb::Helper::TraverseDirectory(train_dataset_dirpath, std::string("train"));
  train_filepaths.reserve(train_filenames.size());
  for (auto& filelname : train_filenames) {
    train_filepaths.push_back(fmt::format("{}/{}", train_dataset_dirpath, filelname));
  }
  std::cout << fmt::format("file count: {}", train_filepaths.size()) << std::endl;

  int64_t total_count = 0;
  std::unordered_map<std::string, std::vector<int64_t>> reverse_index;
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
      if (!item.HasMember(field.c_str())) {
        continue;
      }

      ++total_count;
      int64_t id = get_id_func(item);
      std::string value;
      if (item[field.c_str()].IsString()) {
        value = item[field.c_str()].GetString();
      } else if (item[field.c_str()].IsInt64()) {
        value = fmt::format("{}", item[field.c_str()].GetInt64());
      }

      auto it = reverse_index.find(value);
      if (it == reverse_index.end()) {
        reverse_index.insert(std::make_pair(value, std::vector<int64_t>{id}));
      } else {
        it->second.push_back(id);
      }
    }
  }

  struct Entry {
    std::string value;
    std::vector<int64_t> vector_ids;
    float rate;
    bool operator()(const Entry& lhs, const Entry& rhs) { return lhs.vector_ids.size() > rhs.vector_ids.size(); }
  };

  std::vector<Entry> entrys;
  for (auto& [key, vector_ids] : reverse_index) {
    Entry entry;
    entry.value = key;
    entry.vector_ids = vector_ids;
    entry.rate = static_cast<float>(vector_ids.size()) / total_count * 100;
    entrys.push_back(entry);
  }

  std::sort(entrys.begin(), entrys.end(), Entry());
  {
    rapidjson::Document doc;
    doc.SetArray();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

    for (auto& entry : entrys) {
      rapidjson::Value obj(rapidjson::kObjectType);
      if (IsDigitString(entry.value)) {
        int64_t v = std::strtoll(entry.value.c_str(), nullptr, 10);
        obj.AddMember(rapidjson::StringRef(field.c_str()), v, allocator);
      } else {
        obj.AddMember(rapidjson::StringRef(field.c_str()), rapidjson::StringRef(entry.value.c_str()), allocator);
      }

      rapidjson::Value vector_id_array(rapidjson::kArrayType);
      for (auto& vector_id : entry.vector_ids) {
        vector_id_array.PushBack(vector_id, allocator);
      }

      obj.AddMember("rate", entry.rate, allocator);
      obj.AddMember("vector_ids", vector_id_array, allocator);
      doc.PushBack(obj, allocator);
    }

    rapidjson::StringBuffer str_buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);
    doc.Accept(writer);
    dingodb::Helper::SaveFile(out_filepath, str_buf.GetString());
    doc.Clear();
  }
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

static std::string GetDatasetName() {
  std::string dataset_name;
  if (FLAGS_vector_dataset.find("wikipedia") != std::string::npos) {
    dataset_name = "wikipedia";
  } else if (FLAGS_vector_dataset.find("bioasq") != std::string::npos) {
    dataset_name = "beir-bioasq";
  }

  return dataset_name;
}

void DatasetUtils::Main() {
  if (GetDatasetName().empty()) {
    std::cerr << "Unknown dataset name: " << FLAGS_vector_dataset << std::endl;
    return;
  }

  if (FLAGS_sub_command == "distribution") {
    std::string distribution_filepath = fmt::format("{}/distribution.json", FLAGS_vector_dataset);
    GetStatisticsDistribution(GetDatasetName(), FLAGS_vector_dataset, FLAGS_filter_field, distribution_filepath);

  } else if (FLAGS_sub_command == "split_dataset") {
    SplitDataset(FLAGS_vector_dataset, FLAGS_split_num);

  } else if (FLAGS_sub_command == "gen_neighbor") {
    std::string neighbor_filepath = fmt::format("{}.neighbor", FLAGS_test_dataset_filepath);
    GenNeighbor(GetDatasetName(), FLAGS_test_dataset_filepath, FLAGS_vector_dataset, neighbor_filepath);
  }
}

}  // namespace benchmark
}  // namespace dingodb
