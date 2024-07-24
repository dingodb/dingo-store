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

#ifndef DINGODB_COMMON_STREAM_H_
#define DINGODB_COMMON_STREAM_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "bthread/types.h"
#include "common/failpoint.h"
#include "proto/stream.pb.h"

namespace dingodb {

class Stream;
using StreamPtr = std::shared_ptr<Stream>;

class StreamSet;
using StreamSetPtr = std::shared_ptr<StreamSet>;

class StreamManager;
using StreamManagerPtr = std::shared_ptr<StreamManager>;

class StreamState {
 public:
  StreamState() = default;
  virtual ~StreamState() = default;
};
using StreamStatePtr = std::shared_ptr<StreamState>;

class Stream {
 public:
  Stream(std::string stream_id, uint32_t limit) : stream_id_(stream_id), limit_(limit) {}
  ~Stream() = default;

  static StreamPtr New(uint32_t limit);

  std::string StreamId() const { return stream_id_; }
  uint32_t Limit() const { return limit_; }

  using StreamStateAllocator = std::function<StreamStatePtr()>;

  StreamStatePtr GetOrNewStreamState(StreamStateAllocator allocator) {
    if (stream_state_ != nullptr) {
      return stream_state_;
    }

    stream_state_ = allocator();
    return stream_state_;
  }

  void SetStreamState(StreamStatePtr stream_state) { stream_state_ = stream_state; }
  StreamStatePtr StreamState() const { return stream_state_; }

  int64_t LastTimeMs() const { return last_time_ms_; }

  bool Check(size_t size, size_t bytes) const;

 private:
  std::string stream_id_;
  uint32_t limit_;

  StreamStatePtr stream_state_;

  // last request time, for check stream timeout, clean stream.
  int64_t last_time_ms_;
};

class StreamSet {
 public:
  StreamSet();
  ~StreamSet();

  static StreamSetPtr New() { return std::make_shared<StreamSet>(); }

  bool AddStream(StreamPtr stream);
  bool RemoveStream(StreamPtr stream);
  StreamPtr GetStream(std::string stream_id);
  std::vector<StreamPtr> GetAllStreams();

  int64_t GetStreamCount();

 private:
  bthread_mutex_t mutex_;
  std::unordered_map<std::string, StreamPtr> stream_set_;
};

class StreamManager {
 public:
  StreamManager()
      : stream_set_(StreamSet::New()),
        total_stream_count_("dingo_stream_total_count"),
        release_stream_count_("dingo_stream_release_count") {}

  ~StreamManager() = default;

  static StreamManagerPtr New() { return std::make_shared<StreamManager>(); }

  StreamPtr GetOrNew(const pb::stream::StreamRequestMeta& stream_meta);

  void AddStream(StreamPtr stream);
  void RemoveStream(StreamPtr stream);
  StreamPtr GetStream(std::string stream_id) { return stream_set_->GetStream(stream_id); }

  int64_t GetStreamCount() { return stream_set_->GetStreamCount(); }

  void RecycleExpireStream();

 private:
  StreamSetPtr stream_set_;

  // statistic
  bvar::Adder<uint64_t> total_stream_count_;
  bvar::Adder<int64_t> release_stream_count_;
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_STREAM_H_
