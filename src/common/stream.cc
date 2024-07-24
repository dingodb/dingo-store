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

#include "common/stream.h"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "butil/guid.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "glog/logging.h"

namespace dingodb {

DEFINE_uint32(stream_expire_interval_ms, 10000, "stream expire interval");
DEFINE_int64(stream_message_max_bytes, 60 * 1024 * 1024, "stream message max bytes");
DEFINE_int64(stream_message_max_limit_size, 40960, "stream message max line size");

// generate stream id, use uuid
static std::string GenStreamId() { return butil::GenerateGUID(); }

StreamPtr Stream::New(uint32_t limit) { return std::make_shared<Stream>(GenStreamId(), limit); }

bool Stream::Check(size_t size, size_t bytes) const {
  return (size >= limit_ || size >= FLAGS_stream_message_max_limit_size || bytes >= FLAGS_stream_message_max_bytes);
}

StreamSet::StreamSet() { bthread_mutex_init(&mutex_, nullptr); }
StreamSet::~StreamSet() { bthread_mutex_destroy(&mutex_); }

bool StreamSet::AddStream(StreamPtr stream) {
  CHECK(stream != nullptr) << "stream is nullptr";

  BAIDU_SCOPED_LOCK(mutex_);

  // stream_set_.insert_or_assign(stream->StreamId(), stream);
  return stream_set_.insert(std::make_pair(stream->StreamId(), stream)).second;
}

bool StreamSet::RemoveStream(StreamPtr stream) {
  CHECK(stream != nullptr) << "stream is nullptr";

  BAIDU_SCOPED_LOCK(mutex_);

  return stream_set_.erase(stream->StreamId()) > 0;
}

StreamPtr StreamSet::GetStream(std::string stream_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = stream_set_.find(stream_id);
  return (it != stream_set_.end()) ? it->second : nullptr;
}

std::vector<StreamPtr> StreamSet::GetAllStreams() {
  std::vector<StreamPtr> streams;
  streams.reserve(stream_set_.size());

  BAIDU_SCOPED_LOCK(mutex_);

  for (auto& [_, stream] : stream_set_) {
    streams.push_back(stream);
  }

  return streams;
}

int64_t StreamSet::GetStreamCount() {
  BAIDU_SCOPED_LOCK(mutex_);

  return stream_set_.size();
}

void StreamManager::AddStream(StreamPtr stream) {
  bool ret = stream_set_->AddStream(stream);
  CHECK(ret) << fmt::format("add stream({}) failed.", stream->StreamId());
  total_stream_count_ << 1;
}

void StreamManager::RemoveStream(StreamPtr stream) {
  if (stream_set_->RemoveStream(stream)) {
    release_stream_count_ << 1;
  }
}

StreamPtr StreamManager::GetOrNew(const pb::stream::StreamRequestMeta& stream_meta) {
  auto stream = GetStream(stream_meta.stream_id());
  if (stream != nullptr) {
    return stream;
  }

  stream = Stream::New(stream_meta.limit());
  AddStream(stream);

  return stream;
}

void StreamManager::RecycleExpireStream() {
  int64_t now_time = Helper::TimestampMs() - FLAGS_stream_expire_interval_ms;
  auto streams = stream_set_->GetAllStreams();
  for (auto& stream : streams) {
    if (stream->LastTimeMs() < now_time) {
      RemoveStream(stream);
    }
  }
}

}  // namespace dingodb