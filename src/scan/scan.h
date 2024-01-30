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

#ifndef DINGODB_ENGINE_SCAN_H_
#define DINGODB_ENGINE_SCAN_H_

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "butil/status.h"
#include "coprocessor/raw_coprocessor.h"
#include "engine/iterator.h"
#include "engine/raw_engine.h"
#include "proto/common.pb.h"

namespace dingodb {

// enable scan optimization switch to speed up scan execution if enabled
#ifndef ENABLE_SCAN_OPTIMIZATION
#define ENABLE_SCAN_OPTIMIZATION
#endif

#undef ENABLE_SCAN_OPTIMIZATION

enum class ScanState : unsigned char {
  kUninit = 0,
  kOpening = 1,
  kOpened = 2,
  kBeginning = 3,
  kBegun = 4,
  kContinuing = 5,
  kContinued = 6,
  kReleasing = 7,
  kReleased = 8,
  // error or timeout to destroy
  kError = 9,
  kBegunTimeout = 10,
  kContinuedTimeout = 11,
  kReleasedTimeout = 12,
  kAllowImmediateRecycling = 13,
  kDestroy = 14,
};

class ScanHandler;

class ScanContext {
 public:
  explicit ScanContext(bvar::LatencyRecorder* scan_latency);
  virtual ~ScanContext();

  ScanContext(const ScanContext& rhs) = delete;
  ScanContext& operator=(const ScanContext& rhs) = delete;
  ScanContext(ScanContext&& rhs) = delete;
  ScanContext& operator=(ScanContext&& rhs) = delete;

  virtual void Init(int64_t timeout_ms, int64_t max_bytes_rpc, int64_t max_fetch_cnt_by_server);

  virtual butil::Status Open(const std::string& scan_id, std::shared_ptr<RawEngine> engine, const std::string& cf_name);

  // Is it possible to delete this object
  virtual bool IsRecyclable();

  static const char* GetScanState(ScanState state);

 protected:
  friend class ScanHandler;

 private:
  void Close();
  static std::chrono::milliseconds GetCurrentTime();
  butil::Status GetKeyValue(std::vector<pb::common::KeyValue>& kvs, bool& has_more);  // NOLINT
#if defined(ENABLE_SCAN_OPTIMIZATION)
  butil::Status AsyncWork();
  void WaitForReady();
  butil::Status SeekCheck();
#endif
  std::string scan_id_;

  int64_t region_id_;

  pb::common::Range range_;

  int64_t max_fetch_cnt_;

  bool key_only_;

  bool disable_auto_release_;

  ScanState state_;

  std::shared_ptr<RawEngine> engine_;

  std::string cf_name_;

  IteratorPtr iter_;

  // millisecond 1s = 1000 millisecond
  std::chrono::milliseconds last_time_ms_;

  bthread_mutex_t mutex_;
#if defined(ENABLE_SCAN_OPTIMIZATION)
  enum class SeekState : unsigned char {
    kUninit = 0,
    kInitting = 1,
    kInitted = 2,
  };

  static const char* GetSeekState(SeekState state);

  // default = kUninit
  volatile SeekState seek_state_;
#endif

  bool disable_coprocessor_;

  // coprocessor
  std::shared_ptr<RawCoprocessor> coprocessor_;

  // timeout millisecond to destroy
  int64_t timeout_ms_;

  // Maximum number of bytes per transfer from rpc default 4M
  int64_t max_bytes_rpc_;

  // kv count per transfer specified by the server
  int64_t max_fetch_cnt_by_server_;

  bvar::LatencyRecorder* scan_latency_;
  BvarLatencyGuard bvar_guard_;
};

class ScanContextV1 : public ScanContext {
 public:
  explicit ScanContextV1(bvar::LatencyRecorder* scan_latency);
  ~ScanContextV1() override;

  static bvar::LatencyRecorder* GetScanLatency();

 private:
  static bvar::LatencyRecorder scan_context_v1_latency;
};
class ScanContextV2 : public ScanContext {
 public:
  explicit ScanContextV2(bvar::LatencyRecorder* scan_latency);
  ~ScanContextV2() override;

  static bvar::LatencyRecorder* GetScanLatency();

 private:
  static bvar::LatencyRecorder scan_context_v2_latency;
};

class ScanHandler {
 public:
  ScanHandler() = delete;
  ~ScanHandler() = delete;

  ScanHandler(const ScanHandler& rhs) = delete;
  ScanHandler& operator=(const ScanHandler& rhs) = delete;
  ScanHandler(ScanHandler&& rhs) = delete;
  ScanHandler& operator=(ScanHandler&& rhs) = delete;

  static butil::Status ScanBegin(std::shared_ptr<ScanContext> context, int64_t region_id,
                                 const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                                 bool disable_auto_release, bool disable_coprocessor,
                                 const CoprocessorPbWrapper& coprocessor, std::vector<pb::common::KeyValue>* kvs);

  static butil::Status ScanContinue(std::shared_ptr<ScanContext> context, const std::string& scan_id,
                                    int64_t max_fetch_cnt, std::vector<pb::common::KeyValue>* kvs, bool& has_more);

  static butil::Status ScanRelease(std::shared_ptr<ScanContext> context, [[maybe_unused]] const std::string& scan_id);
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_SCAN_H_  // NOLINT
