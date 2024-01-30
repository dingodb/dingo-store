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

#include "sdk/rawkv/raw_kv_scan_task.h"

#include <memory>

#include "sdk/rawkv/raw_kv_task.h"
#include "sdk/region_scanner.h"

namespace dingodb {
namespace sdk {

RawKvScanTask::RawKvScanTask(const ClientStub& stub, const std::string& start_key, const std::string& end_key,
                             uint64_t limit, std::vector<KVPair>& out_kvs)
    : RawKvTask(stub), start_key_(start_key), end_key_(end_key), limit_(limit), out_kvs_(out_kvs) {}

Status RawKvScanTask::Init() {
  auto meta_cache = stub.GetMetaCache();

  // precheck: return not found if no region in [start, end_key)
  std::shared_ptr<Region> region;
  Status ret = meta_cache->LookupRegionBetweenRange(start_key_, end_key_, region);
  if (!ret.ok()) {
    if (ret.IsNotFound()) {
      DINGO_LOG(WARNING) << fmt::format("region not found between [{},{}), no need retry, status:{}", start_key_,
                                        end_key_, ret.ToString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("lookup region fail between [{},{}), need retry, status:{}", start_key_,
                                        end_key_, ret.ToString());
    }
    return ret;
  }

  next_start_key_ = start_key_;
  return Status::OK();
}

void RawKvScanTask::DoAsync() {
  CHECK(!next_start_key_.empty()) << "next_start_key_ should not empty";
  CHECK(next_start_key_ < end_key_) << fmt::format("next_start_key_:{} should less than end_key_:{}", next_start_key_,
                                                   end_key_);
  ScanNext();
}

void RawKvScanTask::ScanNext() {
  if (ReachLimit()) {
    DINGO_LOG(INFO) << fmt::format("scan end between [{},{}), next_start:{}, limit:{}, scan_cnt:{}", start_key_,
                                   end_key_, next_start_key_, limit_, tmp_out_kvs_.size());
    tmp_out_kvs_.resize(limit_);
    DoAsyncDone(Status::OK());
    return;
  }

  if (next_start_key_ >= end_key_) {
    DINGO_LOG(INFO) << fmt::format("scan end between [{},{}), next_start:{}", start_key_, end_key_, next_start_key_);
    DoAsyncDone(Status::OK());
    return;
  }

  auto meta_cache = stub.GetMetaCache();

  std::shared_ptr<Region> region;
  status_ = meta_cache->LookupRegionBetweenRange(next_start_key_, end_key_, region);

  if (status_.IsNotFound()) {
    DINGO_LOG(INFO) << fmt::format("region not found  between [{},{}), start_key:{} status:{}", next_start_key_,
                                   end_key_, start_key_, status_.ToString());
    DoAsyncDone(Status::OK());
    return;
  }

  if (!status_.ok()) {
    DINGO_LOG(WARNING) << fmt::format("region look fail between [{},{}), start_key:{} status:{}", next_start_key_,
                                      end_key_, start_key_, status_.ToString());
    DoAsyncDone(status_);
    return;
  }

  std::string scanner_start_key =
      next_start_key_ <= region->Range().start_key() ? region->Range().start_key() : next_start_key_;
  std::string scanner_end_key = end_key_ <= region->Range().end_key() ? end_key_ : region->Range().end_key();
  ScannerOptions options(stub, region, scanner_start_key, scanner_end_key);

  std::shared_ptr<RegionScanner> scanner;
  CHECK(stub.GetRawKvRegionScannerFactory()->NewRegionScanner(options, scanner).IsOK());

  scanner->AsyncOpen(
      [this, scanner, region](auto&& s) { ScannerOpenCallback(std::forward<decltype(s)>(s), scanner, region); });
}

void RawKvScanTask::ScannerOpenCallback(Status status, std::shared_ptr<RegionScanner> scanner,
                                        std::shared_ptr<Region> region) {
  status_ = status;
  if (!status_.ok()) {
    DINGO_LOG(WARNING) << fmt::format("region scanner open fail, region:{}, status:{}", region->RegionId(),
                                      status_.ToString());
    DoAsyncDone(status_);
    return;
  }

  DINGO_LOG(INFO) << fmt::format("region:{} scan start, region range:({}-{})", region->RegionId(),
                                 region->Range().start_key(), region->Range().end_key());

  CHECK(scanner->HasMore());
  ScanNextWithScanner(scanner);
}

void RawKvScanTask::ScanNextWithScanner(std::shared_ptr<RegionScanner> scanner) {
  std::shared_ptr<Region> region = scanner->GetRegion();
  if (scanner->HasMore()) {
    tmp_scanner_scan_kvs_.clear();
    scanner->AsyncNextBatch(tmp_scanner_scan_kvs_,
                            [this, scanner](auto&& s) { NextBatchCallback(std::forward<decltype(s)>(s), scanner); });
  } else {
    next_start_key_ = region->Range().end_key();
    DINGO_LOG(INFO) << fmt::format("region:{} scan finished, continue to scan between [{},{}), next_start:{}, ",
                                   region->RegionId(), start_key_, end_key_, next_start_key_);
    ScanNext();
  }
}

void RawKvScanTask::NextBatchCallback(const Status& status, std::shared_ptr<RegionScanner> scanner) {
  status_ = status;
  std::shared_ptr<Region> region = scanner->GetRegion();
  if (!status_.ok()) {
    DINGO_LOG(WARNING) << fmt::format("region scanner NextBatch fail, region:{}, status:{}", region->RegionId(),
                                      status_.ToString());
    DoAsyncDone(status_);
    return;
  }

  if (!tmp_scanner_scan_kvs_.empty()) {
    tmp_out_kvs_.insert(tmp_out_kvs_.end(), std::make_move_iterator(tmp_scanner_scan_kvs_.begin()),
                        std::make_move_iterator(tmp_scanner_scan_kvs_.end()));
  } else {
    DINGO_LOG(INFO) << fmt::format("region:{} scanner NextBatch is empty", region->RegionId());
    CHECK(!scanner->HasMore());
  }

  ScanNextWithScanner(std::move(scanner));
}

bool RawKvScanTask::ReachLimit() { return limit_ != 0 && (tmp_out_kvs_.size() >= limit_); }

void RawKvScanTask::PostProcess() { out_kvs_ = std::move(tmp_out_kvs_); }

}  // namespace sdk

}  // namespace dingodb