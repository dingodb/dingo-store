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
#include "sdk/rawkv/raw_kv_region_scanner_impl.h"

#include <functional>
#include <memory>
#include <string>

#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/store/store_rpc.h"
#include "sdk/store/store_rpc_controller.h"
#include "sdk/utils/async_util.h"

namespace dingodb {
namespace sdk {

RawKvRegionScannerImpl::RawKvRegionScannerImpl(const ClientStub& stub, std::shared_ptr<Region> region, std::string start_key,
                                     std::string end_key)
    : RegionScanner(stub, std::move(region)),
      start_key_(std::move(start_key)),
      end_key_(std::move(end_key)),
      opened_(false),
      has_more_(false),
      batch_size_(FLAGS_scan_batch_size) {}

static void RawKvRegionScannerImplDeleted(Status status, std::string scan_id) {
  VLOG(kSdkVlogLevel) << "RawKvRegionScannerImpl deleted, scanner id: " << scan_id << " status:" << status.ToString();
}

RawKvRegionScannerImpl::~RawKvRegionScannerImpl() {
  std::string scan_id = scan_id_;
  AsyncClose([scan_id](auto&& s) { return RawKvRegionScannerImplDeleted(std::forward<decltype(s)>(s), scan_id); });
}

void RawKvRegionScannerImpl::PrepareScanBegionRpc(KvScanBeginRpc& rpc) {
  auto* request = rpc.MutableRequest();
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->Epoch());
  auto* range_with_option = request->mutable_range();
  range_with_option->mutable_range()->set_start_key(start_key_);
  range_with_option->mutable_range()->set_end_key(end_key_);
  range_with_option->set_with_start(true);
  range_with_option->set_with_end(false);

  request->set_max_fetch_cnt(0);
  request->set_key_only(false);
  // TODO: maybe we should support scan keep_alive
  request->set_disable_auto_release(false);
  request->set_disable_coprocessor(true);
}

void RawKvRegionScannerImpl::AsyncOpen(StatusCallback cb) {
  CHECK(!opened_);
  auto* rpc = new KvScanBeginRpc();
  PrepareScanBegionRpc(*rpc);

  auto* controller = new StoreRpcController(stub, *rpc, region);
  controller->AsyncCall(
      [this, controller, rpc, cb](auto&& s) { AsyncOpenCallback(std::forward<decltype(s)>(s), controller, rpc, cb); });
}

void RawKvRegionScannerImpl::AsyncOpenCallback(Status status, StoreRpcController* controller, KvScanBeginRpc* rpc,
                                          StatusCallback cb) {
  ScopeGuard sg([controller, rpc] {
    delete controller;
    delete rpc;
  });

  if (status.ok()) {
    CHECK_EQ(0, rpc->Response()->kvs_size());
    scan_id_ = rpc->Response()->scan_id();
    has_more_ = true;
    opened_ = true;
  } else {
    DINGO_LOG(WARNING) << "open scanner for region:" << region->RegionId() << ", fail:" << status.ToString();
  }
  cb(status);
}

Status RawKvRegionScannerImpl::Open() {
  CHECK(false) << "Not supported. Use AsyncOpen to Open scanner for region:" << region->RegionId();
  return Status::OK();
}

void RawKvRegionScannerImpl::PrepareScanReleaseRpc(KvScanReleaseRpc& rpc) {
  auto* request = rpc.MutableRequest();
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->Epoch());
  request->set_scan_id(scan_id_);
}

void RawKvRegionScannerImpl::AsyncClose(StatusCallback cb) {
  if (opened_) {
    CHECK(!scan_id_.empty());
    auto* rpc = new KvScanReleaseRpc();
    PrepareScanReleaseRpc(*rpc);

    auto* controller = new StoreRpcController(stub, *rpc, region);

    opened_ = false;
    std::string scan_id = scan_id_;
    controller->AsyncCall([scan_id, controller, rpc, cb](auto&& s) {
      return RawKvRegionScannerImpl::AsyncCloseCallback(std::forward<decltype(s)>(s), scan_id, controller, rpc, cb);
    });
  }
}

void RawKvRegionScannerImpl::AsyncCloseCallback(Status status, std::string scan_id, StoreRpcController* controller,
                                           KvScanReleaseRpc* rpc, StatusCallback cb) {
  ScopeGuard sg([controller, rpc] {
    delete controller;
    delete rpc;
  });

  if (!status.IsOK()) {
    DINGO_LOG(WARNING) << "Fail release scanner, scan_id:" << scan_id << ", status:" << status.ToString();
  } else {
    VLOG(kSdkVlogLevel) << "Success release scanner, scan_id:" << scan_id << ", status:" << status.ToString();
  }

  // TODO: we should support scan keep alive in case release scan fail
  cb(status);
}

void RawKvRegionScannerImpl::Close() {
  CHECK(false) << "Not supported. Use AsyncClose to Close scanner for region:" << region->RegionId()
               << ", scan_id:" << scan_id_;
}

bool RawKvRegionScannerImpl::HasMore() const { return has_more_; }

void RawKvRegionScannerImpl::PrepareScanContinueRpc(KvScanContinueRpc& rpc) {
  auto* request = rpc.MutableRequest();
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->Epoch());
  request->set_scan_id(scan_id_);
  request->set_max_fetch_cnt(batch_size_);
}

void RawKvRegionScannerImpl::AsyncNextBatch(std::vector<KVPair>& kvs, StatusCallback cb) {
  CHECK(opened_);
  CHECK(!scan_id_.empty());
  auto rpc = std::make_unique<KvScanContinueRpc>();
  PrepareScanContinueRpc(*rpc);

  auto controller = std::make_unique<StoreRpcController>(stub, *rpc, region);
  controller->AsyncCall([this, c = controller.release(), r = rpc.release(), &kvs, cb](auto&& s) {
    KvScanContinueRpcCallback(std::forward<decltype(s)>(s), c, r, kvs, cb);
  });
}

void RawKvRegionScannerImpl::KvScanContinueRpcCallback(Status status, StoreRpcController* controller, KvScanContinueRpc* rpc,
                                                  std::vector<KVPair>& kvs, StatusCallback cb) {
  ScopeGuard sg([controller, rpc] {
    delete controller;
    delete rpc;
  });

  if (status.ok()) {
    const auto* response = rpc->Response();
    std::vector<KVPair> tmp_kvs;
    if (response->kvs_size() == 0) {
      // scan to region end_key
      has_more_ = false;
    } else {
      for (const auto& kv : response->kvs()) {
        if (kv.key() < end_key_) {
          tmp_kvs.push_back({kv.key(), kv.value()});
        } else {
          has_more_ = false;
        }
      }
    }

    kvs = std::move(tmp_kvs);
  } else {
    DINGO_LOG(WARNING) << "scanner_id:" << scan_id_ << " scan continue fail region:" << region->RegionId()
                       << ", fail:" << status.ToString();
  }

  cb(status);
}

Status RawKvRegionScannerImpl::NextBatch(std::vector<KVPair>& kvs) {
  Synchronizer sync;
  Status status;
  AsyncNextBatch(kvs, sync.AsStatusCallBack(status));
  sync.Wait();
  return status;
}

Status RawKvRegionScannerImpl::SetBatchSize(int64_t size) {
  uint64_t to_size = size;
  if (size <= kMinScanBatchSize) {
    to_size = kMinScanBatchSize;
  }

  if (size > kMaxScanBatchSize) {
    to_size = kMaxScanBatchSize;
  }

  batch_size_ = to_size;
  return Status::OK();
}

RawKvRegionScannerFactoryImpl::RawKvRegionScannerFactoryImpl() = default;

RawKvRegionScannerFactoryImpl::~RawKvRegionScannerFactoryImpl() = default;

Status RawKvRegionScannerFactoryImpl::NewRegionScanner(const ScannerOptions& options,
                                                  std::shared_ptr<RegionScanner>& scanner) {
  CHECK(options.start_key < options.end_key);
  CHECK(options.start_key >= options.region->Range().start_key())
      << fmt::format("start_key:{} should greater than region range start_key:{}", options.start_key,
                     options.region->Range().start_key());
  CHECK(options.end_key <= options.region->Range().end_key()) << fmt::format(
      "end_key:{} should little than region range end_key:{}", options.end_key, options.region->Range().end_key());

  std::shared_ptr<RegionScanner> tmp(
      new RawKvRegionScannerImpl(options.stub, options.region, options.start_key, options.end_key));
  scanner = std::move(tmp);

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb