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

#include "sdk/client_stub.h"

#include "sdk/common/param_config.h"
#include "sdk/meta_cache.h"
#include "sdk/rawkv/raw_kv_region_scanner_impl.h"
#include "sdk/status.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/transaction/txn_region_scanner_impl.h"
#include "sdk/utils/thread_pool_actuator.h"

namespace dingodb {

namespace sdk {

ClientStub::ClientStub()
    : coordinator_proxy_(nullptr), raw_kv_region_scanner_factory_(nullptr), meta_cache_(nullptr), admin_tool_(nullptr) {}

ClientStub::~ClientStub() = default;

Status ClientStub::Open(std::string naming_service_url) {
  coordinator_proxy_ = std::make_shared<CoordinatorProxy>();
  DINGO_RETURN_NOT_OK(coordinator_proxy_->Open(naming_service_url));

  // TODO: pass use gflag or add options
  brpc::ChannelOptions options;
  options.timeout_ms = FLAGS_rpc_channel_timeout_ms;
  options.connect_timeout_ms = FLAGS_rpc_channel_connect_timeout_ms;
  store_rpc_interaction_.reset(new RpcInteraction(options));

  meta_cache_.reset(new MetaCache(coordinator_proxy_));

  raw_kv_region_scanner_factory_.reset(new RawKvRegionScannerFactoryImpl());

  txn_region_scanner_factory_.reset(new TxnRegionScannerFactoryImpl());

  admin_tool_.reset(new AdminTool(coordinator_proxy_));

  txn_lock_resolver_.reset(new TxnLockResolver(*(this)));

  actuator_.reset(new ThreadPoolActuator());
  actuator_->Start(FLAGS_actuator_thread_num);

  vector_index_cache_.reset(new VectorIndexCache(*coordinator_proxy_));

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb