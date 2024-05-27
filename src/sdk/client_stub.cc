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

#include <memory>

#include "sdk/common/param_config.h"
#include "sdk/meta_cache.h"
#include "sdk/rawkv/raw_kv_region_scanner_impl.h"

#ifdef USE_GRPC
#include "sdk/rpc/grpc/grpc_rpc_client.h"
#else
#include "sdk/rpc/brpc/brpc_rpc_client.h"
#endif
#include "sdk/rpc/coordinator_rpc_controller.h"
#include "sdk/rpc/rpc_client.h"
#include "sdk/status.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/transaction/txn_region_scanner_impl.h"
#include "sdk/utils/net_util.h"
#include "sdk/utils/thread_pool_actuator.h"

namespace dingodb {

namespace sdk {

ClientStub::ClientStub()
    : coordinator_rpc_controller_(nullptr),
      raw_kv_region_scanner_factory_(nullptr),
      meta_cache_(nullptr),
      admin_tool_(nullptr) {}

ClientStub::~ClientStub() = default;

Status ClientStub::Open(const std::vector<EndPoint>& endpoints) {
  CHECK(!endpoints.empty());
  coordinator_rpc_controller_ = std::make_shared<CoordinatorRpcController>(*this);
  coordinator_rpc_controller_->Open(endpoints);

  meta_rpc_controller_ = std::make_shared<CoordinatorRpcController>(*this);
  meta_rpc_controller_->Open(endpoints);

  RpcClientOptions options;
  options.timeout_ms = FLAGS_rpc_channel_timeout_ms;
  options.connect_timeout_ms = FLAGS_rpc_channel_connect_timeout_ms;

#ifdef USE_GRPC
  store_rpc_client_ = std::make_shared<GrpcRpcClient>(options);
  store_rpc_client_->Open();
#else
  store_rpc_client_ = std::make_shared<BrpcRpcClient>(options);
#endif

  meta_cache_ = std::make_shared<MetaCache>(coordinator_rpc_controller_);

  raw_kv_region_scanner_factory_ = std::make_shared<RawKvRegionScannerFactoryImpl>();

  txn_region_scanner_factory_ = std::make_shared<TxnRegionScannerFactoryImpl>();

  admin_tool_ = std::make_shared<AdminTool>(*this);

  txn_lock_resolver_ = std::make_shared<TxnLockResolver>(*(this));

  actuator_ = std::make_shared<ThreadPoolActuator>();
  actuator_->Start(FLAGS_actuator_thread_num);

  vector_index_cache_ = std::make_shared<VectorIndexCache>(*this);

  auto_increment_manager_ = std::make_shared<AutoIncrementerManager>(*this);

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb