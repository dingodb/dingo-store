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

#ifndef DINGODB_SDK_CLIENT_STUB_H_
#define DINGODB_SDK_CLIENT_STUB_H_

#include <memory>

#include "glog/logging.h"
#include "sdk/admin_tool.h"
#include "sdk/coordinator_proxy.h"
#include "sdk/meta_cache.h"
#include "sdk/region_scanner.h"
#include "sdk/rpc/rpc_interaction.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/vector/vector_index_cache.h"
#include "utils/actuator.h"

namespace dingodb {
namespace sdk {

class ClientStub {
 public:
  ClientStub();

  virtual ~ClientStub();

  Status Open(std::string naming_service_url);

  virtual std::shared_ptr<CoordinatorProxy> GetCoordinatorProxy() const {
    DCHECK_NOTNULL(coordinator_proxy_.get());
    return coordinator_proxy_;
  }

  virtual std::shared_ptr<MetaCache> GetMetaCache() const {
    DCHECK_NOTNULL(meta_cache_.get());
    return meta_cache_;
  }

  virtual std::shared_ptr<RpcInteraction> GetStoreRpcInteraction() const {
    DCHECK_NOTNULL(store_rpc_interaction_.get());
    return store_rpc_interaction_;
  }

  virtual std::shared_ptr<RegionScannerFactory> GetRawKvRegionScannerFactory() const {
    DCHECK_NOTNULL(raw_kv_region_scanner_factory_.get());
    return raw_kv_region_scanner_factory_;
  }

  virtual std::shared_ptr<RegionScannerFactory> GetTxnRegionScannerFactory() const {
    DCHECK_NOTNULL(txn_region_scanner_factory_.get());
    return txn_region_scanner_factory_;
  }

  virtual std::shared_ptr<AdminTool> GetAdminTool() const {
    DCHECK_NOTNULL(admin_tool_.get());
    return admin_tool_;
  }

  virtual std::shared_ptr<TxnLockResolver> GetTxnLockResolver() const {
    DCHECK_NOTNULL(txn_lock_resolver_.get());
    return txn_lock_resolver_;
  }

  virtual std::shared_ptr<Actuator> GetActuator() const {
    DCHECK_NOTNULL(actuator_.get());
    return actuator_;
  }

  virtual std::shared_ptr<VectorIndexCache> GetVectorIndexCache() const {
    DCHECK_NOTNULL(vector_index_cache_.get());
    return vector_index_cache_;
  }

 private:
  // TODO: use unique ptr
  std::shared_ptr<CoordinatorProxy> coordinator_proxy_;
  std::shared_ptr<MetaCache> meta_cache_;
  std::shared_ptr<RpcInteraction> store_rpc_interaction_;
  std::shared_ptr<RegionScannerFactory> raw_kv_region_scanner_factory_;
  std::shared_ptr<RegionScannerFactory> txn_region_scanner_factory_;
  std::shared_ptr<AdminTool> admin_tool_;
  std::shared_ptr<TxnLockResolver> txn_lock_resolver_;
  std::shared_ptr<Actuator> actuator_;
  std::shared_ptr<VectorIndexCache> vector_index_cache_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_CLIENT_STUB_H_