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
#include <vector>

#include "sdk/coordinator_proxy.h"
#include "sdk/meta_cache.h"
#include "sdk/region_scanner.h"
#include "sdk/rpc_interaction.h"

namespace dingodb {
namespace sdk {

class ClientStub {
 public:
  ClientStub();

  virtual ~ClientStub();

  Status Open(std::string naming_service_url);

  virtual std::shared_ptr<CoordiantorProxy> GetCoordinatorProxy() const { return coordinator_proxy_; }

  virtual std::shared_ptr<MetaCache> GetMetaCache() const { return meta_cache_; }

  virtual std::shared_ptr<RpcInteraction> GetStoreRpcInteraction() const { return store_rpc_interaction_; }

  virtual std::shared_ptr<RegionScannerFactory> GetRegionScannerFactory() const { return region_scanner_factory_; }

 private:
  std::shared_ptr<CoordiantorProxy> coordinator_proxy_;

  std::shared_ptr<MetaCache> meta_cache_;

  std::shared_ptr<RpcInteraction> store_rpc_interaction_;

  std::shared_ptr<RegionScannerFactory> region_scanner_factory_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_CLIENT_STUB_H_