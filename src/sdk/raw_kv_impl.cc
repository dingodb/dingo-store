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

#include "sdk/raw_kv_impl.h"

#include <memory>

#include "sdk/client.h"
#include "sdk/common.h"
#include "sdk/meta_cache.h"
#include "sdk/store_rpc.h"
#include "sdk/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

RawKV::RawKVImpl::RawKVImpl(const ClientStub& stub) : stub_(stub) {}

Status RawKV::RawKVImpl::Get(const std::string& key, std::string& value) {
  std::shared_ptr<MetaCache> meta_cache = stub_.GetMetaCache();

  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  if (!got.IsOK()) {
    return got;
  }

  KvGetRpc rpc;
  FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
  rpc.MutableRequest()->set_key(key);

  StoreRpcController controller(stub_, rpc, region);
  Status call = controller.Call();
  if (call.IsOK()) {
    value = rpc.Response()->value();
  }
  return call;
}

Status RawKV::RawKVImpl::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  (void)keys;
  (void)kvs;
  return Status::NotSupported("not implement");
}

Status RawKV::RawKVImpl::Put(const std::string& key, const std::string& value) {
  std::shared_ptr<MetaCache> meta_cache = stub_.GetMetaCache();

  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  if (!got.IsOK()) {
    return got;
  }

  KvPutRpc rpc;
  auto* kv = rpc.MutableRequest()->mutable_kv();
  FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
  kv->set_key(key);
  kv->set_value(value);

  StoreRpcController controller(stub_, rpc, region);
  return controller.Call();
}

Status RawKV::RawKVImpl::BatchPut(const std::vector<KVPair>& kvs) {
  (void)kvs;
  return Status::NotSupported("not implement");
}

Status RawKV::RawKVImpl::Delete(const std::string& key) {
  (void)key;
  return Status::NotSupported("not implement");
}

Status RawKV::RawKVImpl::PutIfAbsent(const std::string& key, const std::string& value) {
  (void)key;
  (void)value;
  return Status::NotSupported("not implement");
}

}  // namespace sdk
}  // namespace dingodb