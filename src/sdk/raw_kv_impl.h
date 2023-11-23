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

#ifndef DINGODB_SDK_RAW_KV_IMPL_H_
#define DINGODB_SDK_RAW_KV_IMPL_H_

#include <cstdint>
#include <memory>

#include "proto/store.pb.h"
#include "sdk/client.h"
#include "sdk/client_stub.h"
#include "sdk/meta_cache.h"
#include "sdk/status.h"
#include "sdk/region_scanner.h"

namespace dingodb {
namespace sdk {
class RawKV::RawKVImpl {
 public:
  RawKVImpl(const RawKVImpl&) = delete;
  const RawKVImpl& operator=(const RawKVImpl&) = delete;

  explicit RawKVImpl(const ClientStub& stub);

  ~RawKVImpl() = default;

  Status Get(const std::string& key, std::string& value);

  Status BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs);

  Status Put(const std::string& key, const std::string& value);

  Status BatchPut(const std::vector<KVPair>& kvs);

  Status PutIfAbsent(const std::string& key, const std::string& value, bool& state);

  Status BatchPutIfAbsent(const std::vector<KVPair>& kvs, std::vector<KeyOpState>& states);

  Status Delete(const std::string& key);

  Status BatchDelete(const std::vector<std::string>& keys);

  // TODO: process when region not exist
  Status DeleteRange(const std::string& start, const std::string& end, bool with_start, bool with_end,
                     int64_t& delete_count);

  // expected_value: empty means key not exist
  Status CompareAndSet(const std::string& key, const std::string& value, const std::string& expected_value,
                       bool& state);

  // expected_values size must equal kvs size
  Status BatchCompareAndSet(const std::vector<KVPair>& kvs, const std::vector<std::string>& expected_values,
                            std::vector<KeyOpState>& states);

  Status Scan(const std::string& start_key, const std::string& end_key,  uint64_t limit, std::vector<KVPair>& kvs);

 private:
  struct SubBatchState {
    Rpc* rpc;
    std::shared_ptr<Region> region;
    Status status;
    std::vector<KVPair> result_kvs;
    int64_t delete_count;                   // use for delete range
    std::vector<KeyOpState> key_op_states;  // use for batch_compare_and_set && batch_put_if_absent

    SubBatchState(Rpc* p_rpc, std::shared_ptr<Region> p_region)
        : rpc(p_rpc), region(std::move(p_region)), delete_count(0) {}
  };

  void ProcessSubBatchGet(SubBatchState* sub);

  void ProcessSubBatchPut(SubBatchState* sub);

  void ProcessSubBatchPutIfAbsent(SubBatchState* sub);

  void ProcessSubBatchDelete(SubBatchState* sub);

  void ProcessSubBatchDeleteRange(SubBatchState* sub);

  void ProcessSubBatchCompareAndSet(SubBatchState* sub);

  const ClientStub& stub_;
};
}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_RAW_KV_IMPL_H_