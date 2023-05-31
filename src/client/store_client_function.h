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

#ifndef DINGODB_CLIENT_STORE_CLIENT_FUNCTION_H_
#define DINGODB_CLIENT_STORE_CLIENT_FUNCTION_H_

#include <memory>

#include "client/client_helper.h"
#include "client/client_interation.h"

namespace client {

struct Context {
  std::unique_ptr<Context> Clone() const {
    auto clone_ctx = std::make_unique<Context>();
    clone_ctx->coordinator_interaction = coordinator_interaction;
    clone_ctx->store_interaction = store_interaction;
    clone_ctx->table_name = table_name;
    clone_ctx->partition_num = partition_num;
    clone_ctx->req_num = req_num;

    return clone_ctx;
  }

  ServerInteractionPtr coordinator_interaction;
  ServerInteractionPtr store_interaction;

  std::string table_name;
  int partition_num;
  int req_num;
};

// key/value
void SendKvGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key, std::string& value);
void SendKvBatchGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count);
void SendKvPut(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key, std::string value = "");
void SendKvBatchPut(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count);
void SendKvPutIfAbsent(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key);
void SendKvBatchPutIfAbsent(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count);
void SendKvBatchDelete(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key);
void SendKvDeleteRange(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix);
void SendKvScan(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix);
void SendKvCompareAndSet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key);
void SendKvBatchCompareAndSet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count);

// region
void SendAddRegion(ServerInteractionPtr interaction, uint64_t region_id, const std::string& raft_group,
                   std::vector<std::string> raft_addrs);
void SendChangeRegion(ServerInteractionPtr interaction, uint64_t region_id, const std::string& raft_group,
                      std::vector<std::string> raft_addrs);
void SendDestroyRegion(ServerInteractionPtr interaction, uint64_t region_id);
void SendSnapshot(ServerInteractionPtr interaction, uint64_t region_id);
void BatchSendAddRegion(ServerInteractionPtr interaction, int start_region_id, int region_count, int thread_num,
                        const std::string& raft_group, std::vector<std::string>& raft_addrs);

// test
void TestBatchPut(ServerInteractionPtr interaction, uint64_t region_id, int thread_num, int req_num,
                  const std::string& prefix);
void TestBatchPutGet(ServerInteractionPtr interaction, uint64_t region_id, int thread_num, int req_num,
                     const std::string& prefix);
void TestRegionLifecycle(ServerInteractionPtr interaction, uint64_t region_id, const std::string& raft_group,
                         std::vector<std::string>& raft_addrs, int region_count, int thread_num, int req_num,
                         const std::string& prefix);
void AutoTest(std::shared_ptr<Context> ctx);

}  // namespace client

#endif  // DINGODB_CLIENT_STORE_CLIENT_FUNCTION_H_