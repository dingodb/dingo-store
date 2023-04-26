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

#include "client/client_helper.h"
#include "client/client_interation.h"

namespace client {

// key/value
void SendKvGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key, std::string& value);
void SendKvBatchGet(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count);
void SendKvPut(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key, std::string value = "");
void SendKvBatchPut(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count);
void SendKvPutIfAbsent(ServerInteractionPtr interaction, uint64_t region_id, const std::string& key);
void SendKvBatchPutIfAbsent(ServerInteractionPtr interaction, uint64_t region_id, const std::string& prefix, int count);

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

}  // namespace client

#endif  // DINGODB_CLIENT_STORE_CLIENT_FUNCTION_H_