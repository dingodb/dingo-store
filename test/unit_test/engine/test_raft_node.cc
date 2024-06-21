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

#include <gtest/gtest.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "braft/configuration.h"
#include "brpc/server.h"
#include "butil/endpoint.h"
#include "butil/strings/string_split.h"
#include "butil/strings/stringprintf.h"
#include "config/yaml_config.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "raft/raft_node.h"
#include "raft/store_state_machine.h"

const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: 12345\n"
    "  coordinators: 127.0.0.1:19190,127.0.0.1:19191,127.0.0.1:19192\n"
    "  keyring: TO_BE_CONTINUED\n"
    "server:\n"
    "  host: 127.0.0.1\n"
    "  port: 23000\n"
    "  heartbeat_interval: 10000 # ms\n"
    "raft:\n"
    "  host: 127.0.0.1\n"
    "  port: 23100\n"
    "  path: /tmp/dingo-store/data/store/raft\n"
    "  log_path: /tmp/dingo-store/data/store/log\n"
    "  election_timeout: 1000 # ms\n"
    "  snapshot_interval: 3600 # s\n"
    "log:\n"
    "  path: /tmp/dingo-store/log\n"
    "store:\n"
    "  path: ./rocks_example\n"
    "  base:\n"
    "    block_size: 131072\n"
    "    block_cache: 67108864\n"
    "    arena_block_size: 67108864\n"
    "    min_write_buffer_number_to_merge: 4\n"
    "    max_write_buffer_number: 4\n"
    "    max_compaction_bytes: 134217728\n"
    "    write_buffer_size: 67108864\n"
    "    prefix_extractor: 8\n"
    "    max_bytes_for_level_base: 41943040\n"
    "    target_file_size_base: 4194304\n"
    "  default:\n"
    "  instruction:\n"
    "    max_write_buffer_number: 3\n"
    "  column_families:\n"
    "    - default\n"
    "    - meta\n"
    "    - instruction\n";

std::string GetRaftInitConf(const std::vector<std::string>& raft_addrs) {
  std::string s;
  for (int i = 0; i < raft_addrs.size(); ++i) {
    s += raft_addrs[i];
    if (i + 1 < raft_addrs.size()) {
      s += ",";
    }
  }
  return s;
}

static dingodb::store::RegionPtr BuildRegion(int64_t region_id, const std::string& raft_group_name,
                                             std::vector<std::string>& raft_addrs) {
  dingodb::pb::common::RegionDefinition region_definition;
  region_definition.set_id(region_id);
  region_definition.set_name(raft_group_name);
  auto* range = region_definition.mutable_range();
  range->set_start_key("a");
  range->set_end_key("z");

  for (const auto& inner_addr : raft_addrs) {
    std::vector<std::string> host_port_index;
    butil::SplitString(inner_addr, ':', &host_port_index);

    auto* peer = region_definition.add_peers();
    auto* raft_loc = peer->mutable_raft_location();
    raft_loc->set_host(host_port_index[0]);
    raft_loc->set_port(std::stoi(host_port_index[1]));
    raft_loc->set_index(std::stoi(host_port_index[2]));
  }

  return dingodb::store::Region::New(region_definition);
}

std::string FormatLocation(const dingodb::pb::common::Location& location) {
  return butil::StringPrintf("%s:%d:%d", location.host().c_str(), location.port(), location.index());
}

std::string FormatPeers(const std::vector<dingodb::pb::common::Peer> peers) {
  std::string result;
  int i = 0;
  for (const auto& peer : peers) {
    std::string addr = FormatLocation(peer.raft_location());
    result += addr;
    if (i + 1 < peers.size()) {
      result += ",";
    }
    ++i;
  }

  return result;
}

std::shared_ptr<dingodb::RaftNode> LaunchRaftNode(std::shared_ptr<dingodb::Config> config,
                                                  dingodb::store::RegionPtr region, int64_t node_id,
                                                  const dingodb::pb::common::Peer& peer, std::string init_conf) {
  // build state machine
  auto raft_meta = dingodb::store::RaftMeta::New(region->Id());
  auto state_machine =
      std::make_shared<dingodb::StoreStateMachine>(nullptr, region, raft_meta, nullptr, nullptr, nullptr);
  if (!state_machine->Init()) {
    LOG(INFO) << "Init state machine failed";
    return nullptr;
  }

  std::string log_path = fmt::format("{}/{}", config->GetString("raft.log_path"), node_id);
  auto log_storage = std::make_shared<dingodb::SegmentLogStorage>(log_path, node_id, 8 * 1024 * 1024, INT64_MAX);

  // std::string init_conf = FormatPeers(region->Peers());

  // build braft node
  auto node = std::make_shared<dingodb::RaftNode>(
      node_id, region->Name(), braft::PeerId(FormatLocation(peer.raft_location())), state_machine, log_storage);

  if (node->Init(region, init_conf, config->GetString("raft.path"), config->GetInt("raft.election_timeout_s") * 1000) !=
      0) {
    node->Destroy();
    return nullptr;
  }

  return node;
}

std::vector<std::shared_ptr<dingodb::RaftNode>> LaunchRaftGroup(std::shared_ptr<dingodb::Config> config,
                                                                dingodb::store::RegionPtr region) {
  std::vector<std::shared_ptr<dingodb::RaftNode>> nodes;

  int count = 0;
  std::string init_conf = FormatPeers(region->Peers());
  for (auto& peer : region->Peers()) {
    auto node = LaunchRaftNode(config, region, region->Id() + (++count), peer, init_conf);
    if (node != nullptr) {
      nodes.push_back(node);
    }
  }

  return nodes;
}

class RaftNodeTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    raft_server = std::make_unique<brpc::Server>();
    ASSERT_TRUE(raft_server != nullptr);

    butil::EndPoint endpoint;
    butil::str2endpoint("127.0.0.1", 17001, &endpoint);
    ASSERT_EQ(0, braft::add_service(raft_server.get(), endpoint));

    ASSERT_EQ(0, raft_server->Start(endpoint, nullptr));

    config = std::make_shared<dingodb::YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));
  }

  static void TearDownTestSuite() {
    raft_server->Stop(0);
    raft_server->Join();

    // Clean temp checkpoint file
    // std::filesystem::remove_all(config->GetString("raft.path"));
  }

  void SetUp() override {}

  void TearDown() override {}

 public:
  static std::shared_ptr<dingodb::Config> config;
  static std::unique_ptr<brpc::Server> raft_server;
};

std::shared_ptr<dingodb::Config> RaftNodeTest::config = nullptr;
std::unique_ptr<brpc::Server> RaftNodeTest::raft_server = nullptr;

// Add one peer, from 127.0.0.1:17001:1,127.0.0.1:17001:2,127.0.0.1:17001:3
// to 127.0.0.1:17001:1,127.0.0.1:17001:2,127.0.0.1:17001:3,127.0.0.1:17001:4
// TEST_F(RaftNodeTest, AddOnePeer) {
//   // Add one peer
//   LOG(INFO) << "====Add one peer";

//   std::vector<std::string> raft_addrs = {"127.0.0.1:17001:1", "127.0.0.1:17001:2", "127.0.0.1:17001:3"};

//   auto region = BuildRegion(1000, "unit_test", raft_addrs);
//   auto inner_nodes = LaunchRaftGroup(config, region);

//   // launch new peer
//   dingodb::pb::common::Peer peer;
//   auto* raft_loc = peer.mutable_raft_location();
//   raft_loc->set_host("127.0.0.1");
//   raft_loc->set_port(17001);
//   raft_loc->set_index(4);

//   auto new_node = LaunchRaftNode(config, region, 20001, peer, "");
//   inner_nodes.push_back(new_node);

//   bthread_usleep(3 * 1000 * 1000L);

//   // Change peers
//   LOG(INFO) << "====Change peer";
//   std::vector<dingodb::pb::common::Peer> pb_peers;
//   {
//     dingodb::pb::common::Peer pb_peer;
//     auto* raft_location = pb_peer.mutable_raft_location();
//     raft_location->set_host("127.0.0.1");
//     raft_location->set_port(17001);
//     raft_location->set_index(1);
//     pb_peers.push_back(pb_peer);
//   }
//   {
//     dingodb::pb::common::Peer pb_peer;
//     auto* raft_location = pb_peer.mutable_raft_location();
//     raft_location->set_host("127.0.0.1");
//     raft_location->set_port(17001);
//     raft_location->set_index(2);
//     pb_peers.push_back(pb_peer);
//   }
//   {
//     dingodb::pb::common::Peer pb_peer;
//     auto* raft_location = pb_peer.mutable_raft_location();
//     raft_location->set_host("127.0.0.1");
//     raft_location->set_port(17001);
//     raft_location->set_index(3);
//     pb_peers.push_back(pb_peer);
//   }
//   {
//     dingodb::pb::common::Peer pb_peer;
//     auto* raft_location = pb_peer.mutable_raft_location();
//     raft_location->set_host("127.0.0.1");
//     raft_location->set_port(17001);
//     raft_location->set_index(4);
//     pb_peers.push_back(pb_peer);
//   }

//   for (auto& node : inner_nodes) {
//     if (node->IsLeader()) {
//       node->ChangePeers(pb_peers, nullptr);
//     }
//   }

//   bthread_usleep(3 * 1000 * 1000L);

//   LOG(INFO) << "====Watch peer";

//   for (auto& node : inner_nodes) {
//     LOG(INFO) << "region id: " << node->GetNodeId() << " leader id: " << node->GetLeaderId().to_string() <<
//     '\n'; std::vector<braft::PeerId> tmp_peers; node->ListPeers(&tmp_peers); for (auto& peer : tmp_peers) {
//       LOG(INFO) << "region id: " << node->GetNodeId() << " peer: " << peer.to_string();
//     }
//   }

//   bthread_usleep(5 * 1000 * 1000L);
//   for (auto& node : inner_nodes) {
//     node->Destroy();
//   }

//   inner_nodes.clear();
// }

TEST_F(RaftNodeTest, DeleteOnePeer) {
  // Add one peer
  LOG(INFO) << "====Delete one peer";

  std::vector<std::string> raft_addrs = {"127.0.0.1:17001:1", "127.0.0.1:17001:2", "127.0.0.1:17001:3",
                                         "127.0.0.1:17001:4"};

  auto region = BuildRegion(1000, "unit_test", raft_addrs);
  auto inner_nodes = LaunchRaftGroup(config, region);

  bthread_usleep(5 * 1000 * 1000L);

  // Change peers
  LOG(INFO) << "====Change peer";
  std::vector<dingodb::pb::common::Peer> pb_peers;
  {
    dingodb::pb::common::Peer pb_peer;
    auto* raft_location = pb_peer.mutable_raft_location();
    raft_location->set_host("127.0.0.1");
    raft_location->set_port(17001);
    raft_location->set_index(1);
    pb_peers.push_back(pb_peer);
  }
  {
    dingodb::pb::common::Peer pb_peer;
    auto* raft_location = pb_peer.mutable_raft_location();
    raft_location->set_host("127.0.0.1");
    raft_location->set_port(17001);
    raft_location->set_index(2);
    pb_peers.push_back(pb_peer);
  }
  {
    dingodb::pb::common::Peer pb_peer;
    auto* raft_location = pb_peer.mutable_raft_location();
    raft_location->set_host("127.0.0.1");
    raft_location->set_port(17001);
    raft_location->set_index(3);
    pb_peers.push_back(pb_peer);
  }

  for (auto& node : inner_nodes) {
    if (node->IsLeader()) {
      node->ChangePeers(pb_peers, nullptr);
    }
  }

  bthread_usleep(3 * 1000 * 1000L);

  LOG(INFO) << "====Watch peer";

  for (auto& node : inner_nodes) {
    LOG(INFO) << "region id: " << node->GetNodeId() << " leader id: " << node->GetLeaderId().to_string();
    std::vector<braft::PeerId> tmp_peers;
    node->ListPeers(&tmp_peers);
    for (auto& peer : tmp_peers) {
      LOG(INFO) << "region id: " << node->GetNodeId() << " peer: " << peer.to_string();
    }

    auto status = node->GetStatus();
    if (status != nullptr) {
      LOG(INFO) << "region id: " << node->GetNodeId() << " status: " << status->ShortDebugString();
    }
  }

  bthread_usleep(5 * 1000 * 1000L);
  for (auto& node : inner_nodes) {
    node->Destroy();
  }

  inner_nodes.clear();
}