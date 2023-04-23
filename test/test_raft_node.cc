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

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "braft/configuration.h"
#include "butil/endpoint.h"
#include "butil/strings/string_split.h"
#include "butil/strings/stringprintf.h"
#include "common/helper.h"
#include "config/yaml_config.h"
#include "event/store_state_machine_event.h"
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
    "  heartbeatInterval: 10000 # ms\n"
    "raft:\n"
    "  host: 127.0.0.1\n"
    "  port: 23100\n"
    "  path: /tmp/dingo-store/data/store/raft\n"
    "  electionTimeout: 1000 # ms\n"
    "  snapshotInterval: 3600 # s\n"
    "log:\n"
    "  logPath: /tmp/dingo-store/log\n"
    "store:\n"
    "  dbPath: ./rocks_example\n"
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
    "  columnFamilies:\n"
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

struct Peer {
  dingodb::store::RegionPtr region;
  std::string addr;
};

dingodb::store::RegionPtr BuildRegion(uint64_t region_id, const std::string& raft_group_name,
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

std::vector<std::shared_ptr<dingodb::RaftNode>> BootRaftGroup(std::vector<Peer>& peers,
                                                              std::shared_ptr<dingodb::Config> config) {
  std::vector<std::shared_ptr<dingodb::RaftNode>> nodes;
  int peer_count = 0;
  for (auto& peer : peers) {
    auto region = peer.region;
    // build state machine
    auto raft_meta = dingodb::StoreRaftMeta::NewRaftMeta(region->Id());
    auto* state_machine = new dingodb::StoreStateMachine(nullptr, region, raft_meta, nullptr);
    if (!state_machine->Init()) {
      std::cout << "Init state machine failed";
      return {};
    }

    std::string init_conf;
    int i = 0;
    for (const auto& peer : region->Peers()) {
      std::string addr = butil::StringPrintf("%s:%d:%d", peer.raft_location().host().c_str(),
                                             peer.raft_location().port(), peer.raft_location().index());
      init_conf += addr;
      if (i + 1 < region->Peers().size()) {
        init_conf += ",";
      }
      ++i;
    }

    // build braft node
    auto node =
        std::make_shared<dingodb::RaftNode>(region->Id(), region->Name(), braft::PeerId(peer.addr), state_machine);

    if (node->Init(init_conf, config) != 0) {
      node->Destroy();
      break;
    }
    nodes.push_back(node);
  }

  return nodes;
}

class RaftNodeTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    raft_server = std::make_unique<brpc::Server>();

    butil::EndPoint endpoint;
    butil::str2endpoint("127.0.0.1", 17001, &endpoint);
    if (braft::add_service(raft_server.get(), endpoint) != 0) {
      std::cout << "Fail to add raft service!" << std::endl;
      return;
    }

    if (raft_server->Start(endpoint, nullptr) != 0) {
      std::cout << "Fail to start raft server!" << std::endl;
      return;
    }

    config = std::make_shared<dingodb::YamlConfig>();
    if (config->Load(kYamlConfigContent) != 0) {
      std::cout << "Load config failed" << std::endl;
      return;
    }

    std::vector<std::string> raft_addrs = {"127.0.0.1:17001:1", "127.0.0.1:17001:2", "127.0.0.1:17001:3"};
    std::vector<Peer> peers;
    int count = 0;
    for (const auto& raft_addr : raft_addrs) {
      ++count;
      Peer peer;
      peer.addr = raft_addr;
      peer.region = BuildRegion(1000 + count, "unit_test", raft_addrs);
      peers.push_back(peer);
    }
    nodes = BootRaftGroup(peers, config);

    bthread_usleep(5 * 1000 * 1000L);
  }

  static void TearDownTestSuite() {
    for (auto& node : nodes) {
      node->Destroy();
    }

    nodes.clear();

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
  static std::vector<std::shared_ptr<dingodb::RaftNode>> nodes;
};

std::shared_ptr<dingodb::Config> RaftNodeTest::config = nullptr;
std::unique_ptr<brpc::Server> RaftNodeTest::raft_server = nullptr;
std::vector<std::shared_ptr<dingodb::RaftNode>> RaftNodeTest::nodes = {};

// Add one peer, from 127.0.0.1:17001:1,127.0.0.1:17001:2,127.0.0.1:17001:3
// to 127.0.0.1:17001:1,127.0.0.1:17001:2,127.0.0.1:17001:3,127.0.0.1:17001:4
TEST_F(RaftNodeTest, AddOnePeer) {
  // Add one peer
  std::cout << "====Add one peer" << std::endl;
  std::vector<std::string> raft_addrs = {"127.0.0.1:17001:1", "127.0.0.1:17001:2", "127.0.0.1:17001:3",
                                         "127.0.0.1:17001:4"};
  std::vector<Peer> peers;
  Peer peer;
  peer.addr = "127.0.0.1:17001:4";
  peer.region = BuildRegion(1004, "unit_test", raft_addrs);
  peers.push_back(peer);

  auto new_nodes = BootRaftGroup(peers, config);
  for (auto& new_node : new_nodes) {
    RaftNodeTest::nodes.push_back(new_node);
  }

  bthread_usleep(3 * 1000 * 1000L);

  // Change peers
  std::cout << "====Change peer" << std::endl;
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
  {
    dingodb::pb::common::Peer pb_peer;
    auto* raft_location = pb_peer.mutable_raft_location();
    raft_location->set_host("127.0.0.1");
    raft_location->set_port(17001);
    raft_location->set_index(4);
    pb_peers.push_back(pb_peer);
  }

  for (auto& node : RaftNodeTest::nodes) {
    if (node->IsLeader()) {
      node->ChangePeers(pb_peers, nullptr);
    }
  }

  bthread_usleep(3 * 1000 * 1000L);

  std::cout << "====Watch peer" << std::endl;

  for (auto& node : RaftNodeTest::nodes) {
    std::cout << "region id: " << node->GetNodeId() << " leader id: " << node->GetLeaderId().to_string() << std::endl;
    std::vector<braft::PeerId> tmp_peers;
    node->ListPeers(&tmp_peers);
    for (auto& peer : tmp_peers) {
      std::cout << "region id: " << node->GetNodeId() << " peer: " << peer.to_string() << std::endl;
    }
  }

  bthread_usleep(5 * 1000 * 1000L);
}