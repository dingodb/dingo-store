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
#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>

#include "braft/configuration.h"
#include "butil/endpoint.h"
#include "butil/strings/string_split.h"
#include "butil/strings/stringprintf.h"
#include "common/helper.h"
#include "config/yaml_config.h"
#include "event/store_state_machine_event.h"
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

std::vector<std::shared_ptr<dingodb::RaftNode>> BootRaftGroup(const std::vector<std::string>& raft_addrs,
                                                              std::shared_ptr<dingodb::Config> config) {
  std::vector<std::shared_ptr<dingodb::RaftNode>> nodes;
  int peer_count = 0;
  for (const auto& addr : raft_addrs) {
    // build region
    auto region = std::make_shared<dingodb::pb::store_internal::Region>();
    region->set_id(10000 + (++peer_count));
    auto* region_definition = region->mutable_definition();
    region_definition->set_id(region->id());
    region_definition->set_name("uint-test");
    auto* range = region_definition->mutable_range();
    range->set_start_key("a");
    range->set_end_key("z");

    int count = 0;
    for (const auto& inner_addr : raft_addrs) {
      std::vector<std::string> host_port;
      butil::SplitString(inner_addr, ':', &host_port);

      auto* peer = region_definition->add_peers();
      peer->set_store_id(1000 + (++count));
      auto* raft_loc = peer->mutable_raft_location();
      raft_loc->set_host(host_port[0]);
      raft_loc->set_port(std::stoi(host_port[1]));
    }

    // build state machine
    auto raft_meta = dingodb::StoreRaftMeta::NewRaftMeta(region->id());
    auto listener_factory = std::make_shared<dingodb::StoreSmEventListenerFactory>();
    auto* state_machine = new dingodb::StoreStateMachine(nullptr, region, raft_meta, listener_factory->Build());
    if (!state_machine->Init()) {
      std::cout << "Init state machine failed";
      return {};
    }

    // build braft node
    auto node = std::make_shared<dingodb::RaftNode>(region->id(), region_definition->name(), braft::PeerId(addr),
                                                    state_machine);

    if (node->Init(GetRaftInitConf(raft_addrs), config) != 0) {
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

    std::vector<std::string> raft_addrs = {"127.0.0.1:17001:0", "127.0.0.1:17001:2", "127.0.0.1:17001:3"};
    nodes = BootRaftGroup(raft_addrs, config);

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

TEST_F(RaftNodeTest, ChangePeer) {
  std::vector<dingodb::pb::common::Peer> peers;
  dingodb::pb::common::Peer peer;
  auto* raft_location = peer.mutable_raft_location();
  raft_location->set_host("127.0.0.1");
  raft_location->set_port(17001);
  raft_location->set_index(0);
  peers.push_back(peer);

  for (auto& node : RaftNodeTest::nodes) {
    if (node->IsLeader()) {
      node->ChangePeers(peers, nullptr);
    }
  }

  bthread_usleep(3 * 1000 * 1000L);

  for (auto& node : RaftNodeTest::nodes) {
    std::cout << "leader id: " << node->GetLeaderId().to_string() << std::endl;
  }

  bthread_usleep(5 * 1000 * 1000L);
}