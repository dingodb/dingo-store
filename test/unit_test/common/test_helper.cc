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
#include <filesystem>
#include <iostream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "braft/configuration.h"
#include "butil/endpoint.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "proto/error.pb.h"
#include "server/service_helper.h"

class HelperTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(HelperTest, IsIp) {
  EXPECT_EQ(false, dingodb::Helper::IsIp(""));
  EXPECT_EQ(false, dingodb::Helper::IsIp("hello"));
  EXPECT_EQ(false, dingodb::Helper::IsIp("   "));
  EXPECT_EQ(false, dingodb::Helper::IsIp("10.10"));
  EXPECT_EQ(false, dingodb::Helper::IsIp("10.10.a.10"));
  EXPECT_EQ(true, dingodb::Helper::IsIp("10.10.10.10"));
  EXPECT_EQ(true, dingodb::Helper::IsIp("127.0.0.1"));
}

TEST_F(HelperTest, IsExecutorRaw) {
  EXPECT_EQ(false, dingodb::Helper::IsExecutorRaw(""));
  EXPECT_EQ(true, dingodb::Helper::IsExecutorRaw("rhello"));
  EXPECT_EQ(false, dingodb::Helper::IsExecutorRaw("hello"));
}
TEST_F(HelperTest, IsExecutorTxn) {
  EXPECT_EQ(false, dingodb::Helper::IsExecutorTxn(""));
  EXPECT_EQ(true, dingodb::Helper::IsExecutorTxn("thello"));
  EXPECT_EQ(false, dingodb::Helper::IsExecutorTxn("hello"));
}

TEST_F(HelperTest, IsClientRaw) {
  EXPECT_EQ(false, dingodb::Helper::IsClientRaw(""));
  EXPECT_EQ(true, dingodb::Helper::IsClientRaw("whello"));
  EXPECT_EQ(false, dingodb::Helper::IsClientRaw("hello"));
}

TEST_F(HelperTest, IsClientTxn) {
  EXPECT_EQ(false, dingodb::Helper::IsClientTxn(""));
  EXPECT_EQ(true, dingodb::Helper::IsClientTxn("xhello"));
  EXPECT_EQ(false, dingodb::Helper::IsClientTxn("hello"));
}

TEST_F(HelperTest, Ip2HostName) {
  EXPECT_EQ("", dingodb::Helper::Ip2HostName(""));

  EXPECT_EQ("localhost", dingodb::Helper::Ip2HostName("127.0.0.1"));

  EXPECT_EQ("localhost", dingodb::Helper::Ip2HostName("localhost"));
}

TEST_F(HelperTest, StringToLocation) {
  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");
    auto actual = dingodb::Helper::StringToLocation("127.0.0.1");

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");
    expect.set_port(80);
    auto actual = dingodb::Helper::StringToLocation("127.0.0.1:80");

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("localhost");
    expect.set_port(80);
    auto actual = dingodb::Helper::StringToLocation("localhost:80");

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");
    expect.set_port(80);
    expect.set_index(11);
    auto actual = dingodb::Helper::StringToLocation("127.0.0.1:80:11");

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }

  {
    dingodb::pb::common::Location expect;
    auto actual = dingodb::Helper::StringToLocation("");

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }
}

TEST_F(HelperTest, StringToLocation2) {
  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");
    auto actual = dingodb::Helper::StringToLocation("127.0.0.1", 0);

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");
    expect.set_port(80);
    auto actual = dingodb::Helper::StringToLocation("127.0.0.1", 80);

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("localhost");
    expect.set_port(80);
    auto actual = dingodb::Helper::StringToLocation("localhost", 80);

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");
    expect.set_port(80);
    expect.set_index(11);
    auto actual = dingodb::Helper::StringToLocation("127.0.0.1", 80, 11);

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_port(80);
    auto actual = dingodb::Helper::StringToLocation("", 80);

    EXPECT_EQ(expect.host(), actual.host());
    EXPECT_EQ(expect.port(), actual.port());
    EXPECT_EQ(expect.index(), actual.index());
  }
}

TEST_F(HelperTest, LocationToString) {
  {
    dingodb::pb::common::Location location;
    location.set_host("127.0.0.1");
    location.set_port(80);
    location.set_index(11);
    EXPECT_EQ("127.0.0.1:80:11", dingodb::Helper::LocationToString(location));
  }
  {
    dingodb::pb::common::Location location;
    location.set_host("127.0.0.1");
    location.set_port(80);
    EXPECT_EQ("127.0.0.1:80:0", dingodb::Helper::LocationToString(location));
  }
  {
    dingodb::pb::common::Location location;
    location.set_host("127.0.0.1");
    location.set_port(0);
    location.set_index(11);
    EXPECT_EQ("127.0.0.1:0:11", dingodb::Helper::LocationToString(location));
  }
  {
    dingodb::pb::common::Location location;
    location.set_host("localhost");
    location.set_port(80);
    location.set_index(11);
    EXPECT_EQ("localhost:80:11", dingodb::Helper::LocationToString(location));
  }
}

TEST_F(HelperTest, LocationsToString) {
  {
    std::vector<dingodb::pb::common::Location> locations;
    {
      dingodb::pb::common::Location location;
      location.set_host("127.0.0.1");
      location.set_port(80);
      location.set_index(11);
      locations.push_back(location);
    }

    {
      dingodb::pb::common::Location location;
      location.set_host("127.0.0.2");
      location.set_port(80);
      location.set_index(11);
      locations.push_back(location);
    }

    {
      dingodb::pb::common::Location location;
      location.set_host("127.0.0.3");
      location.set_port(80);
      location.set_index(11);
      locations.push_back(location);
    }

    EXPECT_EQ("127.0.0.1:80:11,127.0.0.2:80:11,127.0.0.3:80:11", dingodb::Helper::LocationsToString(locations));
  }
}

TEST_F(HelperTest, StringToEndPoint) {
  {
    auto endpoint = dingodb::Helper::StringToEndPoint("", 0);
    EXPECT_EQ(butil::IP_ANY, endpoint.ip);
    EXPECT_EQ(0, endpoint.port);
  }

  {
    auto endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1", 80);
    EXPECT_EQ(0, strcmp("127.0.0.1", butil::ip2str(endpoint.ip).c_str()));
    EXPECT_EQ(80, endpoint.port);
  }

  {
    auto endpoint = dingodb::Helper::StringToEndPoint("localhost", 80);
    EXPECT_EQ(0, strcmp("127.0.0.1", butil::ip2str(endpoint.ip).c_str()));
    EXPECT_EQ(80, endpoint.port);
  }
}

TEST_F(HelperTest, StringToEndPoint2) {
  {
    auto endpoint = dingodb::Helper::StringToEndPoint("");
    EXPECT_EQ(butil::IP_ANY, endpoint.ip);
    EXPECT_EQ(0, endpoint.port);
  }

  {
    auto endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1");
    EXPECT_EQ(0, strcmp("127.0.0.1", butil::ip2str(endpoint.ip).c_str()));
    EXPECT_EQ(0, endpoint.port);
  }

  {
    auto endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1:80");
    EXPECT_EQ(0, strcmp("127.0.0.1", butil::ip2str(endpoint.ip).c_str()));
    EXPECT_EQ(80, endpoint.port);
  }

  {
    auto endpoint = dingodb::Helper::StringToEndPoint("localhost:80");
    EXPECT_EQ(0, strcmp("127.0.0.1", butil::ip2str(endpoint.ip).c_str()));
    EXPECT_EQ(80, endpoint.port);
  }

  {
    auto endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1:80:10");
    EXPECT_EQ(0, strcmp("127.0.0.1", butil::ip2str(endpoint.ip).c_str()));
    EXPECT_EQ(80, endpoint.port);
  }
}

TEST_F(HelperTest, StringToEndpoints) {
  std::vector<butil::EndPoint> expect = {
      dingodb::Helper::StringToEndPoint("127.0.0.1:80"), dingodb::Helper::StringToEndPoint("127.0.0.1:81:11"),
      dingodb::Helper::StringToEndPoint("192.168.0.1:80"), dingodb::Helper::StringToEndPoint("192.168.0.2:80:12")};

  auto actual = dingodb::Helper::StringToEndpoints("127.0.0.1:80,127.0.0.1:81:11,192.168.0.1:80,192.168.0.2:80:12");
  ASSERT_EQ(4, actual.size());
  EXPECT_EQ(false, dingodb::Helper::IsDifferenceEndPoint(expect[0], actual[0]));
  EXPECT_EQ(false, dingodb::Helper::IsDifferenceEndPoint(expect[1], actual[1]));
  EXPECT_EQ(false, dingodb::Helper::IsDifferenceEndPoint(expect[2], actual[2]));
  EXPECT_EQ(false, dingodb::Helper::IsDifferenceEndPoint(expect[3], actual[3]));
}

TEST_F(HelperTest, EndPointToString) {
  EXPECT_EQ("127.0.0.1:80", dingodb::Helper::EndPointToString(dingodb::Helper::StringToEndPoint("127.0.0.1:80")));

  EXPECT_EQ("127.0.0.1:0", dingodb::Helper::EndPointToString(dingodb::Helper::StringToEndPoint("127.0.0.1")));

  EXPECT_EQ("127.0.0.1:80", dingodb::Helper::EndPointToString(dingodb::Helper::StringToEndPoint("127.0.0.1:80:11")));

  EXPECT_EQ("0.0.0.0:0", dingodb::Helper::EndPointToString(dingodb::Helper::StringToEndPoint("")));
}

TEST_F(HelperTest, StringToPeerId) {}

TEST_F(HelperTest, StringToPeerId2) {}

TEST_F(HelperTest, PeerIdToLocation) {
  {
    butil::EndPoint endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1:80");
    dingodb::pb::common::Location location = dingodb::Helper::PeerIdToLocation(braft::PeerId(endpoint));
    EXPECT_EQ("127.0.0.1", location.host());
    EXPECT_EQ(80, location.port());
  }

  {
    butil::EndPoint endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1");
    std::cout << "endpoint: " << butil::ip2str(endpoint.ip) << '\n';
    dingodb::pb::common::Location location = dingodb::Helper::PeerIdToLocation(braft::PeerId(endpoint));
    EXPECT_EQ("127.0.0.1", location.host());
    EXPECT_EQ(0, location.port());
  }

  {
    butil::EndPoint endpoint = dingodb::Helper::StringToEndPoint("localhost:80");
    dingodb::pb::common::Location location = dingodb::Helper::PeerIdToLocation(braft::PeerId(endpoint));
    EXPECT_EQ("127.0.0.1", location.host());
    EXPECT_EQ(80, location.port());
  }
}

TEST_F(HelperTest, PeerIdsToString) {
  {
    std::vector<braft::PeerId> peer_ids = {dingodb::Helper::StringToPeerId("127.0.0.1:80")};
    EXPECT_EQ("127.0.0.1:80:0:0", dingodb::Helper::PeerIdsToString(peer_ids));
  }
  {
    std::vector<braft::PeerId> peer_ids = {dingodb::Helper::StringToPeerId("127.0.0.1:80:11")};
    EXPECT_EQ("127.0.0.1:80:11:0", dingodb::Helper::PeerIdsToString(peer_ids));
  }
  {
    std::vector<braft::PeerId> peer_ids = {dingodb::Helper::StringToPeerId("127.0.0.1:80"),
                                           dingodb::Helper::StringToPeerId("127.0.0.2:81")};
    EXPECT_EQ("127.0.0.1:80:0:0,127.0.0.2:81:0:0", dingodb::Helper::PeerIdsToString(peer_ids));
  }
  {
    std::vector<braft::PeerId> peer_ids = {dingodb::Helper::StringToPeerId("127.0.0.1:80:10"),
                                           dingodb::Helper::StringToPeerId("127.0.0.2:81:11")};
    EXPECT_EQ("127.0.0.1:80:10:0,127.0.0.2:81:11:0", dingodb::Helper::PeerIdsToString(peer_ids));
  }
}

TEST_F(HelperTest, LocationToEndPoint) {
  {
    butil::EndPoint expect = dingodb::Helper::StringToEndPoint("127.0.0.1:80");
    dingodb::pb::common::Location location;
    location.set_host("127.0.0.3");
    location.set_port(80);
    location.set_index(11);

    auto actual = dingodb::Helper::LocationToEndPoint(location);
    EXPECT_EQ(true, dingodb::Helper::IsDifferenceEndPoint(expect, actual));
  }

  {
    butil::EndPoint expect = dingodb::Helper::StringToEndPoint("127.0.0.1:80");
    dingodb::pb::common::Location location;
    location.set_host("127.0.0.1");
    location.set_port(80);

    auto actual = dingodb::Helper::LocationToEndPoint(location);
    EXPECT_EQ(false, dingodb::Helper::IsDifferenceEndPoint(expect, actual));
  }

  {
    butil::EndPoint expect = dingodb::Helper::StringToEndPoint("127.0.0.1:80:11");
    dingodb::pb::common::Location location;
    location.set_host("127.0.0.1");
    location.set_port(80);
    location.set_index(11);

    auto actual = dingodb::Helper::LocationToEndPoint(location);
    EXPECT_EQ(false, dingodb::Helper::IsDifferenceEndPoint(expect, actual));
  }
}

TEST_F(HelperTest, EndPointToLocation) {
  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");
    expect.set_port(80);
    expect.set_index(0);

    butil::EndPoint endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1:80");
    auto actual = dingodb::Helper::EndPointToLocation(endpoint);

    EXPECT_EQ(false, dingodb::Helper::IsDifferenceLocation(expect, actual));
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");
    expect.set_port(80);
    expect.set_index(11);

    butil::EndPoint endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1:80:11");
    auto actual = dingodb::Helper::EndPointToLocation(endpoint);

    EXPECT_EQ(false, dingodb::Helper::IsDifferenceLocation(expect, actual));
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");

    butil::EndPoint endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1");
    auto actual = dingodb::Helper::EndPointToLocation(endpoint);

    EXPECT_EQ(false, dingodb::Helper::IsDifferenceLocation(expect, actual));
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.2");

    butil::EndPoint endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1");
    auto actual = dingodb::Helper::EndPointToLocation(endpoint);

    EXPECT_EQ(true, dingodb::Helper::IsDifferenceLocation(expect, actual));
  }

  {
    dingodb::pb::common::Location expect;
    expect.set_host("127.0.0.1");
    expect.set_port(80);

    butil::EndPoint endpoint = dingodb::Helper::StringToEndPoint("127.0.0.1:81");
    auto actual = dingodb::Helper::EndPointToLocation(endpoint);

    EXPECT_EQ(true, dingodb::Helper::IsDifferenceLocation(expect, actual));
  }
}

TEST_F(HelperTest, LocationToPeerId) {
  {
    auto location = dingodb::Helper::StringToLocation("127.0.0.1:80:11");
    EXPECT_TRUE(dingodb::Helper::StringToPeerId("127.0.0.1:80:11") == dingodb::Helper::LocationToPeerId(location));
  }

  {
    auto location = dingodb::Helper::StringToLocation("127.0.0.1:80");
    EXPECT_TRUE(dingodb::Helper::StringToPeerId("127.0.0.1:80") == dingodb::Helper::LocationToPeerId(location));
  }

  {
    auto location = dingodb::Helper::StringToLocation("127.0.0.1");
    EXPECT_TRUE(dingodb::Helper::StringToPeerId("127.0.0.1") == dingodb::Helper::LocationToPeerId(location));
  }

  {
    auto location = dingodb::Helper::StringToLocation("127.0.0.1:80:11");
    EXPECT_TRUE(dingodb::Helper::StringToPeerId("127.0.0.2:80:11") != dingodb::Helper::LocationToPeerId(location));
  }

  {
    auto location = dingodb::Helper::StringToLocation("127.0.0.1:80:11");
    EXPECT_TRUE(dingodb::Helper::StringToPeerId("127.0.0.1:81:11") != dingodb::Helper::LocationToPeerId(location));
  }

  {
    auto location = dingodb::Helper::StringToLocation("127.0.0.1:80:11");
    EXPECT_TRUE(dingodb::Helper::StringToPeerId("127.0.0.1:80:12") != dingodb::Helper::LocationToPeerId(location));
  }
}

TEST_F(HelperTest, IsDifferenceLocation) {
  {
    dingodb::pb::common::Location location1;
    location1.set_host("127.0.0.1");
    location1.set_port(80);
    dingodb::pb::common::Location location2;
    location2.set_host("127.0.0.1");
    location2.set_port(80);
    EXPECT_EQ(false, dingodb::Helper::IsDifferenceLocation(location1, location2));
  }

  {
    dingodb::pb::common::Location location1;
    location1.set_host("127.0.0.1");
    location1.set_port(90);
    dingodb::pb::common::Location location2;
    location2.set_host("127.0.0.1");
    location2.set_port(80);
    EXPECT_EQ(true, dingodb::Helper::IsDifferenceLocation(location1, location2));
  }

  {
    dingodb::pb::common::Location location1;
    location1.set_host("10.10.10.10");
    location1.set_port(80);
    dingodb::pb::common::Location location2;
    location2.set_host("127.0.0.1");
    location2.set_port(80);
    EXPECT_EQ(true, dingodb::Helper::IsDifferenceLocation(location1, location2));
  }
}

TEST_F(HelperTest, IsDifferencePeers) {
  std::vector<dingodb::pb::common::Peer> peers;

  {
    dingodb::pb::common::Peer peer;
    peer.set_store_id(3);
    peer.mutable_raft_location()->set_host("127.0.0.1");
    peer.mutable_raft_location()->set_port(80);
    peers.push_back(peer);
  }
  {
    dingodb::pb::common::Peer peer;
    peer.set_store_id(2);
    peer.mutable_raft_location()->set_host("192.168.0.1");
    peer.mutable_raft_location()->set_port(80);
    peers.push_back(peer);
  }
  {
    dingodb::pb::common::Peer peer;
    peer.set_store_id(1);
    peer.mutable_raft_location()->set_host("127.0.0.1");
    peer.mutable_raft_location()->set_port(81);
    peers.push_back(peer);
  }

  EXPECT_EQ(false, dingodb::Helper::IsDifferencePeers(peers, peers));

  std::vector<dingodb::pb::common::Peer> peers2;

  {
    dingodb::pb::common::Peer peer;
    peer.set_store_id(3);
    peer.mutable_raft_location()->set_host("128.0.0.1");
    peer.mutable_raft_location()->set_port(80);
    peers2.push_back(peer);
  }
  {
    dingodb::pb::common::Peer peer;
    peer.set_store_id(2);
    peer.mutable_raft_location()->set_host("193.168.0.1");
    peer.mutable_raft_location()->set_port(80);
    peers2.push_back(peer);
  }
  {
    dingodb::pb::common::Peer peer;
    peer.set_store_id(1);
    peer.mutable_raft_location()->set_host("127.0.0.1");
    peer.mutable_raft_location()->set_port(81);
    peers2.push_back(peer);
  }

  EXPECT_EQ(true, dingodb::Helper::IsDifferencePeers(peers, peers2));
}

TEST_F(HelperTest, IsDifferencePeers2) {
  {
    dingodb::pb::common::RegionDefinition definition1;
    definition1.add_peers()->set_store_id(3);
    definition1.add_peers()->set_store_id(6);
    definition1.add_peers()->set_store_id(9);

    dingodb::pb::common::RegionDefinition definition2;
    definition2.add_peers()->set_store_id(3);
    definition2.add_peers()->set_store_id(6);
    definition2.add_peers()->set_store_id(9);

    EXPECT_EQ(false, dingodb::Helper::IsDifferencePeers(definition1, definition2));
  }

  {
    dingodb::pb::common::RegionDefinition definition1;
    definition1.add_peers()->set_store_id(3);
    definition1.add_peers()->set_store_id(6);
    definition1.add_peers()->set_store_id(9);

    dingodb::pb::common::RegionDefinition definition2;
    definition2.add_peers()->set_store_id(3);
    definition2.add_peers()->set_store_id(6);
    definition2.add_peers()->set_store_id(8);

    EXPECT_EQ(true, dingodb::Helper::IsDifferencePeers(definition1, definition2));
  }

  {
    dingodb::pb::common::RegionDefinition definition1;
    definition1.add_peers()->set_store_id(3);
    definition1.add_peers()->set_store_id(6);
    definition1.add_peers()->set_store_id(9);

    dingodb::pb::common::RegionDefinition definition2;
    definition2.add_peers()->set_store_id(3);
    definition2.add_peers()->set_store_id(6);

    EXPECT_EQ(true, dingodb::Helper::IsDifferencePeers(definition1, definition2));
  }
}

TEST_F(HelperTest, SortPeers) {
  std::vector<dingodb::pb::common::Peer> peers;
  {
    dingodb::pb::common::Peer peer;
    peer.set_store_id(3);
    peers.push_back(peer);
  }
  {
    dingodb::pb::common::Peer peer;
    peer.set_store_id(2);
    peers.push_back(peer);
  }
  {
    dingodb::pb::common::Peer peer;
    peer.set_store_id(1);
    peers.push_back(peer);
  }

  dingodb::Helper::SortPeers(peers);

  ASSERT_EQ(3, peers.size());
  ASSERT_EQ(1, peers[0].store_id());
  ASSERT_EQ(2, peers[1].store_id());
  ASSERT_EQ(3, peers[2].store_id());
}

TEST_F(HelperTest, ExtractLocations) {}

TEST_F(HelperTest, ExtractLocations2) {}

TEST_F(HelperTest, FormatTime) {
  auto format_time = dingodb::Helper::FormatTime(1681970908, "%Y-%m-%d %H:%M:%S");
  LOG(INFO) << format_time;

  EXPECT_EQ("2023-04-20 14:08:28", format_time);

  auto format_ms_time = dingodb::Helper::FormatMsTime(1681970908001, "%Y-%m-%d %H:%M:%S");
  LOG(INFO) << format_ms_time;

  EXPECT_EQ("2023-04-20 14:08:28.1", format_ms_time);
}

TEST_F(HelperTest, TransformRangeWithOptions) {
  dingodb::pb::common::Range region_range;
  char start_key[] = {0x61, 0x64};
  char end_key[] = {0x78, 0x65};
  region_range.set_start_key(start_key, 2);
  region_range.set_end_key(end_key, 2);

  {
    // [0x61, 0x78]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61};
    char end_key[] = {0x78};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x61, 0x78)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61};
    char end_key[] = {0x78};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x61, 0x78]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61};
    char end_key[] = {0x78};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x61, 0x78)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61};
    char end_key[] = {0x78};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x60, 0x77]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x60};
    char end_key[] = {0x77};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x60, 0x77]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x60};
    char end_key[] = {0x77};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x60, 0x77)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x60};
    char end_key[] = {0x77};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x60, 0x77)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x60};
    char end_key[] = {0x77};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x62, 0x79)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x62};
    char end_key[] = {0x79};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  // ==================================================
  {
    // [0x6164, 0x7865]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x6164, 0x7865)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x6164, 0x7865]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x6164, 0x7865)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x6163, 0x7865]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x63};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x6163, 0x7865)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x63};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  // ========================================
  {
    // [0x616461, 0x786563]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x65, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x616461, 0x786563)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x65, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x616461, 0x786563]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x65, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x616461, 0x786563)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x65, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }
  {
    // [0x616461, 0x786463]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x64, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }
}

TEST_F(HelperTest, ToUpper) {
  std::string value = "hello world";
  EXPECT_EQ("HELLO WORLD", dingodb::Helper::ToUpper(value));
}

TEST_F(HelperTest, ToLower) {
  std::string value = "HELLO WORLD";
  EXPECT_EQ("hello world", dingodb::Helper::ToLower(value));
}

TEST_F(HelperTest, Ltrim) {
  EXPECT_EQ("hello", dingodb::Helper::Ltrim("hello", ""));

  EXPECT_EQ(" hello", dingodb::Helper::Ltrim(" hello", ""));

  EXPECT_EQ("hello", dingodb::Helper::Ltrim(" hello", " "));

  EXPECT_EQ("hello", dingodb::Helper::Ltrim("aaahello", "a"));

  EXPECT_EQ("hello", dingodb::Helper::Ltrim("...hello", "."));

  EXPECT_EQ("hello", dingodb::Helper::Ltrim("ababhello", "ab"));
}

TEST_F(HelperTest, Rtrim) {
  EXPECT_EQ("hello", dingodb::Helper::Rtrim("hello", ""));

  EXPECT_EQ("hello ", dingodb::Helper::Rtrim("hello ", ""));

  EXPECT_EQ("hello", dingodb::Helper::Rtrim("hello ", " "));

  EXPECT_EQ("hello", dingodb::Helper::Rtrim("helloaaa", "a"));

  EXPECT_EQ("hello", dingodb::Helper::Rtrim("hello...", "."));

  EXPECT_EQ("hello", dingodb::Helper::Rtrim("helloabab", "ab"));
}

TEST_F(HelperTest, Trim) {
  EXPECT_EQ("hello", dingodb::Helper::Trim("hello", ""));

  EXPECT_EQ(" hello ", dingodb::Helper::Trim(" hello ", ""));

  EXPECT_EQ("hello", dingodb::Helper::Trim(" hello ", " "));

  EXPECT_EQ("hello", dingodb::Helper::Trim("aaahelloaaa", "a"));

  EXPECT_EQ("hello", dingodb::Helper::Trim("...hello...", "."));

  EXPECT_EQ("hello", dingodb::Helper::Trim("ababhelloabab", "ab"));
}

TEST_F(HelperTest, TraverseDirectory) {
  std::string path = "/tmp/unit_test_traverse_directory";

  std::filesystem::create_directories(path);
  std::filesystem::create_directories(fmt::format("{}/a1", path));
  std::filesystem::create_directories(fmt::format("{}/a1/b1", path));
  std::filesystem::create_directories(fmt::format("{}/a1/b2", path));
  std::filesystem::create_directories(fmt::format("{}/a2", path));
  std::filesystem::create_directories(fmt::format("{}/a2/b3", path));

  auto filenames = dingodb::Helper::TraverseDirectory(path);
  for (const auto& filename : filenames) {
    LOG(INFO) << "filename: " << filename;
  }

  std::filesystem::remove_all(path);
  EXPECT_EQ(2, filenames.size());
}

TEST_F(HelperTest, TraverseDirectoryByPrefix) {
  std::string path = "/tmp/unit_test_traverse_directory_prefix";

  std::filesystem::create_directories(path);
  std::filesystem::create_directories(fmt::format("{}/a1", path));
  std::filesystem::create_directories(fmt::format("{}/a1/b1", path));
  std::filesystem::create_directories(fmt::format("{}/a1/b2", path));
  std::filesystem::create_directories(fmt::format("{}/a2", path));
  std::filesystem::create_directories(fmt::format("{}/a2/b3", path));

  auto filenames = dingodb::Helper::TraverseDirectory(path, std::string("a1"));
  for (const auto& filename : filenames) {
    LOG(INFO) << "filename: " << filename;
  }

  std::filesystem::remove_all(path);
  EXPECT_EQ(1, filenames.size());
}

TEST_F(HelperTest, GetSystemMemoryInfo) {
  std::map<std::string, int64_t> output;
  auto ret = dingodb::Helper::GetSystemMemoryInfo(output);
  EXPECT_EQ(true, ret);

  for (auto& it : output) {
    LOG(INFO) << it.first << ": " << it.second;
  }
}

TEST_F(HelperTest, GetSystemCpuUsage) {
  std::map<std::string, int64_t> output;
  auto ret = dingodb::Helper::GetSystemCpuUsage(output);
  EXPECT_EQ(true, ret);

  for (auto& it : output) {
    LOG(INFO) << it.first << ": " << it.second;
  }
}

TEST_F(HelperTest, GetSystemDiskIoUtil) {
  GTEST_SKIP() << "Skip GetSystemDiskIoUtil...";

  std::map<std::string, int64_t> output;
  std::string device_name = "sda";
  auto ret = dingodb::Helper::GetSystemDiskIoUtil(device_name, output);
  EXPECT_EQ(true, ret);

  for (auto& it : output) {
    LOG(INFO) << it.first << ": " << it.second;
  }
}

TEST_F(HelperTest, GetProcessMemoryInfo) {
  std::map<std::string, int64_t> output;
  auto ret = dingodb::Helper::GetProcessMemoryInfo(output);
  EXPECT_EQ(true, ret);

  for (auto& it : output) {
    LOG(INFO) << it.first << ": " << it.second;
  }
}

TEST_F(HelperTest, CleanFirstSlash) {
  EXPECT_EQ("", dingodb::Helper::CleanFirstSlash(""));
  EXPECT_EQ("hello.txt", dingodb::Helper::CleanFirstSlash("hello.txt"));
  EXPECT_EQ("hello.txt", dingodb::Helper::CleanFirstSlash("/hello.txt"));
}

TEST_F(HelperTest, PaddingUserKey) {
  std::string a = "abc";
  std::string pa = dingodb::Helper::HexToString("6162630000000000FA");

  EXPECT_EQ(pa, dingodb::Helper::PaddingUserKey(a));
  EXPECT_EQ(a, dingodb::Helper::UnpaddingUserKey(pa));

  a = "aaaabbbb";
  pa = dingodb::Helper::HexToString("6161616162626262FF0000000000000000F7");

  EXPECT_EQ(pa, dingodb::Helper::PaddingUserKey(a));
  EXPECT_EQ(a, dingodb::Helper::UnpaddingUserKey(pa));

  a = "aaaabbbbc";
  pa = dingodb::Helper::HexToString("6161616162626262FF6300000000000000F8");

  EXPECT_EQ(pa, dingodb::Helper::PaddingUserKey(a));
  EXPECT_EQ(a, dingodb::Helper::UnpaddingUserKey(pa));

  a = "aaaabbbbaaaabbbbcc";
  pa = dingodb::Helper::HexToString("6161616162626262FF6161616162626262FF6363000000000000F9");

  EXPECT_EQ(pa, dingodb::Helper::PaddingUserKey(a));
  EXPECT_EQ(a, dingodb::Helper::UnpaddingUserKey(pa));
}

TEST_F(HelperTest, IsContinuous) {
  // Test with an empty set
  std::set<int64_t> empty_set;
  EXPECT_TRUE(dingodb::Helper::IsContinuous(empty_set));

  // Test with a set of continuous numbers
  std::set<int64_t> continuous_set = {1, 2, 3, 4, 5};
  EXPECT_TRUE(dingodb::Helper::IsContinuous(continuous_set));

  // Test with a set of non-continuous numbers
  std::set<int64_t> non_continuous_set = {1, 2, 4, 5};
  EXPECT_FALSE(dingodb::Helper::IsContinuous(non_continuous_set));

  // Test with a large set of continuous numbers
  std::set<int64_t> large_continuous_set;
  for (int64_t i = 0; i < 10000; ++i) {
    large_continuous_set.insert(i);
  }
  EXPECT_TRUE(dingodb::Helper::IsContinuous(large_continuous_set));

  // Test with a large set of non-continuous numbers
  std::set<int64_t> large_non_continuous_set;
  for (int64_t i = 0; i < 10000; ++i) {
    if (i != 5000) {  // Skip one number to make the set non-continuous
      large_non_continuous_set.insert(i);
    }
  }
  EXPECT_FALSE(dingodb::Helper::IsContinuous(large_non_continuous_set));

  // Test with a set of continuous negative numbers
  std::set<int64_t> negative_continuous_set = {-5, -4, -3, -2, -1, 0};
  EXPECT_TRUE(dingodb::Helper::IsContinuous(negative_continuous_set));

  // Test with a set of non-continuous negative numbers
  std::set<int64_t> negative_non_continuous_set = {-5, -4, -2, -1, 0};
  EXPECT_FALSE(dingodb::Helper::IsContinuous(negative_non_continuous_set));

  // Test with a set of continuous numbers that includes both positive and negative numbers
  std::set<int64_t> mixed_continuous_set = {-2, -1, 0, 1, 2};
  EXPECT_TRUE(dingodb::Helper::IsContinuous(mixed_continuous_set));

  // Test with a set of non-continuous numbers that includes both positive and negative numbers
  std::set<int64_t> mixed_non_continuous_set = {-2, -1, 1, 2};
  EXPECT_FALSE(dingodb::Helper::IsContinuous(mixed_non_continuous_set));
}

TEST_F(HelperTest, IsConflictRange) {
  // Test with two ranges that do not intersect
  dingodb::pb::common::Range range1;
  range1.set_start_key("hello");
  range1.set_end_key("hello0000");
  dingodb::pb::common::Range range2;
  range2.set_start_key("hello0000");
  range2.set_end_key("hello00000000");
  EXPECT_FALSE(dingodb::Helper::IsConflictRange(range1, range2));

  // Test with two ranges that intersect
  range2.set_start_key("hello000");
  range2.set_end_key("hello000000");
  EXPECT_TRUE(dingodb::Helper::IsConflictRange(range1, range2));

  // Test with two ranges that are the same
  range2.set_start_key("hello");
  range2.set_end_key("hello0000");
  EXPECT_TRUE(dingodb::Helper::IsConflictRange(range1, range2));

  // Test with two ranges where one is inside the other
  range2.set_start_key("hello0");
  range2.set_end_key("hello000");
  EXPECT_TRUE(dingodb::Helper::IsConflictRange(range1, range2));

  // Test with a large number of ranges
  for (int i = 0; i < 1000; ++i) {
    range1.set_start_key("hello" + std::to_string(i));
    range1.set_end_key("hello" + std::to_string(i + 1));
    range2.set_start_key("hello" + std::to_string(i + 1));
    range2.set_end_key("hello" + std::to_string(i + 2));
    EXPECT_FALSE(dingodb::Helper::IsConflictRange(range1, range2));
  }

  // Test with a large number of ranges that intersect
  for (int i = 900; i < 996; ++i) {
    range1.set_start_key("hello" + std::to_string(i));
    range1.set_end_key("hello" + std::to_string(i + 2));
    range2.set_start_key("hello" + std::to_string(i + 1));
    range2.set_end_key("hello" + std::to_string(i + 3));
    EXPECT_TRUE(dingodb::Helper::IsConflictRange(range1, range2));
  }
}

TEST_F(HelperTest, GetMemComparableRange) {
  std::string start_key = "hello";
  std::string end_key = "hello0000";
  dingodb::pb::common::Range range;
  range.set_start_key(start_key);
  range.set_end_key(end_key);

  auto mem_comparable_range = dingodb::Helper::GetMemComparableRange(range);

  DINGO_LOG(INFO) << "start_key: " << dingodb::Helper::StringToHex(range.start_key()) << " "
                  << dingodb::Helper::StringToHex(mem_comparable_range.start_key());
  DINGO_LOG(INFO) << "end_key: " << dingodb::Helper::StringToHex(range.end_key()) << " "
                  << dingodb::Helper::StringToHex(mem_comparable_range.end_key());

  EXPECT_EQ(dingodb::Helper::PaddingUserKey(range.start_key()), mem_comparable_range.start_key());
  EXPECT_EQ(dingodb::Helper::PaddingUserKey(range.end_key()), mem_comparable_range.end_key());
}

TEST_F(HelperTest, CompareRegionEpoch) {
  {
    dingodb::pb::common::RegionEpoch epoch1, epoch2;
    epoch1.set_conf_version(1);
    epoch1.set_version(2);
    epoch2.set_conf_version(1);
    epoch2.set_version(2);
    ASSERT_EQ(0, dingodb::Helper::CompareRegionEpoch(epoch1, epoch2));
  }

  {
    dingodb::pb::common::RegionEpoch epoch1, epoch2;
    epoch1.set_conf_version(2);
    epoch1.set_version(3);
    epoch2.set_conf_version(1);
    epoch2.set_version(2);
    ASSERT_EQ(1, dingodb::Helper::CompareRegionEpoch(epoch1, epoch2));
  }

  {
    dingodb::pb::common::RegionEpoch epoch1, epoch2;
    epoch1.set_conf_version(1);
    epoch1.set_version(3);
    epoch2.set_conf_version(1);
    epoch2.set_version(2);
    ASSERT_EQ(1, dingodb::Helper::CompareRegionEpoch(epoch1, epoch2));
  }

  {
    dingodb::pb::common::RegionEpoch epoch1, epoch2;
    epoch1.set_conf_version(0);
    epoch1.set_version(2);
    epoch2.set_conf_version(1);
    epoch2.set_version(2);
    ASSERT_EQ(-1, dingodb::Helper::CompareRegionEpoch(epoch1, epoch2));
  }

  {
    dingodb::pb::common::RegionEpoch epoch1, epoch2;
    epoch1.set_conf_version(1);
    epoch1.set_version(2);
    epoch2.set_conf_version(2);
    epoch2.set_version(3);
    ASSERT_EQ(-1, dingodb::Helper::CompareRegionEpoch(epoch1, epoch2));
  }
}

TEST_F(HelperTest, IsEqualRegionEpoch) {
  {
    dingodb::pb::common::RegionEpoch epoch1, epoch2;
    epoch1.set_conf_version(1);
    epoch1.set_version(2);
    epoch2.set_conf_version(1);
    epoch2.set_version(2);
    ASSERT_EQ(true, dingodb::Helper::IsEqualRegionEpoch(epoch1, epoch2));
  }

  {
    dingodb::pb::common::RegionEpoch epoch1, epoch2;
    epoch1.set_conf_version(1);
    epoch1.set_version(2);
    epoch2.set_conf_version(1);
    epoch2.set_version(3);
    ASSERT_EQ(false, dingodb::Helper::IsEqualRegionEpoch(epoch1, epoch2));
  }

  {
    dingodb::pb::common::RegionEpoch epoch1, epoch2;
    epoch1.set_conf_version(1);
    epoch1.set_version(2);
    epoch2.set_conf_version(2);
    epoch2.set_version(2);
    ASSERT_EQ(false, dingodb::Helper::IsEqualRegionEpoch(epoch1, epoch2));
  }
}

TEST_F(HelperTest, RegionEpochToString) {
  {
    dingodb::pb::common::RegionEpoch epoch;
    epoch.set_conf_version(1);
    epoch.set_version(2);
    ASSERT_EQ("1-2", dingodb::Helper::RegionEpochToString(epoch));
  }

  {
    dingodb::pb::common::RegionEpoch epoch;
    ASSERT_EQ("0-0", dingodb::Helper::RegionEpochToString(epoch));
  }
}

TEST_F(HelperTest, PrintStatus) {
  {
    butil::Status status(dingodb::pb::error::EINDEX_NOT_FOUND, "Index not found");
    ASSERT_EQ("EINDEX_NOT_FOUND Index not found", dingodb::Helper::PrintStatus(status));
  }

  {
    butil::Status status(dingodb::pb::error::OK, "Successed");
    ASSERT_EQ("OK OK", dingodb::Helper::PrintStatus(status));
  }
}

TEST_F(HelperTest, SplitStringToString) {
  {
    std::vector<std::string> result;
    dingodb::Helper::SplitString("", '.', result);
    ASSERT_EQ(0, result.size());
  }

  {
    std::vector<std::string> result;
    dingodb::Helper::SplitString("hello", '.', result);
    ASSERT_EQ(1, result.size());
    ASSERT_EQ("hello", result[0]);
  }

  {
    std::vector<std::string> result;
    dingodb::Helper::SplitString("hello.world", ',', result);
    ASSERT_EQ(1, result.size());
    ASSERT_EQ("hello.world", result[0]);
  }

  {
    std::vector<std::string> result;
    dingodb::Helper::SplitString("how.are.you", '.', result);
    ASSERT_EQ(3, result.size());
    ASSERT_EQ("how", result[0]);
    ASSERT_EQ("are", result[1]);
    ASSERT_EQ("you", result[2]);
  }
}

TEST_F(HelperTest, SplitStringToInt64) {
  {
    std::vector<int64_t> result;
    dingodb::Helper::SplitString("", '.', result);
    ASSERT_EQ(0, result.size());
  }

  {
    std::vector<int64_t> result;
    dingodb::Helper::SplitString("1111", '.', result);
    ASSERT_EQ(1, result.size());
    ASSERT_EQ(1111, result[0]);
  }

  {
    std::vector<int64_t> result;
    dingodb::Helper::SplitString("11111,", ',', result);
    ASSERT_EQ(1, result.size());
    ASSERT_EQ(11111, result[0]);
  }

  {
    std::vector<int64_t> result;
    dingodb::Helper::SplitString("101.102.103", '.', result);
    ASSERT_EQ(3, result.size());
    ASSERT_EQ(101, result[0]);
    ASSERT_EQ(102, result[1]);
    ASSERT_EQ(103, result[2]);
  }

  {
    std::vector<int64_t> result;
    dingodb::Helper::SplitString("101.-102", '.', result);
    ASSERT_EQ(2, result.size());
    ASSERT_EQ(101, result[0]);
    ASSERT_EQ(-102, result[1]);
  }
}

TEST_F(HelperTest, DingoFaissInnerProduct) {
  {
    std::vector<float> vector1 = {};
    std::vector<float> vector2 = {};
    EXPECT_FLOAT_EQ(0.0, dingodb::Helper::DingoFaissInnerProduct(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1};
    std::vector<float> vector2 = {0.3};
    EXPECT_FLOAT_EQ(0.33000001,
                    dingodb::Helper::DingoFaissInnerProduct(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2};
    std::vector<float> vector2 = {0.3, 1.4};
    EXPECT_FLOAT_EQ(2.01, dingodb::Helper::DingoFaissInnerProduct(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2, 0.5, 0.7};
    std::vector<float> vector2 = {0.3, 1.4, 0.7, 0.2};
    EXPECT_FLOAT_EQ(2.5, dingodb::Helper::DingoFaissInnerProduct(vector1.data(), vector2.data(), vector1.size()));
  }
}

TEST_F(HelperTest, DingoFaissL2sqr) {
  {
    std::vector<float> vector1 = {};
    std::vector<float> vector2 = {};
    EXPECT_FLOAT_EQ(0.0, dingodb::Helper::DingoFaissL2sqr(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1};
    std::vector<float> vector2 = {0.3};
    EXPECT_FLOAT_EQ(0.64000005, dingodb::Helper::DingoFaissL2sqr(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2};
    std::vector<float> vector2 = {0.3, 1.4};
    EXPECT_FLOAT_EQ(0.68000001, dingodb::Helper::DingoFaissL2sqr(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2, 0.5, 0.7};
    std::vector<float> vector2 = {0.3, 1.4, 0.7, 0.2};
    EXPECT_FLOAT_EQ(0.97000003, dingodb::Helper::DingoFaissL2sqr(vector1.data(), vector2.data(), vector1.size()));
  }
}
TEST_F(HelperTest, DingoHnswInnerProduct) {
  {
    std::vector<float> vector1 = {};
    std::vector<float> vector2 = {};
    EXPECT_FLOAT_EQ(0.0, dingodb::Helper::DingoHnswInnerProduct(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1};
    std::vector<float> vector2 = {0.3};
    EXPECT_FLOAT_EQ(0.33000001, dingodb::Helper::DingoHnswInnerProduct(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2};
    std::vector<float> vector2 = {0.3, 1.4};
    EXPECT_FLOAT_EQ(2.01, dingodb::Helper::DingoHnswInnerProduct(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2, 0.5, 0.7};
    std::vector<float> vector2 = {0.3, 1.4, 0.7, 0.2};
    EXPECT_FLOAT_EQ(2.5, dingodb::Helper::DingoHnswInnerProduct(vector1.data(), vector2.data(), vector1.size()));
  }
}

TEST_F(HelperTest, DingoHnswInnerProductDistance) {
  {
    std::vector<float> vector1 = {};
    std::vector<float> vector2 = {};
    EXPECT_FLOAT_EQ(1, dingodb::Helper::DingoHnswInnerProductDistance(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1};
    std::vector<float> vector2 = {0.3};
    EXPECT_FLOAT_EQ(0.66999996,
                    dingodb::Helper::DingoHnswInnerProductDistance(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2};
    std::vector<float> vector2 = {0.3, 1.4};
    EXPECT_FLOAT_EQ(-1.01,
                    dingodb::Helper::DingoHnswInnerProductDistance(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2, 0.5, 0.7};
    std::vector<float> vector2 = {0.3, 1.4, 0.7, 0.2};
    EXPECT_FLOAT_EQ(-1.5,
                    dingodb::Helper::DingoHnswInnerProductDistance(vector1.data(), vector2.data(), vector1.size()));
  }
}

TEST_F(HelperTest, DingoHnswL2Sqr) {
  {
    std::vector<float> vector1 = {};
    std::vector<float> vector2 = {};
    EXPECT_FLOAT_EQ(0.0, dingodb::Helper::DingoHnswL2Sqr(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1};
    std::vector<float> vector2 = {0.3};
    EXPECT_FLOAT_EQ(0.64000005, dingodb::Helper::DingoHnswL2Sqr(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2};
    std::vector<float> vector2 = {0.3, 1.4};
    EXPECT_FLOAT_EQ(0.68000001, dingodb::Helper::DingoHnswL2Sqr(vector1.data(), vector2.data(), vector1.size()));
  }

  {
    std::vector<float> vector1 = {1.1, 1.2, 0.5, 0.7};
    std::vector<float> vector2 = {0.3, 1.4, 0.7, 0.2};
    EXPECT_FLOAT_EQ(0.97000003, dingodb::Helper::DingoHnswL2Sqr(vector1.data(), vector2.data(), vector1.size()));
  }
}

TEST_F(HelperTest, VectorToString) {
  {
    std::vector<float> vec = {};
    EXPECT_EQ("", dingodb::Helper::VectorToString(vec));
  }

  {
    std::vector<float> vec = {0.1, 0.2};
    EXPECT_EQ("0.1, 0.2", dingodb::Helper::VectorToString(vec));
  }

  {
    std::vector<float> vec = {0.1, 0.2, 0.3, 0.4};
    EXPECT_EQ("0.1, 0.2, 0.3, 0.4", dingodb::Helper::VectorToString(vec));
  }
}

TEST_F(HelperTest, StringToVector) {
  {
    std::vector<float> vec = dingodb::Helper::StringToVector("");
    EXPECT_EQ(0, vec.size());
  }

  {
    std::vector<float> vec = dingodb::Helper::StringToVector("0.1");
    EXPECT_EQ(1, vec.size());
    EXPECT_FLOAT_EQ(0.1, vec[0]);
  }

  {
    std::vector<float> vec = dingodb::Helper::StringToVector("0.1,0.2");
    EXPECT_EQ(2, vec.size());
    EXPECT_FLOAT_EQ(0.1, vec[0]);
    EXPECT_FLOAT_EQ(0.2, vec[1]);
  }
}

TEST_F(HelperTest, GetPid) { EXPECT_TRUE(dingodb::Helper::GetPid() > 0); }

TEST_F(HelperTest, GetThreadIds) {
  int64_t pid = dingodb::Helper::GetPid();
  std::vector<int64_t> thread_ids = dingodb::Helper::GetThreadIds(pid);

  EXPECT_TRUE(!thread_ids.empty());
}

TEST_F(HelperTest, GetThreadNames) {
  int64_t pid = dingodb::Helper::GetPid();
  std::vector<std::string> thread_names = dingodb::Helper::GetThreadNames(pid);

  EXPECT_TRUE(!thread_names.empty());
}

TEST_F(HelperTest, GetThreadNamesWithFilter) {
  int64_t pid = dingodb::Helper::GetPid();
  {
    std::vector<std::string> thread_names = dingodb::Helper::GetThreadNames(pid, "hello");

    EXPECT_TRUE(thread_names.empty());
  }

  {
    std::vector<std::string> thread_names = dingodb::Helper::GetThreadNames(pid, "dingodb_unit");

    EXPECT_TRUE(!thread_names.empty());
  }

  {
    std::vector<std::string> thread_names = dingodb::Helper::GetThreadNames(pid, "unit");

    EXPECT_TRUE(!thread_names.empty());
  }
}

TEST_F(HelperTest, NowHour) {
  int hour = dingodb::Helper::NowHour();
  ASSERT_LE(hour, 23);
  ASSERT_GE(hour, 0);
}

TEST_F(HelperTest, PrefixNext) {
  using namespace std::string_literals;
  std::string start_key = std::string("\x72\x00\x00\x00\x00\x00\x00\xea\x61"s);
  std::string next_start_key1 = std::string("\x72\x00\x00\x00\x00\x00\x00\xea\x62"s);
  std::string next_start_key2 = std::string("\x72\x00\x00\x00\x00\x00\x00\xea\x61\x00"s);
  std::string next_start_key3 = std::string("\x72\x00\x00\x00\x00\x00\x00\xea\x61\x00\x00"s);

  std::string new_start_key = dingodb::Helper::PrefixNext(start_key);

  EXPECT_EQ(next_start_key1, new_start_key);

  EXPECT_TRUE(next_start_key2 < next_start_key3);

  EXPECT_TRUE(next_start_key2 < next_start_key1);

  dingodb::pb::common::RangeWithOptions range_with_options;
  range_with_options.mutable_range()->set_start_key(start_key);
  range_with_options.mutable_range()->set_end_key(start_key);
  range_with_options.set_with_start(true);
  range_with_options.set_with_end(true);

  dingodb::pb::common::Range range = dingodb::Helper::TransformRangeWithOptions(range_with_options);

  EXPECT_EQ(next_start_key1, range.end_key());
}
