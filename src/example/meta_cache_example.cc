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

#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "glog/logging.h"
#include "sdk/meta_cache.h"
#include "sdk/status.h"

using dingodb::sdk::MetaCache;
using dingodb::sdk::Region;
using dingodb::sdk::Status;

DEFINE_string(coordinator_url, "", "coordinator url");

static std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction;

void CreateRegion(std::string name, std::string start_key, std::string end_key, int replicas = 3) {
  CHECK(!name.empty()) << "name should not empty";
  CHECK(!start_key.empty()) << "start_key should not empty";
  CHECK(!end_key.empty()) << "end_key should not empty";
  CHECK(start_key < end_key) << "start_key must < end_key";
  CHECK(replicas > 0) << "replicas must > 0";

  dingodb::pb::coordinator::CreateRegionRequest request;
  dingodb::pb::coordinator::CreateRegionResponse response;

  request.set_region_name(name);
  request.set_replica_num(replicas);
  request.mutable_range()->set_start_key(start_key);
  request.mutable_range()->set_end_key(end_key);

  DINGO_LOG(INFO) << "Create region request: " << request.DebugString();

  auto status2 = coordinator_interaction->SendRequest("CreateRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status2;
  DINGO_LOG(INFO) << response.DebugString();
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_coordinator_url.empty()) {
    DINGO_LOG(ERROR) << "coordinator url is empty, try to use file://./coor_list";
    FLAGS_coordinator_url = "file://./coor_list";
  }

  CHECK(!FLAGS_coordinator_url.empty());
  coordinator_interaction = std::make_shared<dingodb::CoordinatorInteraction>();
  if (!coordinator_interaction->InitByNameService(
          FLAGS_coordinator_url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeCoordinator)) {
    DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << FLAGS_coordinator_url;
    return -1;
  }

  CreateRegion("meta_cache_example", "wa00000000", "wc00000000", 3);

  auto meta_cache = std::make_shared<MetaCache>(coordinator_interaction);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey("wb", region);
  DINGO_LOG(INFO) << got.ToString() << ", " << (got.ok() ? region->ToString() : "null");
  got = meta_cache->LookupRegionByKey("wc00000000", region);
  CHECK(got.IsNotFound());
  DINGO_LOG(INFO) << got.ToString();

  meta_cache->Dump();
}