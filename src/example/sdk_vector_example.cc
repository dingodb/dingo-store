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

#include <memory>

#include "common/logging.h"
#include "common/synchronization.h"
#include "glog/logging.h"
#include "sdk/client.h"
#include "sdk/status.h"
#include "sdk/vector.h"

using dingodb::sdk::Status;

DEFINE_string(coordinator_url, "", "coordinator url");
static std::shared_ptr<dingodb::sdk::Client> g_client;

static void PrepareVectorIndex() {
  dingodb::sdk::VectorIndexCreator* creator;
  Status built = g_client->NewVectorIndexCreator(&creator);
  CHECK(built.IsOK()) << "dingo creator build fail:" << built.ToString();
  CHECK_NOTNULL(creator);
  dingodb::ScopeGuard guard([&]() { delete creator; });

  dingodb::sdk::FlatParam flat_param(1000, dingodb::sdk::MetricType::kL2);
  int64_t index_id{0};
  Status create = creator->SetSchemaId(2)
                      .SetName("test01")
                      .SetReplicaNum(3)
                      .SetRangePartitions({5, 10, 20})
                      .SetFlatParam(flat_param)
                      .Create(index_id);
  DINGO_LOG(INFO) << "Create index status: " << create.ToString() << ", index_id:" << index_id;
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  FLAGS_v = dingodb::kGlobalValueOfDebug;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_coordinator_url.empty()) {
    DINGO_LOG(ERROR) << "coordinator url is empty, try to use file://./coor_list";
    FLAGS_coordinator_url = "file://./coor_list";
  }

  std::shared_ptr<dingodb::sdk::Client> client;
  Status built = dingodb::sdk::Client::Build(FLAGS_coordinator_url, client);
  if (!built.ok()) {
    DINGO_LOG(ERROR) << "Fail to build client, please check parameter --url=" << FLAGS_coordinator_url;
    return -1;
  }
  CHECK_NOTNULL(client.get());
  g_client = std::move(client);

  PrepareVectorIndex();
}
