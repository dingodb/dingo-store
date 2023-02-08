// Copyright (c) 2023 dingo.com, Inc. All Rights Reserved
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


#include <iostream>

#include <brpc/server.h>
#include <gflags/gflags.h>

#include "proto/store.pb.h"
#include "store_service.h"


DEFINE_int32(port, 8200, "Listen port of this peer");


int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  brpc::Server server;
  store::StoreServiceImpl store_service;

  if (server.AddService(&store_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add store service";
    return -1;
  }

  if (server.Start(FLAGS_port, NULL) != 0) {
    LOG(ERROR) << "Fail to start Server";
    return -1;
  }
  LOG(INFO) << "service is running on" << server.listen_address();

  // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
  while (!brpc::IsAskedToQuit()) {
    sleep(1);
  }
  LOG(INFO) << "service is going to quit";

  server.Stop(0);
  server.Join();

  return 0;
}