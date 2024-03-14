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

#ifndef DINGODB_REGION_SERVICE_H_
#define DINGODB_REGION_SERVICE_H_

#include <memory>

#include "brpc/builtin/tabbed.h"
#include "coordinator/coordinator_control.h"
#include "proto/cluster_stat.pb.h"

namespace dingodb {

class RegionImpl : public pb::cluster::region, public brpc::Tabbed {
 public:
  RegionImpl() = default;
  void default_method(::google::protobuf::RpcController* controller, const pb::cluster::RegionRequest* request,
                      pb::cluster::RegionResponse* response, ::google::protobuf::Closure* done) override;
  void GetTabInfo(brpc::TabInfoList*) const override;
  void SetControl(std::shared_ptr<CoordinatorControl> controller) { coordinator_controller_ = controller; }

  void PrintRegions(std::ostream& os, bool use_html);

 private:
  std::shared_ptr<CoordinatorControl> coordinator_controller_;
};

}  // namespace dingodb

#endif  // DINGODB_REGION_SERVICE_H_