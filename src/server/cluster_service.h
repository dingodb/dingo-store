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

#ifndef DINGODB_CLUSTER_STAT_SERVICE_H_
#define DINGODB_CLUSTER_STAT_SERVICE_H_

#include <memory>
#include <ostream>
#include <string>

#include "brpc/builtin/tabbed.h"
#include "coordinator/coordinator_control.h"
#include "proto/cluster_stat.pb.h"

namespace dingodb {

class ClusterStatImpl : public pb::cluster::ClusterStat, public brpc::Tabbed {
 public:
  ClusterStatImpl() = default;
  void default_method(::google::protobuf::RpcController* controller, const pb::cluster::ClusterStatRequest* request,
                      pb::cluster::ClusterStatResponse* response, ::google::protobuf::Closure* done);
  void GetTabInfo(brpc::TabInfoList*) const override;
  void SetControl(std::shared_ptr<CoordinatorControl> controller) { controller_ = controller; }

 private:
  std::shared_ptr<CoordinatorControl> controller_;
  std::string GetTabHead();
  bool GetRegionInfo(uint64_t region_id, const pb::common::RegionMap& region_map, pb::common::Region& result);
  void PrintSchema(std::ostream& os, const std::string& schema_name) const;
  void PrintRegionNode(std::ostream& os, const pb::common::Region& region) const;
  void PrintTableDefinition(std::ostream& os, const pb::meta::TableDefinition& table_definition) const;
  void PrintTableRegions(std::ostream& os, const pb::common::RegionMap& region_map,
                         const pb::meta::TableRange& table_range);
};

}  // namespace dingodb

#endif  // DINGODB_CLUSTER_STAT_SERVICE_H_
