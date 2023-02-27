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

#include "server/meta_service.h"

#include <cstddef>
#include <memory>

#include "proto/common.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {
using pb::meta::CreateTableResponse;

void MetaServiceImpl::GetSchemas(
    google::protobuf::RpcController * /*controller*/,
    const pb::meta::GetSchemasRequest *request,
    pb::meta::GetSchemasResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetSchemas request:  schema_id = [" << request->schema_id()
            << "]";

  auto *schema = response->add_schemas();
  schema->set_id(1);
  schema->set_name("meta");
  schema->add_table_ids(1);
  schema->add_table_ids(2);
  schema->add_schema_ids(2);
}

void MetaServiceImpl::GetTables(
    google::protobuf::RpcController * /*controller*/,
    const pb::meta::GetTablesRequest *request,
    pb::meta::GetTablesResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetTables request:  schema_id = [" << request->schema_id()
            << "]";

  // set table info
  auto *table = response->add_tables();
  table->set_id(1);

  // set partition info
  auto *partition = table->add_partitions();
  partition->set_id(1);

  // set region start
  auto *range = partition->mutable_range();
  char key1[5] = {0x0, 0x0, 0x0, 0x0, 0x0};
  char key2[5] = {static_cast<char>(0xFF), static_cast<char>(0xFF),
                  static_cast<char>(0xFF), static_cast<char>(0xFF),
                  static_cast<char>(0xFF)};
  range->set_start_key(key1);
  range->set_end_key(key1);
  auto *region = partition->add_regions();
  region->set_id(1);
  region->set_partition_id(1);
  region->set_epoch(1);
  region->set_name("test1");
  region->set_status(pb::common::RegionStatus::REGION_NORMAL);
  region->set_leader_store_id(1);
  for (int i = 1; i < 3; i++) {
    auto *store = region->add_electors();
    store->set_id(1000);
    store->set_resource_tag("dingo-phy");
    store->set_status(pb::common::StoreStatus::STORE_NORMAL);
    auto *server = store->mutable_server_location();
    server->set_host("127.0.0.1");
    server->set_port(19200 + i);
  }
  auto *region_range = region->mutable_range();
  region_range->set_start_key(key1);
  region_range->set_end_key(key2);

  // set meta info begin
  region->set_schema_id(1);
  region->set_table_id(1);
  region->set_partition_id(1);
  // set meta info end

  region->set_create_timestamp(1677479019);
  // set region info end
  // set partition info end
  // set table info end
}

void MetaServiceImpl::CreateTable(
    google::protobuf::RpcController * /*controller*/,
    const pb::meta::CreateTableRequest *request,
    CreateTableResponse * /*response*/, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "CreatTable request:  schema_id = [" << request->schema_id()
            << "]";
  LOG(INFO) << request->DebugString();
}

void MetaServiceImpl::DropTable(
    google::protobuf::RpcController * /*controller*/,
    const pb::meta::DropTableRequest *request,
    pb::meta::DropTableResponse * /*response*/,
    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "DropTable request:  schema_id = [" << request->schema_id()
            << "]"
            << " table_id = [" << request->table_id() << "]";
  LOG(INFO) << request->DebugString();
}

}  // namespace dingodb