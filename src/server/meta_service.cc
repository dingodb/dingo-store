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

#include <memory>

#include "proto/common.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

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
}  // namespace dingodb