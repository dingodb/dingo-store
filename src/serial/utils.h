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

#ifndef DINGO_SERIAL_UTILS_H_
#define DINGO_SERIAL_UTILS_H_

#include <vector>
#include <any>
#include "proto/meta.pb.h"
#include "schema/base_schema.h"
#include "schema/boolean_schema.h"
#include "schema/double_schema.h"
#include "schema/integer_schema.h"
#include "schema/long_schema.h"
#include "schema/string_schema.h"

namespace dingodb {

void SortSchema(std::vector<BaseSchema*>* schemas);
int* GetApproPerRecordSize(std::vector<BaseSchema*>* schemas);
bool VectorFindAndRemove(std::vector<int>* v, int t);
std::vector<BaseSchema*>* TableDefinitionToDingoSchema(pb::meta::TableDefinition* td);
std::vector<std::any>* ElementToSql(pb::meta::TableDefinition* td, std::vector<std::any>* record);
std::vector<std::any>* SqlToElement(pb::meta::TableDefinition* td, std::vector<std::any>* record);

}  // namespace dingodb

#endif