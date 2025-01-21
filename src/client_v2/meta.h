
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGODB_CLIENT_META_H_
#define DINGODB_CLIENT_META_H_

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

#include "CLI/CLI.hpp"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "client_v2/store.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"

namespace client_v2 {

void SetUpMetaSubCommands(CLI::App &app);

dingodb::pb::meta::TableDefinitionWithId SendGetIndex(int64_t index_id);
dingodb::pb::meta::TableDefinitionWithId SendGetTable(int64_t table_id);
dingodb::pb::meta::TableRange SendGetTableRange(int64_t table_id);
dingodb::pb::meta::IndexRange SendGetIndexRange(int64_t table_id);

butil::Status SendGetSchema(int64_t tenant_id, int64_t schema_id, dingodb::pb::meta::Schema &schema);
butil::Status SendGetSchemas(int64_t tenant_id, std::vector<dingodb::pb::meta::Schema> &schemas);

int GetCreateTableId(int64_t &table_id);
int64_t SendCreateTable(const std::string &table_name, int partition_num);
void SendDropTable(int64_t table_id);
int64_t SendGetTableByName(const std::string &table_name);
butil::Status SendGetTableByName(const std::string &table_name,
                                 dingodb::pb::meta::TableDefinitionWithId &table_definition);
std::vector<int64_t> SendGetTablesBySchema();

butil::Status GetSqlTableOrIndexMeta(int64_t table_id, dingodb::pb::meta::TableDefinitionWithId &table_definition);
butil::Status GetSqlTableOrIndexMeta(std::string table_name, int64_t schema_id,
                                     dingodb::pb::meta::TableDefinitionWithId &table_definition);

butil::Status GetSqlSchemaMeta(int64_t tenant_id, int64_t schema_id, dingodb::pb::meta::Schema &schema);
butil::Status GetSqlSchemasMeta(int64_t tenant_id, std::vector<dingodb::pb::meta::Schema> &schemas);

butil::Status GetTableOrIndexDefinition(int64_t id, dingodb::pb::meta::TableDefinition &table_definition);
butil::Status GetTableOrIndexDefinition(int64_t id, dingodb::pb::meta::TableDefinitionWithId &table_definition_with_id);
butil::Status GetTableOrIndexDefinition(std::string table_name, int64_t schema_id,
                                        dingodb::pb::meta::TableDefinitionWithId &table_definition_with_id);

butil::Status GetSchemaDefinition(int64_t tenant_id, int64_t schema_id, dingodb::pb::meta::Schema &schema);

butil::Status GetSchemasDefinition(int64_t tenant_id, std::vector<dingodb::pb::meta::Schema> &schemas);

struct MetaHelloOptions {
  std::string coor_url;
};
void SetUpMetaHello(CLI::App &app);
void RunMetaHello(MetaHelloOptions const &opt);

struct GetTenantOptions {
  std::string coor_url;
  int32_t source;
  int64_t tenant_id;
};
void SetUpGetTenant(CLI::App &app);
void RunGetTenant(GetTenantOptions const &opt);

struct GetSchemasOptions {
  std::string coor_url;
  int64_t tenant_id;
};
void SetUpGetSchemas(CLI::App &app);
void RunGetSchemas(GetSchemasOptions const &opt);

struct GetSchemaOptions {
  std::string coor_url;
  int64_t tenant_id;
  int64_t schema_id;
};
void SetUpGetSchema(CLI::App &app);
void RunGetSchema(GetSchemaOptions const &opt);

struct GetSchemaByNameOptions {
  std::string coor_url;
  int64_t tenant_id;
  std::string name;
};
void SetUpGetSchemaByName(CLI::App &app);
void RunGetSchemaByName(GetSchemaByNameOptions const &opt);

struct GetTablesBySchemaOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpGetTablesBySchema(CLI::App &app);
void RunGetTablesBySchema(GetTablesBySchemaOptions const &opt);

struct GetTablesCountOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpGetTablesCount(CLI::App &app);
void RunGetTablesCount(GetTablesCountOptions const &opt);

struct CreateTableOptions {
  std::string coor_url;
  std::string name;
  int64_t schema_id;
  int32_t part_count;
  bool enable_rocks_engine;
  std::string engine;
  int32_t replica;
  bool with_increment;
};
void SetUpCreateTable(CLI::App &app);
void RunCreateTable(CreateTableOptions const &opt);

struct CreateTableIdsOptions {
  std::string coor_url;
  int32_t part_count;
};
void SetUpCreateTableIds(CLI::App &app);
void RunCreateTableIds(CreateTableIdsOptions const &opt);

struct CreateTableIdOptions {
  std::string coor_url;
};
void SetUpCreateTableId(CLI::App &app);
void RunCreateTableId(CreateTableIdOptions const &opt);

struct DropTableOptions {
  std::string coor_url;
  int64_t id;
  int64_t schema_id;
};
void SetUpDropTable(CLI::App &app);
void RunDropTable(DropTableOptions const &opt);

struct CreateSchemaOptions {
  std::string coor_url;
  std::string name;
  int64_t tenant_id;
};
void SetUpCreateSchema(CLI::App &app);
void RunCreateSchema(CreateSchemaOptions const &opt);

struct DropSchemaOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpDropSchema(CLI::App &app);
void RunDropSchema(DropSchemaOptions const &opt);

struct GetTableOptions {
  std::string coor_url;
  bool is_index;
  int64_t id;
};
void SetUpGetTable(CLI::App &app);
void RunGetTable(GetTableOptions const &opt);

struct GetTableByNameOptions {
  std::string coor_url;
  std::string name;
  int64_t schema_id;
};
void SetUpGetTableByName(CLI::App &app);
void RunGetTableByName(GetTableByNameOptions const &opt);

struct GetTableRangeOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetTableRange(CLI::App &app);
void RunGetTableRange(GetTableRangeOptions const &opt);

struct GetTableMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetTableMetrics(CLI::App &app);
void RunGetTableMetrics(GetTableMetricsOptions const &opt);

struct SwitchAutoSplitOptions {
  std::string coor_url;
  int64_t id;
  int64_t schema_id;
  bool auto_split;
};
void SetUpSwitchAutoSplit(CLI::App &app);
void RunSwitchAutoSplit(SwitchAutoSplitOptions const &opt);

struct GetDeletedTableOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetDeletedTable(CLI::App &app);
void RunGetDeletedTable(GetDeletedTableOptions const &opt);

struct GetDeletedIndexOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetDeletedIndex(CLI::App &app);
void RunGetDeletedIndex(GetDeletedIndexOptions const &opt);

struct CleanDeletedTableOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpCleanDeletedTable(CLI::App &app);
void RunCleanDeletedTable(CleanDeletedTableOptions const &opt);

struct CleanDeletedIndexOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpCleanDeletedIndex(CLI::App &app);
void RunCleanDeletedIndex(CleanDeletedIndexOptions const &opt);

// tenant
struct CreateTenantOptions {
  std::string coor_url;
  std::string name;
  std::string comment;
  int64_t tenant_id;
};
void SetUpCreateTenant(CLI::App &app);
void RunCreateTenant(CreateTenantOptions const &opt);

struct UpdateTenantOptions {
  std::string coor_url;
  std::string name;
  std::string comment;
  int64_t tenant_id;
};
void SetUpUpdateTenant(CLI::App &app);
void RunUpdateTenant(UpdateTenantOptions const &opt);

struct DropTenantOptions {
  std::string coor_url;
  int64_t tenant_id;
};
void SetUpDropTenant(CLI::App &app);
void RunDropTenant(DropTenantOptions const &opt);

// indexs
struct GetIndexesOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpGetIndexes(CLI::App &app);
void RunGetIndexes(GetIndexesOptions const &opt);

struct GetIndexesCountOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpGetIndexesCount(CLI::App &app);
void RunGetIndexesCount(GetIndexesCountOptions const &opt);
struct CreateIndexIdOptions {
  std::string coor_url;
};
void SetUpCreateIndexId(CLI::App &app);
void RunCreateIndexId(CreateIndexIdOptions const &opt);

struct UpdateIndexOptions {
  std::string coor_url;
  int64_t id;
  int32_t max_elements;
};
void SetUpUpdateIndex(CLI::App &app);
void RunUpdateIndex(UpdateIndexOptions const &opt);

struct DropIndexOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpDropIndex(CLI::App &app);
void RunDropIndex(DropIndexOptions const &opt);

struct GetIndexOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetIndex(CLI::App &app);
void RunGetIndex(GetIndexOptions const &opt);

struct GetIndexByNameOptions {
  std::string coor_url;
  int64_t schema_id;
  std::string name;
};
void SetUpGetIndexByName(CLI::App &app);
void RunGetIndexByName(GetIndexByNameOptions const &opt);

struct GetIndexRangeOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetIndexRange(CLI::App &app);
void RunGetIndexRange(GetIndexRangeOptions const &opt);

struct GetIndexMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetIndexMetrics(CLI::App &app);
void RunGetIndexMetrics(GetIndexMetricsOptions const &opt);

struct GenerateTableIdsOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpGenerateTableIds(CLI::App &app);
void RunGenerateTableIds(GenerateTableIdsOptions const &opt);

struct CreateTablesOptions {
  std::string coor_url;
  std::string name;
  int64_t schema_id;
  int64_t part_count;
  int64_t replica;
  std::string engine;
};
void SetUpCreateTables(CLI::App &app);
void RunCreateTables(CreateTablesOptions const &opt);

struct UpdateTablesOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t def_version;
  int64_t part_count;
  int64_t replica;
  bool is_updating_index;
  std::string engine;
  std::string name;
};
void SetUpUpdateTables(CLI::App &app);
void RunUpdateTables(UpdateTablesOptions const &opt);

struct AddIndexOnTableOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t index_id;
  std::string name;
  int64_t def_version;
  int64_t part_count;
  int64_t replica;
  bool is_updating_index;
  std::string engine;
};
void SetUpAddIndexOnTable(CLI::App &app);
void RunAddIndexOnTable(AddIndexOnTableOptions const &opt);

struct DropIndexOnTableOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t index_id;
};
void SetUpDropIndexOnTable(CLI::App &app);
void RunDropIndexOnTable(DropIndexOnTableOptions const &opt);

struct GetTablesOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetTables(CLI::App &app);
void RunGetTables(GetTablesOptions const &opt);

struct DropTablesOptions {
  std::string coor_url;
  int64_t id;
  int64_t schema_id;
};
void SetUpDropTables(CLI::App &app);
void RunDropTables(DropTablesOptions const &opt);

// autoincrement
struct GetAutoIncrementsOptions {
  std::string coor_url;
};
void SetUpGetAutoIncrements(CLI::App &app);
void RunGetAutoIncrements(GetAutoIncrementsOptions const &opt);

struct GetAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetAutoIncrement(CLI::App &app);
void RunGetAutoIncrement(GetAutoIncrementOptions const &opt);

struct CreateAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
  int64_t incr_start_id;
};
void SetUpCreateAutoIncrement(CLI::App &app);
void RunCreateAutoIncrement(CreateAutoIncrementOptions const &opt);

struct UpdateAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
  int64_t incr_start_id;
  bool force;
};
void SetUpUpdateAutoIncrement(CLI::App &app);
void RunUpdateAutoIncrement(UpdateAutoIncrementOptions const &opt);

struct GenerateAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
  int32_t generate_count;
  int32_t auto_increment_offset;
  int32_t auto_increment_increment;
};
void SetUpGenerateAutoIncrement(CLI::App &app);
void RunGenerateAutoIncrement(GenerateAutoIncrementOptions const &opt);

struct DeleteAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpDeleteAutoIncrement(CLI::App &app);
void RunDeleteAutoIncrement(DeleteAutoIncrementOptions const &opt);

// meta watch
struct ListWatchOptions {
  std::string coor_url;
  int64_t watch_id;
};
void SetUpListWatch(CLI::App &app);
void RunListWatch(ListWatchOptions const &opt);

struct CreateWatchOptions {
  std::string coor_url;
  int64_t watch_id;
  int64_t start_revision;
  std::string watch_type;
};
void SetUpCreateWatch(CLI::App &app);
void RunCreateWatch(CreateWatchOptions const &opt);

struct CancelWatchOptions {
  std::string coor_url;
  int64_t watch_id;
  int64_t start_revision;
};
void SetUpCancelWatch(CLI::App &app);
void RunCancelWatch(CancelWatchOptions const &opt);

struct ProgressWatchOptions {
  std::string coor_url;
  int64_t watch_id;
};
void SetUpProgressWatch(CLI::App &app);
void RunProgressWatch(ProgressWatchOptions const &opt);

// tso
struct GenTsoOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGenTso(CLI::App &app);
void RunGenTso(GenTsoOptions const &opt);

struct ResetTsoOptions {
  std::string coor_url;
  int64_t tso_new_physical;
  int64_t tso_save_physical;
  int64_t tso_new_logical;
};
void SetUpResetTso(CLI::App &app);
void RunResetTso(ResetTsoOptions const &opt);

struct UpdateTsoOptions {
  std::string coor_url;
  int64_t tso_new_physical;
  int64_t tso_save_physical;
  int64_t tso_new_logical;
};
void SetUpUpdateTso(CLI::App &app);
void RunUpdateTso(UpdateTsoOptions const &opt);

struct GetRegionByTableOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t tenant_id;
  std::string table_name;
  int64_t schema_id;
};
void SetUpGetRegionByTable(CLI::App &app);
void RunGetRegionByTable(GetRegionByTableOptions const &opt);

struct CreateIdsOptions {
  std::string coor_url;
  int64_t count;
  std::string epoch_type;
};
void SetUpCreateIds(CLI::App &app);
void RunCreateIds(CreateIdsOptions const &opt);

}  // namespace client_v2

#endif  // DINGODB_CLIENT_META_H_