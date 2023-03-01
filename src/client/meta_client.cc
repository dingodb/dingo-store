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

#include <cstdint>
#include <string>

#include "braft/raft.h"
#include "braft/route_table.h"
#include "braft/util.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "gflags/gflags.h"
#include "google/protobuf/port.h"
#include "proto/meta.pb.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_string(meta_addr, "127.0.0.1:19190", "meta server addr");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_string(method, "Hello", "Request method");

bvar::LatencyRecorder g_latency_recorder("dingo-meta");

void SendGetSchemas(brpc::Controller& cntl,
                    dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetSchemasRequest request;
  dingodb::pb::meta::GetSchemasResponse response;

  // const char* op = nullptr;
  request.set_schema_id(0);
  stub.GetSchemas(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " schema_id =" << request.schema_id()
              << " schema_count =" << response.schemas_size()
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size()
              << " latency=" << cntl.latency_us();
    LOG(INFO) << response.DebugString();

    for (int32_t i = 0; i < response.schemas_size(); i++) {
      LOG(INFO) << "schema_id=[" << response.schemas(i).id() << "]"
                << "child_schema_count="
                << response.schemas(i).schema_ids_size()
                << "child_table_count=" << response.schemas(i).table_ids_size();
      for (int32_t j = 0; j < response.schemas(i).schema_ids_size(); j++) {
        LOG(INFO) << "child schema_id=[" << response.schemas(i).schema_ids(j)
                  << "]";
      }
      for (int32_t j = 0; j < response.schemas(i).table_ids_size(); j++) {
        LOG(INFO) << "child table_id=[" << response.schemas(i).table_ids(j)
                  << "]";
      }
    }
  }
}

void SendGetTables(brpc::Controller& cntl,
                   dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  // const char* op = nullptr;
  request.set_schema_id(0);
  stub.GetTables(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " request schema_id =" << request.schema_id()
              << " table_count =" << response.tables_size()
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size()
              << " latency=" << cntl.latency_us();
    LOG(INFO) << response.DebugString();
    // for (int32_t i = 0; i < response.tables_size(); i++) {
    //   const auto& table = response.tables(i);
    //   LOG(INFO) << "table_id=[" << table.id() << "]"
    //             << "partition_count=" << table.partitions_size();

    //   for (int32_t j = 0; j < table.partitions_size(); j++) {
    //     const auto& partition = table.partitions(j);
    //     LOG(INFO) << "partition_id=[" << partition.id()
    //               << "] region_count = " << partition.regions_size()
    //               << " start_key " << partition.range().start_key()
    //               << " end_key " << partition.range().end_key();

    //     for (int32_t k = 0; k < partition.regions_size(); k++) {
    //       const auto& region  = partition.regions(k);
    //       LOG(INFO) << "region_id = " << region.id() << " region_name = " <<
    //       region.name() ;
    //     }
    //   }
    // }
  }
}

void SendCreateTable(brpc::Controller& cntl,
                     dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::CreateTableRequest request;
  dingodb::pb::meta::CreateTableResponse response;

  request.set_schema_id(1);
  // string name = 1;
  auto* table_definition = request.mutable_table_definition();
  table_definition->set_name("t_test1");
  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 10; i++) {
    auto* column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type(::dingodb::pb::common::SqlType::SQL_TYPE_INTEGER);
    column->set_element_type(
        ::dingodb::pb::common::ElementType::ELEM_TYPE_BYTES);
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");
  }
  // map<string, Index> indexes = 3;
  // uint32 version = 4;
  table_definition->set_version(1);
  // uint64 ttl = 5;
  table_definition->set_ttl(0);
  // PartitionRule table_partition = 6;
  // Engine engine = 7;
  table_definition->set_engine(::dingodb::pb::common::Engine::ENG_ROCKSDB);
  // map<string, string> properties = 8;
  auto* prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  stub.CreateTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " request schema_id =" << request.schema_id()
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size()
              << " latency=" << cntl.latency_us();
    LOG(INFO) << request.DebugString();
    LOG(INFO) << response.DebugString();
  }
}

void SendDropTable(brpc::Controller& cntl,
                   dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  // const char* op = nullptr;
  request.set_schema_id(1);
  request.set_table_id(1);
  stub.DropTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " request schema_id =" << request.schema_id()
              << " request table_id =" << request.table_id()
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size()
              << " latency=" << cntl.latency_us();
    LOG(INFO) << response.DebugString();
  }
}

void SendCreateSchema(brpc::Controller& cntl,
                      dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::CreateSchemaRequest request;
  dingodb::pb::meta::CreateSchemaResponse response;

  request.set_parent_schema_id(2);
  request.set_schema_name("test_create_schema");
  stub.CreateSchema(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
    return;
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " request parent_schema_id =" << request.parent_schema_id()
              << " request schema_name=" << request.schema_name()
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size()
              << " latency=" << cntl.latency_us();
    LOG(INFO) << response.DebugString();
  }
}

void* Sender(void* /*arg*/) {
  while (!brpc::IsAskedToQuit()) {
    braft::PeerId leader(FLAGS_meta_addr);

    // rpc
    brpc::Channel channel;
    if (channel.Init(leader.addr, nullptr) != 0) {
      LOG(ERROR) << "Fail to init channel to " << leader;
      bthread_usleep(FLAGS_timeout_ms * 1000L);
      continue;
    }
    dingodb::pb::meta::MetaService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);

    if (FLAGS_method == "GetSchemas") {
      SendGetSchemas(cntl, stub);
    } else if (FLAGS_method == "GetTables") {
      SendGetTables(cntl, stub);
    } else if (FLAGS_method == "CreateTable") {
      SendCreateTable(cntl, stub);
    } else if (FLAGS_method == "DropTable") {
      SendDropTable(cntl, stub);
    } else if (FLAGS_method == "CreateSchema") {
      SendCreateSchema(cntl, stub);
    }

    bthread_usleep(FLAGS_timeout_ms * 10000L);
  }
  return nullptr;
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    if (bthread_start_background(&tids[i], nullptr, Sender, nullptr) != 0) {
      LOG(ERROR) << "Fail to create bthread";
      return -1;
    }
  }

  while (!brpc::IsAskedToQuit()) {
    LOG_IF(INFO, !FLAGS_log_each_request)
        << "Sending Request"
        << " qps=" << g_latency_recorder.qps(1)
        << " latency=" << g_latency_recorder.latency(1);
  }

  LOG(INFO) << "meta client is going to quit";
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }

  return 0;
}