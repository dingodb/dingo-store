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

#include "server/node_service.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "butil/status.h"
#include "common/failpoint.h"
#include "common/logging.h"
#include "coordinator/coordinator_closure.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/node.pb.h"
#include "server/server.h"
#include "vector/vector_index_snapshot.h"

namespace dingodb {
using pb::error::Errno;
using pb::node::LogDetail;
using pb::node::LogLevel;

void NodeServiceImpl::SetServer(dingodb::Server* server) { this->server_ = server; }

void NodeServiceImpl::GetNodeInfo(google::protobuf::RpcController* /*controller*/,
                                  const pb::node::GetNodeInfoRequest* request, pb::node::GetNodeInfoResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);

  if (request->cluster_id() < 0) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EILLEGAL_PARAMTETERS);
  }

  auto* node_info = response->mutable_node_info();

  node_info->set_id(server_->Id());
  node_info->set_role(server_->GetRole());

  // parse server location
  auto* server_location = node_info->mutable_server_location();
  auto* server_host = server_location->mutable_host();
  auto host_str = butil::ip2str(server_->ServerEndpoint().ip);
  server_host->assign(std::string(host_str.c_str()));
  server_location->set_port(server_->ServerEndpoint().port);

  // parse raft location
  auto* raft_location = node_info->mutable_raft_location();
  auto* raft_host = raft_location->mutable_host();
  auto raft_host_str = butil::ip2str(server_->RaftEndpoint().ip);
  raft_host->assign(std::string(host_str.c_str()));
  raft_location->set_port(server_->RaftEndpoint().port);
}

void NodeServiceImpl::GetLogLevel(google::protobuf::RpcController* /*controller*/,
                                  const pb::node::GetLogLevelRequest* request, pb::node::GetLogLevelResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);

  DINGO_LOG(INFO) << "GetLogLevel receive Request:" << request->DebugString();

  auto* log_detail = response->mutable_log_detail();
  log_detail->set_log_buf_secs(DingoLogger::GetLogBuffSecs());
  log_detail->set_max_log_size(DingoLogger::GetMaxLogSize());
  log_detail->set_stop_logging_if_full_disk(DingoLogger::GetStoppingWhenDiskFull());

  int const min_log_level = DingoLogger::GetMinLogLevel();
  int const min_verbose_level = DingoLogger::GetMinVerboseLevel();

  if (min_log_level > pb::node::FATAL) {
    DINGO_LOG(ERROR) << "Invalid Log Level:" << min_log_level;
    response->mutable_error()->set_errcode(::dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  if (min_log_level == 0 && min_verbose_level > 1) {
    response->set_log_level(static_cast<LogLevel>(0));
  } else {
    response->set_log_level(static_cast<LogLevel>(min_log_level + 1));
  }
}

void NodeServiceImpl::ChangeLogLevel(google::protobuf::RpcController* /* controller */,
                                     const pb::node::ChangeLogLevelRequest* request,
                                     pb::node::ChangeLogLevelResponse* /*response*/, google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);

  DINGO_LOG(INFO) << "ChangeLogLevel=>Receive Request:" << request->DebugString();

  const LogLevel log_level = request->log_level();
  DingoLogger::ChangeGlogLevelUsingDingoLevel(log_level);

  const LogDetail& log_detail = request->log_detail();
  DingoLogger::SetLogBuffSecs(log_detail.log_buf_secs());
  DingoLogger::SetMaxLogSize(log_detail.max_log_size());
  DingoLogger::SetStoppingWhenDiskFull(log_detail.stop_logging_if_full_disk());
}

class PrometheusMetricsDumper : public bvar::Dumper {
 public:
  explicit PrometheusMetricsDumper(butil::IOBufBuilder* os, const std::string& server_prefix)
      : os_(os), server_prefix_(server_prefix) {}

  bool dump(const std::string& name, const butil::StringPiece& desc) override;

  PrometheusMetricsDumper(const PrometheusMetricsDumper&) = delete;
  const PrometheusMetricsDumper& operator=(const PrometheusMetricsDumper&) = delete;

 private:
  // Return true iff name ends with suffix output by LatencyRecorder.
  bool DumpLatencyRecorderSuffix(const butil::StringPiece& name, const butil::StringPiece& desc);

  // 6 is the number of bvars in LatencyRecorder that indicating percentiles
  static const int kNpercentiles = 6;

  struct SummaryItems {
    std::string latency_percentiles[kNpercentiles];
    int64_t latency_avg;
    int64_t count;
    std::string metric_name;

    bool IsComplete() const { return !metric_name.empty(); }
  };
  const SummaryItems* ProcessLatencyRecorderSuffix(const butil::StringPiece& name, const butil::StringPiece& desc);

  butil::IOBufBuilder* os_;
  const std::string server_prefix_;
  std::map<std::string, SummaryItems> m_;
};

bool PrometheusMetricsDumper::dump(const std::string& name, const butil::StringPiece& desc) {
  if (!desc.empty() && desc[0] == '"') {
    // there is no necessary to monitor string in prometheus
    return true;
  }
  if (DumpLatencyRecorderSuffix(name, desc)) {
    // Has encountered name with suffix exposed by LatencyRecorder,
    // Leave it to DumpLatencyRecorderSuffix to output Summary.
    return true;
  }

  auto get_metrics_name = [](const std::string& name) -> std::string_view {
    auto pos = name.find_last_of('{');
    if (pos == std::string::npos) {
      return name;
    }
    return std::string_view(name.data(), pos);
  };

  *os_ << "# HELP " << name << '\n'
       << "# TYPE " << get_metrics_name(name) << " gauge" << '\n'
       << name << " " << desc << '\n';
  return true;
}

const PrometheusMetricsDumper::SummaryItems* PrometheusMetricsDumper::ProcessLatencyRecorderSuffix(
    const butil::StringPiece& name, const butil::StringPiece& desc) {
  static std::string latency_names[] = {butil::string_printf("_latency_%d", (int)bvar::FLAGS_bvar_latency_p1),
                                        butil::string_printf("_latency_%d", (int)bvar::FLAGS_bvar_latency_p2),
                                        butil::string_printf("_latency_%d", (int)bvar::FLAGS_bvar_latency_p3),
                                        "_latency_999",
                                        "_latency_9999",
                                        "_max_latency"};
  CHECK(kNpercentiles == arraysize(latency_names));
  const std::string desc_str = desc.as_string();
  butil::StringPiece metric_name(name);
  for (int i = 0; i < kNpercentiles; ++i) {
    if (!metric_name.ends_with(latency_names[i])) {
      continue;
    }
    metric_name.remove_suffix(latency_names[i].size());
    SummaryItems* si = &m_[metric_name.as_string()];
    si->latency_percentiles[i] = desc_str;
    if (i == kNpercentiles - 1) {
      // '_max_latency' is the last suffix name that appear in the sorted bvar
      // list, which means all related percentiles have been gathered and we are
      // ready to output a Summary.
      si->metric_name = metric_name.as_string();
    }
    return si;
  }
  // Get the average of latency in recent window size
  if (metric_name.ends_with("_latency")) {
    metric_name.remove_suffix(8);
    SummaryItems* si = &m_[metric_name.as_string()];
    si->latency_avg = strtoll(desc_str.data(), nullptr, 10);
    return si;
  }
  if (metric_name.ends_with("_count")) {
    metric_name.remove_suffix(6);
    SummaryItems* si = &m_[metric_name.as_string()];
    si->count = strtoll(desc_str.data(), nullptr, 10);
    return si;
  }
  return nullptr;
}

bool PrometheusMetricsDumper::DumpLatencyRecorderSuffix(const butil::StringPiece& name,
                                                        const butil::StringPiece& desc) {
  if (!name.starts_with(server_prefix_)) {
    return false;
  }
  const SummaryItems* si = ProcessLatencyRecorderSuffix(name, desc);
  if (!si) {
    return false;
  }
  if (!si->IsComplete()) {
    return true;
  }
  *os_ << "# HELP " << si->metric_name << '\n'
       << "# TYPE " << si->metric_name << " summary\n"
       << si->metric_name << "{quantile=\"" << (double)(bvar::FLAGS_bvar_latency_p1) / 100 << "\"} "
       << si->latency_percentiles[0] << '\n'
       << si->metric_name << "{quantile=\"" << (double)(bvar::FLAGS_bvar_latency_p2) / 100 << "\"} "
       << si->latency_percentiles[1] << '\n'
       << si->metric_name << "{quantile=\"" << (double)(bvar::FLAGS_bvar_latency_p3) / 100 << "\"} "
       << si->latency_percentiles[2] << '\n'
       << si->metric_name << "{quantile=\"0.999\"} " << si->latency_percentiles[3] << '\n'
       << si->metric_name << "{quantile=\"0.9999\"} " << si->latency_percentiles[4] << '\n'
       << si->metric_name << "{quantile=\"1\"} " << si->latency_percentiles[5] << '\n'
       << si->metric_name << "{quantile=\"avg\"} " << si->latency_avg << '\n'
       << si->metric_name
       << "_sum "
       // There is no sum of latency in bvar output, just use
       // average * count as approximation
       << si->latency_avg * si->count << '\n'
       << si->metric_name << "_count " << si->count << '\n';
  return true;
}

int DumpPrometheusMetricsToIOBuf(butil::IOBuf* output) {
  butil::IOBufBuilder os;
  PrometheusMetricsDumper dumper(&os, "rpc_server");
  const int ndump = bvar::Variable::dump_exposed(&dumper, nullptr);
  if (ndump < 0) {
    return -1;
  }
  os.move_to(*output);

  PrometheusMetricsDumper dumper_md(&os, "rpc_server");
  const int ndump_md = bvar::MVariable::dump_exposed(&dumper_md, nullptr);
  if (ndump_md < 0) {
    return -1;
  }
  output->append(butil::IOBuf::Movable(os.buf()));
  return 0;
}

void NodeServiceImpl::DingoMetrics(google::protobuf::RpcController* controller,
                                   const pb::node::MetricsRequest* /*request*/, pb::node::MetricsResponse* /*response*/,
                                   google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("text/plain");
  if (DumpPrometheusMetricsToIOBuf(&cntl->response_attachment()) != 0) {
    cntl->SetFailed("Fail to dump metrics");
    return;
  }
}

void NodeServiceImpl::SetFailPoint(google::protobuf::RpcController*, const pb::node::SetFailPointRequest* request,
                                   pb::node::SetFailPointResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  const auto& failpoint = request->failpoint();
  if (failpoint.name().empty() || failpoint.config().empty()) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    error->set_errmsg("Param is error.");
    return;
  }

  auto status = FailPointManager::GetInstance().ConfigureFailPoint(failpoint.name(), failpoint.config());
  if (!status.ok()) {
    auto* error = response->mutable_error();
    error->set_errcode(static_cast<Errno>(status.error_code()));
    error->set_errmsg(status.error_str());
  }
}

void NodeServiceImpl::GetFailPoints(google::protobuf::RpcController*, const pb::node::GetFailPointRequest* request,
                                    pb::node::GetFailPointResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  std::vector<std::shared_ptr<FailPoint>> failpoints;
  if (request->names().empty()) {
    failpoints = FailPointManager::GetInstance().GetAllFailPoints();
  } else {
    for (const auto& name : request->names()) {
      auto failpoint = FailPointManager::GetInstance().GetFailPoint(name);
      if (failpoint != nullptr) {
        failpoints.push_back(failpoint);
      }
    }
  }

  for (const auto& failpoint : failpoints) {
    auto* mut_failpoint = response->add_failpoints();
    mut_failpoint->set_name(failpoint->Name());
    mut_failpoint->set_config(failpoint->Config());

    for (auto& action : failpoint->GetActions()) {
      auto* mut_action = mut_failpoint->add_actions();
      mut_action->set_percent(action->Percent());
      mut_action->set_max_count(action->MaxCount());
      mut_action->set_run_count(action->Count());
      mut_action->set_type(action->GetType());
      mut_action->set_arg(action->Arg());
    }
  }
}

void NodeServiceImpl::DeleteFailPoints(google::protobuf::RpcController*,
                                       const pb::node::DeleteFailPointRequest* request,
                                       pb::node::DeleteFailPointResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  for (const auto& name : request->names()) {
    FailPointManager::GetInstance().DeleteFailPoint(name);
  }
}

butil::Status ValidateInstallVectorIndexSnapshotRequest(const pb::node::InstallVectorIndexSnapshotRequest* request) {
  if (request->meta().filenames().empty()) {
    return butil::Status(Errno::EILLEGAL_PARAMTETERS, "Param filename is error.");
  }

  if (request->meta().snapshot_log_index() <= 1) {
    return butil::Status(Errno::EILLEGAL_PARAMTETERS, "Param snapshot_log_index is error.");
  }

  return butil::Status();
}

void NodeServiceImpl::InstallVectorIndexSnapshot(google::protobuf::RpcController* controller,
                                                 const pb::node::InstallVectorIndexSnapshotRequest* request,
                                                 pb::node::InstallVectorIndexSnapshotResponse* response,
                                                 google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = (brpc::Controller*)controller;

  auto status = ValidateInstallVectorIndexSnapshotRequest(request);
  if (!status.ok()) {
    auto* error = response->mutable_error();
    error->set_errcode(static_cast<Errno>(status.error_code()));
    error->set_errmsg(status.error_str());
    DINGO_LOG(INFO) << fmt::format("InstallVectorIndexSnapshot request: {} response: {}", request->ShortDebugString(),
                                   response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, nullptr, response);
  status = VectorIndexSnapshot::HandleInstallSnapshot(ctx, request->uri(), request->meta());
  if (!status.ok()) {
    auto* error = response->mutable_error();
    error->set_errcode(static_cast<Errno>(status.error_code()));
    error->set_errmsg(status.error_str());
  }

  DINGO_LOG(INFO) << fmt::format("InstallVectorIndexSnapshot request: {} response: {}", request->ShortDebugString(),
                                 response->ShortDebugString());
}

void NodeServiceImpl::GetVectorIndexSnapshot(google::protobuf::RpcController* controller,
                                             const pb::node::GetVectorIndexSnapshotRequest* request,
                                             pb::node::GetVectorIndexSnapshotResponse* response,
                                             google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = (brpc::Controller*)controller;

  auto vector_index_manager = Server::GetInstance()->GetVectorIndexManager();
  if (vector_index_manager->GetVectorIndex(request->vector_index_id()) == nullptr) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EVECTOR_INDEX_NOT_FOUND);
    error->set_errmsg("Not found vector index.");
    DINGO_LOG(INFO) << fmt::format("GetVectorIndexSnapshot request: {} response: {}", request->ShortDebugString(),
                                   response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, nullptr, response);
  auto status = VectorIndexSnapshot::HandlePullSnapshot(ctx, request->vector_index_id());
  if (!status.ok()) {
    auto* error = response->mutable_error();
    error->set_errcode(static_cast<Errno>(status.error_code()));
    error->set_errmsg(status.error_str());
  }
  DINGO_LOG(INFO) << fmt::format("GetVectorIndexSnapshot request: {} response: {}", request->ShortDebugString(),
                                 response->ShortDebugString());
}

}  // namespace dingodb
