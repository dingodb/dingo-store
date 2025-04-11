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

#include "br/tool.h"

#include <memory>

#include "br/tool_client.h"
#include "br/utils.h"
#include "common/logging.h"
#include "fmt/core.h"

namespace br {

Tool::Tool(const ToolParams& params) : tool_params_(params) {}

Tool::~Tool(){};

std::shared_ptr<Tool> Tool::GetSelf() { return shared_from_this(); }

butil::Status Tool::Init() {
  butil::Status status;

  if ("dump" == tool_params_.br_tool_type) {
    ToolDumpParams tool_dump_params;
    tool_dump_params.br_type = tool_params_.br_type;
    tool_dump_params.br_tool_type = tool_params_.br_tool_type;
    tool_dump_params.br_dump_file = tool_params_.br_dump_file;
    tool_dump_ = std::make_shared<ToolDump>(tool_dump_params);
  } else if ("diff" == tool_params_.br_tool_type) {
    ToolDiffParams tool_diff_params;
    tool_diff_params.br_type = tool_params_.br_type;
    tool_diff_params.br_tool_type = tool_params_.br_tool_type;
    tool_diff_params.br_diff_file1 = tool_params_.br_diff_file1;
    tool_diff_params.br_diff_file2 = tool_params_.br_diff_file2;
    tool_diff_ = std::make_shared<ToolDiff>(tool_diff_params);
  } else if ("client" == tool_params_.br_tool_type) {
    ToolClientParams tool_client_params;
    tool_client_params.br_type = tool_params_.br_type;
    tool_client_params.br_tool_type = tool_params_.br_tool_type;
    tool_client_params.br_client_method = tool_params_.br_client_method;
    tool_client_params.br_client_method_param1 = tool_params_.br_client_method_param1;

    tool_client_ = std::make_shared<ToolClient>(tool_client_params);
  } else {
    std::string s = fmt::format("tool type not support. use dump or diff. {}", tool_params_.br_tool_type);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  if (tool_dump_) {
    status = tool_dump_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (tool_diff_) {
    status = tool_diff_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (tool_client_) {
    status = tool_client_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status Tool::Run() {
  butil::Status status;

  if (tool_dump_) {
    status = tool_dump_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (tool_diff_) {
    status = tool_diff_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (tool_client_) {
    status = tool_client_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status Tool::Finish() {
  butil::Status status;

  if (tool_dump_) {
    status = tool_dump_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (tool_diff_) {
    status = tool_diff_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (tool_client_) {
    status = tool_client_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  return butil::Status::OK();
}

}  // namespace br