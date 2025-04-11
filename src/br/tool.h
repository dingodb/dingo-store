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

#ifndef DINGODB_BR_TOOL_H_
#define DINGODB_BR_TOOL_H_

#include <memory>

#include "br/parameter.h"
#include "br/tool_client.h"
#include "br/tool_diff.h"
#include "br/tool_dump.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class Tool : public std::enable_shared_from_this<Tool> {
 public:
  Tool(const ToolParams& params);
  ~Tool();

  Tool(const Tool&) = delete;
  const Tool& operator=(const Tool&) = delete;
  Tool(Tool&&) = delete;
  Tool& operator=(Tool&&) = delete;

  std::shared_ptr<Tool> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  ToolParams tool_params_;
  std::shared_ptr<ToolDump> tool_dump_;
  std::shared_ptr<ToolDiff> tool_diff_;
  std::shared_ptr<ToolClient> tool_client_;
};

}  // namespace br

#endif  // DINGODB_BR_TOOL_H_