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

#ifndef DINGODB_BR_TOOL_DUMP_H_
#define DINGODB_BR_TOOL_DUMP_H_

#include <functional>
#include <memory>

#include "br/parameter.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class ToolDump : public std::enable_shared_from_this<ToolDump> {
 public:
  ToolDump(const ToolDumpParams& params);
  ~ToolDump();

  ToolDump(const ToolDump&) = delete;
  const ToolDump& operator=(const ToolDump&) = delete;
  ToolDump(ToolDump&&) = delete;
  ToolDump& operator=(ToolDump&&) = delete;

  std::shared_ptr<ToolDump> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  static butil::Status DumpDirect(const std::string& path, const std::string& file_name, const std::string& desc,
                                  std::function<butil::Status(const std::string&, std::string&)> handler);
  static butil::Status DumpSst(const std::string& path, const std::string& file_name, const std::string& desc,
                               std::function<butil::Status(const std::string&, std::string&)> handler);
  static butil::Status DumpRegionCfSst(
      const std::string& path, const std::string& file_name, const std::string& desc,
      std::function<butil::Status(const std::string&, std::string&, std::string&, std::string&)> handler);

  static butil::Status DumpRegionDefintionSst(
      const std::string& path, const std::string& file_name, const std::string& desc,
      std::function<butil::Status(const std::string&, std::string&, std::string&, std::string&)> handler);

  static butil::Status DumpRegionDataSst(
      const std::string& path, const std::string& file_name, const std::string& desc,
      std::function<butil::Status(const std::string&, std::string&, std::string&, std::string&)> handler);

  static butil::Status DumpDirectFunction(const std::string& path, std::string& content);
  static butil::Status DumpBackupMetaFunction(const std::string& path, std::string& content);
  static butil::Status DumpBackupMetaDataFileFunction(const std::string& path, std::string& content);
  static butil::Status DumpBackupMetaSchemaFunction(const std::string& path, std::string& content);
  static butil::Status DumpCoordinatorSdkMetaFunction(const std::string& path, std::string& content);
  static butil::Status DumpRegionCfFunction(const std::string& path, std::string& content_summary,
                                            std::string& content_brief, std::string& content_detail);
  static butil::Status DumpRegionDefinitionFunction(const std::string& path, std::string& content_summary,
                                                    std::string& content_brief, std::string& content_detail);
  static butil::Status DumpRegionDataFunction(const std::string& path, std::string& content_summary,
                                              std::string& content_brief, std::string& content_detail);

  ToolDumpParams tool_dump_params_;
  std::string path_internal_;
  std::string file_name_;
};

}  // namespace br

#endif  // DINGODB_BR_TOOL_DUMP_H_