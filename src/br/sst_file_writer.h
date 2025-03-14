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

#ifndef DINGODB_BR_SST_FILE_WRITER_H_
#define DINGODB_BR_SST_FILE_WRITER_H_

#include <cstdint>
#include <string>

#include "butil/status.h"
#include "proto/common.pb.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace br {
class SstFileWriter {
 public:
  SstFileWriter(const rocksdb::Options& options)
      : options_(options),
        sst_writer_(std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), options_, nullptr, true)) {}
  ~SstFileWriter() = default;

  SstFileWriter(SstFileWriter&& rhs) = delete;
  SstFileWriter& operator=(SstFileWriter&& rhs) = delete;

  butil::Status SaveFile(const std::map<std::string, std::string>& kvs, const std::string& filename);

  int64_t GetSize() { return sst_writer_->FileSize(); }

 private:
  rocksdb::Options options_;
  std::unique_ptr<rocksdb::SstFileWriter> sst_writer_;
};
using SstFileWriterPtr = std::shared_ptr<SstFileWriter>;
}  // namespace br

#endif  // DINGODB_BR_SST_FILE_WRITER_H_