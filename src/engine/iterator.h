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

#ifndef DINGODB_ENGINE_ITERATOR_H_
#define DINGODB_ENGINE_ITERATOR_H_

#include <memory>
#include <string_view>

#include "butil/status.h"
#include "common/logging.h"

namespace dingodb {

enum class IteratorType {
  kRawRocksEngine = 0,
  kRawXdpEngine = 1,
  kMemEngine = 2,
  kRocksEngine = 3,
  kRaftEngine = 4,
  kColumnarEngine = 5,
  kBdbEngine = 6,
};

struct IteratorOptions {
  std::string lower_bound;
  std::string upper_bound;
};

class Iterator {
 public:
  Iterator() = default;
  virtual ~Iterator() = default;

  Iterator(const Iterator&) = delete;
  void operator=(const Iterator&) = delete;

  virtual std::string GetName() = 0;
  virtual IteratorType GetID() = 0;

  virtual bool Valid() const = 0;

  virtual void SeekToFirst() { DINGO_LOG(ERROR) << "Not support SeekToFirst"; };
  virtual void SeekToLast() { DINGO_LOG(ERROR) << "Not support SeekToLast"; };

  virtual void Seek(const std::string& target) = 0;
  virtual void SeekForPrev(const std::string& /*target*/) { DINGO_LOG(ERROR) << "Not support SeekForPrev"; };

  virtual void Next() = 0;
  virtual void Prev() { DINGO_LOG(ERROR) << "Not support Prev"; };

  virtual std::string_view Key() const = 0;
  virtual std::string_view Value() const = 0;

  virtual butil::Status Status() const = 0;
};

using IteratorPtr = std::shared_ptr<Iterator>;

};  // namespace dingodb

#endif  // DINGODB_ENGINE_ITERATOR_H_
