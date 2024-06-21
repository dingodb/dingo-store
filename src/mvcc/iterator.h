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

#ifndef DINGODB_MVCC_ITERATOR_H_
#define DINGODB_MVCC_ITERATOR_H_

#include <cstdint>
#include <memory>
#include <string>

#include "engine/iterator.h"

namespace dingodb {

namespace mvcc {

class Iterator : public dingodb::Iterator {
 public:
  Iterator(int64_t ts, IteratorPtr iter) : ts_(ts), iter_(iter) {}
  ~Iterator() override = default;

  std::string GetName() override { return "MVCC"; }
  IteratorType GetID() override { return IteratorType::kMVCC; }

  bool Valid() const override;

  void SeekToFirst() override;
  void SeekToLast() override;

  void Seek(const std::string& target) override;
  void SeekForPrev(const std::string& target) override;

  void Next() override;
  void Prev() override;

  std::string_view Key() const override;
  std::string_view Value() const override;

  butil::Status Status() const override;

 private:
  void NextVisibleKey();
  void PrevVisibleKey();

  enum class Type {
    kNone = 0,
    kBackward = 1,
    kForward = 2,
  };

  Type type_{Type::kNone};
  int64_t ts_;
  int64_t now_time_;
  dingodb::IteratorPtr iter_;

  // used by backward iterate
  std::string prev_encode_key_;

  // used by forward iterate
  std::string key_;
  std::string value_;
};

using IteratorPtr = std::shared_ptr<Iterator>;

}  // namespace mvcc

}  // namespace dingodb

#endif  // DINGODB_MVCC_ITERATOR_H_