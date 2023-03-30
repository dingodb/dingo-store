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

#include "engine/storage.h"

#include <vector>

#include "common/constant.h"
#include "common/helper.h"
#include "engine/write_data.h"
#include "proto/common.pb.h"

namespace dingodb {

Storage::Storage(std::shared_ptr<Engine> engine) : engine_(engine) {}

Storage::~Storage() = default;

Snapshot* Storage::GetSnapshot() { return nullptr; }

void Storage::ReleaseSnapshot() {}

butil::Status Storage::KvGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                             std::vector<pb::common::KeyValue>& kvs) {
  auto reader = engine_->NewReader(Constant::kStoreDataCF);
  for (const auto& key : keys) {
    std::string value;
    auto status = reader->KvGet(ctx, key, value);
    if (!status.ok()) {
      if (pb::error::EKEY_NOTFOUND == status.error_code()) {
        continue;
      }
      kvs.clear();
      return status;
    }

    pb::common::KeyValue kv;
    kv.set_key(key);
    kv.set_value(value);
    kvs.emplace_back(kv);
  }

  return butil::Status();
}

butil::Status Storage::KvPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs) {
  WriteData write_data;
  std::shared_ptr<PutDatum> datum = std::make_shared<PutDatum>();
  datum->cf_name = ctx->CfName();
  datum->kvs = kvs;
  write_data.AddDatums(std::static_pointer_cast<DatumAble>(datum));

  return engine_->AsyncWrite(ctx, write_data, [ctx](butil::Status status) {
    if (!status.ok()) {
      Helper::SetPbMessageError(status, ctx->Response());
    }
  });
}

butil::Status Storage::KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                     bool is_atomic) {
  WriteData write_data;
  std::shared_ptr<PutIfAbsentDatum> datum = std::make_shared<PutIfAbsentDatum>();
  datum->cf_name = ctx->CfName();
  datum->kvs = kvs;
  datum->is_atomic = is_atomic;
  write_data.AddDatums(std::static_pointer_cast<DatumAble>(datum));

  return engine_->AsyncWrite(ctx, write_data, [ctx](butil::Status status) {
    if (!status.ok()) {
      Helper::SetPbMessageError(status, ctx->Response());
    }
  });
}

butil::Status Storage::KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys) {
  WriteData write_data;
  std::shared_ptr<DeleteBatchDatum> datum = std::make_shared<DeleteBatchDatum>();
  datum->cf_name = ctx->CfName();
  datum->keys = std::move(const_cast<std::vector<std::string>&>(keys));  // NOLINT
  write_data.AddDatums(std::static_pointer_cast<DatumAble>(datum));

  return engine_->AsyncWrite(ctx, write_data, [ctx](butil::Status status) {
    if (!status.ok()) {
      Helper::SetPbMessageError(status, ctx->Response());
    }
  });
}

butil::Status Storage::KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) {
  WriteData write_data;
  std::shared_ptr<DeleteRangeDatum> datum = std::make_shared<DeleteRangeDatum>();
  datum->cf_name = ctx->CfName();
  datum->ranges.emplace_back(std::move(const_cast<pb::common::Range&>(range)));
  write_data.AddDatums(std::static_pointer_cast<DatumAble>(datum));

  return engine_->AsyncWrite(ctx, write_data, [ctx](butil::Status status) {
    if (!status.ok()) {
      Helper::SetPbMessageError(status, ctx->Response());
    }
  });
}

}  // namespace dingodb
