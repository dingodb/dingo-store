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

#include "server/service_helper.h"

#include <string>
#include <string_view>

#include "common/helper.h"
#include "fmt/core.h"

namespace dingodb {

// Validate region state
butil::Status ServiceHelper::ValidateRegionState(store::RegionPtr region) {
  // Check is exist region.
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }
  if (region->State() == pb::common::StoreRegionState::NEW) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is new, waiting later");
  }
  if (region->State() == pb::common::StoreRegionState::STANDBY) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is standby, waiting later");
  }
  if (region->State() == pb::common::StoreRegionState::DELETING) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is deleting");
  }
  if (region->State() == pb::common::StoreRegionState::DELETED) {
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "Region is deleted");
  }

  return butil::Status();
}

butil::Status ServiceHelper::ValidateRange(const pb::common::Range& range) {
  if (BAIDU_UNLIKELY(range.start_key().empty() || range.end_key().empty())) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Range key is empty");
  }

  if (BAIDU_UNLIKELY(range.start_key() >= range.end_key())) {
    return butil::Status(pb::error::ERANGE_INVALID, "Range is invalid");
  }

  return butil::Status();
}

// Validate key in range
butil::Status ServiceHelper::ValidateKeyInRange(const pb::common::Range& range,
                                                const std::vector<std::string_view>& keys) {
  for (const auto& key : keys) {
    if (range.start_key().compare(key) > 0 || range.end_key().compare(key) <= 0) {
      return butil::Status(
          pb::error::EKEY_OUT_OF_RANGE,
          fmt::format("Key out of range, region range[{}-{}] key[{}]", Helper::StringToHex(range.start_key()),
                      Helper::StringToHex(range.end_key()), Helper::StringToHex(key)));
    }
  }

  return butil::Status();
}

// Validate range in range [)
butil::Status ServiceHelper::ValidateRangeInRange(const pb::common::Range& region_range,
                                                  const pb::common::Range& req_range) {
  // Validate start_key
  int min_length = std::min(region_range.start_key().size(), req_range.start_key().size());
  std::string_view req_truncate_start_key(req_range.start_key().data(), min_length);
  std::string_view region_truncate_start_key(region_range.start_key().data(), min_length);
  if (req_truncate_start_key < region_truncate_start_key) {
    return butil::Status(
        pb::error::EKEY_OUT_OF_RANGE,
        fmt::format("Key out of range, region range[{}-{}] req range[{}-{}]",
                    Helper::StringToHex(region_range.start_key()), Helper::StringToHex(region_range.end_key()),
                    Helper::StringToHex(req_range.start_key()), Helper::StringToHex(req_range.end_key())));
  }

  // Validate end_key
  min_length = std::min(region_range.end_key().size(), req_range.end_key().size());
  std::string_view req_truncate_end_key(req_range.end_key().data(), min_length);
  std::string_view region_truncate_end_key(region_range.end_key().data(), min_length);

  std::string next_prefix_key;
  if (req_range.end_key().size() > region_range.end_key().size()) {
    next_prefix_key = Helper::PrefixNext(req_truncate_end_key);
    req_truncate_end_key = std::string_view(next_prefix_key.data(), next_prefix_key.size());
  } else if (req_range.end_key().size() < region_range.end_key().size()) {
    next_prefix_key = Helper::PrefixNext(region_truncate_end_key);
    region_truncate_end_key = std::string_view(next_prefix_key.data(), next_prefix_key.size());
  }

  if (req_truncate_end_key > region_truncate_end_key) {
    return butil::Status(
        pb::error::EKEY_OUT_OF_RANGE,
        fmt::format("Key out of range, region range[{}-{}] req range[{}-{}]",
                    Helper::StringToHex(region_range.start_key()), Helper::StringToHex(region_range.end_key()),
                    Helper::StringToHex(req_range.start_key()), Helper::StringToHex(req_range.end_key())));
  }

  return butil::Status();
}

butil::Status ServiceHelper::ValidateRegion(uint64_t region_id, const std::vector<std::string_view>& keys) {
  // Todo: use double buffer map replace.
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(region_id);

  auto status = ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  status = ValidateKeyInRange(region->Range(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

}  // namespace dingodb