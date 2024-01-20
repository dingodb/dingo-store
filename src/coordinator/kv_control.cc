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

#include "coordinator/kv_control.h"

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "braft/configuration.h"
#include "bthread/mutex.h"
#include "butil/containers/flat_map.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/safe_map.h"
#include "coordinator/coordinator_meta_storage.h"
#include "coordinator/coordinator_prefix.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "server/server.h"

namespace dingodb {

KvControl::KvControl(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer,
                     std::shared_ptr<RawEngine> raw_engine_of_meta)
    : meta_reader_(meta_reader), meta_writer_(meta_writer), leader_term_(-1), raw_engine_of_meta_(raw_engine_of_meta) {
  // init bthread mutex
  bthread_mutex_init(&lease_to_key_map_temp_mutex_, nullptr);
  bthread_mutex_init(&one_time_watch_map_mutex_, nullptr);
  leader_term_.store(-1, butil::memory_order_release);

  // the data structure below will write to raft
  id_epoch_meta_ = new MetaMemMapFlat<pb::coordinator_internal::IdEpochInternal>(&id_epoch_map_, kPrefixKvIdEpoch,
                                                                                 raw_engine_of_meta);

  // version kv
  kv_lease_meta_ =
      new MetaMemMapFlat<pb::coordinator_internal::LeaseInternal>(&kv_lease_map_, kPrefixKvLease, raw_engine_of_meta);
  kv_index_meta_ =
      new MetaMemMapStd<pb::coordinator_internal::KvIndexInternal>(&kv_index_map_, kPrefixKvIndex, raw_engine_of_meta);
  kv_rev_meta_ = new MetaDiskMap<pb::coordinator_internal::KvRevInternal>(kPrefixKvRev, raw_engine_of_meta_);

  // init SafeMap
  id_epoch_map_.Init(100);  // id_epoch_map_ is a small map
  // version kv
  kv_lease_map_.Init(10000);
}

KvControl::~KvControl() {
  delete id_epoch_meta_;
  delete kv_lease_meta_;
  delete kv_index_meta_;
  delete kv_rev_meta_;
}

// InitIds
// Setup some initial ids for human readable
void KvControl::InitIds() {
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION, 10000);
  }
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_LEASE) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_LEASE, 90000);
  }
}

bool KvControl::Recover() {
  DINGO_LOG(INFO) << "KvControl start to Recover";

  std::vector<pb::common::KeyValue> kvs;

  // 0.id_epoch map
  if (!meta_reader_->Scan(id_epoch_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);

    if (!id_epoch_meta_->Recover(kvs)) {
      return false;
    }

    // set id_epoch_map_ present id
    InitIds();

    DINGO_LOG(WARNING) << "id_epoch_map_ size=" << id_epoch_map_.Size();
    DINGO_LOG(WARNING) << "term=" << id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM);
    DINGO_LOG(WARNING) << "index="
                       << id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX);
  }
  DINGO_LOG(INFO) << "Recover id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 14.lease map
  if (!meta_reader_->Scan(kv_lease_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    if (!kv_lease_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover lease_meta, count=" << kvs.size();
  kvs.clear();

  // 15.kv_index map
  if (!meta_reader_->Scan(kv_index_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    if (!kv_index_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover kv_index_meta, count=" << kvs.size();
  kvs.clear();

  // 16.kv_rev map
  if (!meta_reader_->Scan(kv_rev_meta_->Prefix(), kvs)) {
    return false;
  }
  // {
  //   if (!kv_rev_meta_->Recover(kvs)) {
  //     return false;
  //   }
  // }
  DINGO_LOG(INFO) << "Recover kv_rev_meta, count=" << kvs.size();
  kvs.clear();

  // build id_epoch, schema_name, table_name, index_name maps
  BuildTempMaps();

  // build version_lease_to_key_map_temp_
  BuildLeaseToKeyMap();
  DINGO_LOG(INFO) << "Recover lease_to_key_map_temp, count=" << lease_to_key_map_temp_.size();

  std::map<std::string, pb::coordinator_internal::KvRevInternal> kv_rev_map;
  kv_rev_meta_->GetAllIdElements(kv_rev_map);
  for (auto& kv : kv_rev_map) {
    DINGO_LOG(INFO) << "kv_rev_map key=" << Helper::StringToHex(kv.first) << " value=" << kv.second.DebugString();
  }

  return true;
}

void KvControl::GetServerLocation(pb::common::Location& raft_location, pb::common::Location& server_location) {
  // find in cache
  auto raft_location_string = raft_location.host() + ":" + std::to_string(raft_location.port());
  if (coordinator_location_cache_.find(raft_location_string) != coordinator_location_cache_.end()) {
    server_location = coordinator_location_cache_[raft_location_string];
    DINGO_LOG(DEBUG) << "GetServiceLocation Cache Hit raft_location=" << raft_location.host() << ":"
                     << raft_location.port();
    return;
  }

  Helper::GetServerLocation(raft_location, server_location);

  // transform ip to hostname
  Server::GetInstance().Ip2Hostname(*server_location.mutable_host());

  // add to cache if get server_location
  if (server_location.host().length() > 0 && server_location.port() > 0) {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, add new cache raft_location=" << raft_location.host() << ":"
                    << raft_location.port();
    coordinator_location_cache_[raft_location_string] = server_location;
  } else {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, can't get server_location, raft_location="
                    << raft_location.host() << ":" << raft_location.port();
  }
}

void KvControl::GetRaftLocation(pb::common::Location& server_location, pb::common::Location& raft_location) {
  // find in cache
  auto server_location_string = server_location.host() + ":" + std::to_string(server_location.port());
  if (coordinator_location_cache_.find(server_location_string) != coordinator_location_cache_.end()) {
    raft_location = coordinator_location_cache_[server_location_string];
    DINGO_LOG(INFO) << "GetServiceLocation Cache Hit server_location=" << server_location.host() << ":"
                    << server_location.port();
    return;
  }

  Helper::GetServerLocation(server_location, raft_location);

  // add to cache if get raft_location
  if (raft_location.host().length() > 0 && raft_location.port() > 0) {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, add new cache server_location=" << server_location.host() << ":"
                    << server_location.port();
    coordinator_location_cache_[server_location_string] = raft_location;
  } else {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, can't get raft_location, server_location="
                    << server_location.host() << ":" << server_location.port();
  }
}

void KvControl::GetLeaderLocation(pb::common::Location& leader_server_location) {
  if (raft_node_ == nullptr) {
    DINGO_LOG(ERROR) << "GetLeaderLocation raft_node_ is nullptr";
    return;
  }

  // parse leader raft location from string
  auto leader_string = raft_node_->GetLeaderId().to_string();

  pb::common::Location leader_raft_location;
  int ret = Helper::PeerIdToLocation(raft_node_->GetLeaderId(), leader_raft_location);
  if (ret < 0) {
    return;
  }

  // GetServerLocation
  GetServerLocation(leader_raft_location, leader_server_location);
}

// GetNextId only update id_epoch_map_temp_ in leader, the persistent id_epoch_map_ will be updated in on_apply
// When on_leader_start, the id_epoch_map_temp_ will init from id_epoch_map_
// only id_epoch_map_ is in state machine, and will persistent to raft and local rocksdb
int64_t KvControl::GetNextId(const pb::coordinator_internal::IdEpochType& key,
                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  // get next id from id_epoch_map_safe_temp_
  int64_t next_id = 0;
  id_epoch_map_.GetNextId(key, next_id);

  if (next_id == INT64_MAX || next_id < 0) {
    DINGO_LOG(FATAL) << "GetNextId next_id=" << next_id << " key=" << key << ", FATAL id is used up";
  }

  // generate meta_increment
  auto* idepoch = meta_increment.add_idepochs();
  idepoch->set_id(key);
  idepoch->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

  auto* idepoch_internal = idepoch->mutable_idepoch();
  idepoch_internal->set_id(key);
  idepoch_internal->set_value(next_id);

  return next_id;
}

int64_t KvControl::GetPresentId(const pb::coordinator_internal::IdEpochType& key) {
  int64_t value = 0;
  id_epoch_map_.GetPresentId(key, value);

  return value;
}

int64_t KvControl::UpdatePresentId(const pb::coordinator_internal::IdEpochType& key, int64_t new_id,
                                   pb::coordinator_internal::MetaIncrement& meta_increment) {
  // get next id from id_epoch_map_safe_temp_
  id_epoch_map_.UpdatePresentId(key, new_id);

  // generate meta_increment
  auto* idepoch = meta_increment.add_idepochs();
  idepoch->set_id(key);
  idepoch->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

  auto* idepoch_internal = idepoch->mutable_idepoch();
  idepoch_internal->set_id(key);
  idepoch_internal->set_value(new_id);

  return new_id;
}

void KvControl::GetMemoryInfo(pb::coordinator::CoordinatorMemoryInfo& memory_info) {
  // compute size
  memory_info.set_id_epoch_safe_map_temp_count(id_epoch_map_.Size());
  memory_info.set_id_epoch_safe_map_temp_size(id_epoch_map_.MemorySize());

  {
    // BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);

    // set term & index
    pb::coordinator_internal::IdEpochInternal temp_term;
    pb::coordinator_internal::IdEpochInternal temp_index;
    int ret_term = id_epoch_map_.Get(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM, temp_term);
    int ret_index = id_epoch_map_.Get(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX, temp_index);

    if (ret_term >= 0) {
      memory_info.set_applied_term(temp_term.value());
    }
    if (ret_index >= 0) {
      memory_info.set_applied_index(temp_index.value());
    }

    // set count & size
    memory_info.set_id_epoch_map_count(id_epoch_map_.Size());
    memory_info.set_total_size(memory_info.total_size() + id_epoch_map_.MemorySize());

    // dump id & epoch to kv
    butil::FlatMap<int64_t, pb::coordinator_internal::IdEpochInternal> id_epoch_map_temp;
    id_epoch_map_temp.init(100);
    int ret = id_epoch_map_.GetRawMapCopy(id_epoch_map_temp);
    for (auto& it : id_epoch_map_temp) {
      const google::protobuf::EnumDescriptor* enum_descriptor =
          dingodb::pb::coordinator_internal::IdEpochType_descriptor();
      const google::protobuf::EnumValueDescriptor* enum_value_descriptor = enum_descriptor->FindValueByNumber(it.first);
      std::string name = enum_value_descriptor->name();

      auto* id_epoch = memory_info.add_id_epoch_values();
      id_epoch->set_key(name);
      id_epoch->set_value(std::to_string(it.second.value()));
    }
    {
      memory_info.set_kv_lease_map_count(kv_lease_map_.Size());
      memory_info.set_kv_lease_map_size(kv_lease_map_.MemorySize());
      memory_info.set_total_size(memory_info.total_size() + memory_info.kv_lease_map_size());
    }
    {
      memory_info.set_kv_index_map_count(kv_index_map_.Size());
      memory_info.set_kv_index_map_size(kv_index_map_.MemorySize());
      memory_info.set_total_size(memory_info.total_size() + memory_info.kv_index_map_size());
    }
    {
      int64_t kv_rev_count = kv_rev_meta_->Count();
      memory_info.set_kv_rev_map_count(kv_rev_count);
    }
    {
      int64_t kv_watch_count = one_time_watch_closure_status_map_.Size();
      memory_info.set_kv_watch_count(kv_watch_count);
    }
  }
}

}  // namespace dingodb
