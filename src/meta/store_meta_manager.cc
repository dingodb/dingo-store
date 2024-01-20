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

#include "meta/store_meta_manager.h"

#include <algorithm>
#include <any>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "bthread/mutex.h"
#include "butil/scoped_lock.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/role.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "server/server.h"

namespace dingodb {

namespace store {

Region::Region(int64_t region_id) {
  inner_region_.set_id(region_id);
  bthread_mutex_init(&mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.Region][id({})]", region_id);
};

Region::~Region() {
  DINGO_LOG(DEBUG) << fmt::format("[delete.Region][id({})]", Id());
  bthread_mutex_destroy(&mutex_);
}

std::shared_ptr<Region> Region::New(int64_t region_id) { return std::make_shared<Region>(region_id); }

std::shared_ptr<Region> Region::New(const pb::common::RegionDefinition& definition) {
  auto region = std::make_shared<Region>(definition.id());
  if (definition.index_parameter().index_type() == pb::common::INDEX_TYPE_VECTOR) {
    region->inner_region_.set_region_type(pb::common::INDEX_REGION);
    auto vector_index_wrapper =
        VectorIndexWrapper::New(definition.id(), definition.index_parameter().vector_index_parameter());
    if (vector_index_wrapper == nullptr) {
      return nullptr;
    }
    region->SetVectorIndexWrapper(vector_index_wrapper);

  } else {
    region->inner_region_.set_region_type(pb::common::STORE_REGION);
  }
  *(region->inner_region_.mutable_definition()) = definition;
  region->SetState(pb::common::StoreRegionState::NEW);

  return region;
}

bool Region::Recover() {
  if (Type() == pb::common::INDEX_REGION) {
    auto vector_index_wrapper =
        VectorIndexWrapper::New(Id(), inner_region_.definition().index_parameter().vector_index_parameter());
    if (vector_index_wrapper == nullptr) {
      return false;
    }
    SetVectorIndexWrapper(vector_index_wrapper);
    return vector_index_wapper_->Recover();
  }

  return true;
}

std::string Region::Serialize() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.SerializeAsString();
}

void Region::DeSerialize(const std::string& data) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.ParsePartialFromArray(data.data(), data.size());
  state_.store(inner_region_.state());
}

pb::common::RegionEpoch Region::Epoch(bool lock) {
  if (lock) {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_.definition().epoch();
  } else {
    return inner_region_.definition().epoch();
  }
}

std::string Region::EpochToString() { return Helper::RegionEpochToString(Epoch()); }

void Region::LockRegionMeta() { bthread_mutex_lock(&mutex_); }

void Region::UnlockRegionMeta() { bthread_mutex_unlock(&mutex_); }

void Region::SetEpochVersionAndRange(int64_t version, const pb::common::Range& range) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.mutable_definition()->mutable_epoch()->set_version(version);
  *(inner_region_.mutable_definition()->mutable_range()) = range;
}

void Region::GetEpochAndRange(pb::common::RegionEpoch& epoch, pb::common::Range& range) {
  BAIDU_SCOPED_LOCK(mutex_);
  epoch = inner_region_.mutable_definition()->epoch();
  range = inner_region_.mutable_definition()->range();
}

void Region::SetEpochConfVersion(int64_t version) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_last_change_job_id(inner_region_.last_change_job_id() + 1);
  inner_region_.mutable_definition()->mutable_epoch()->set_conf_version(version);
}

void Region::SetSnapshotEpochVersion(int64_t version) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_snapshot_epoch_version(version);
}

int64_t Region::LeaderId() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.leader_id();
}

void Region::SetLeaderId(int64_t leader_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_leader_id(leader_id);
}

pb::common::Range Region::Range(bool lock) {
  if (lock) {
    BAIDU_SCOPED_LOCK(mutex_);
    return inner_region_.definition().range();
  } else {
    return inner_region_.definition().range();
  }
}

std::string Region::RangeToString() { return Helper::RangeToString(Range()); }

bool Region::CheckKeyInRange(const std::string& key) {
  auto region_range = Range();
  return key >= region_range.start_key() && key < region_range.end_key();
}

void Region::SetIndexParameter(const pb::common::IndexParameter& index_parameter) {
  BAIDU_SCOPED_LOCK(mutex_);
  *(inner_region_.mutable_definition()->mutable_index_parameter()) = index_parameter;
}

std::vector<pb::common::Peer> Region::Peers() {
  BAIDU_SCOPED_LOCK(mutex_);

  std::vector<pb::common::Peer> peers(inner_region_.definition().peers().begin(),
                                      inner_region_.definition().peers().end());
  return peers;
}

void Region::SetPeers(std::vector<pb::common::Peer>& peers) {
  google::protobuf::RepeatedPtrField<pb::common::Peer> tmp_peers;
  tmp_peers.Add(peers.begin(), peers.end());

  {
    BAIDU_SCOPED_LOCK(mutex_);
    *(inner_region_.mutable_definition()->mutable_peers()) = tmp_peers;
  }
}

pb::common::StoreRegionState Region::State() const { return state_.load(std::memory_order_relaxed); }

void Region::SetState(pb::common::StoreRegionState state) {
  state_.store(state, std::memory_order_relaxed);

  {
    BAIDU_SCOPED_LOCK(mutex_);
    inner_region_.set_state(state);
  }
}

void Region::AppendHistoryState(pb::common::StoreRegionState state) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.add_history_states(state);
}

bool Region::NeedBootstrapDoSnapshot() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.need_bootstrap_do_snapshot();
}

void Region::SetNeedBootstrapDoSnapshot(bool need_do_snapshot) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_need_bootstrap_do_snapshot(need_do_snapshot);
}

bool Region::DisableChange() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.disable_change();
}

void Region::SetDisableChange(bool disable_change) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_disable_change(disable_change);
}

bool Region::TemporaryDisableChange() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.temporary_disable_change();
}

void Region::SetTemporaryDisableChange(bool disable_change) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_temporary_disable_change(disable_change);
}

pb::raft::SplitStrategy Region::SplitStrategy() { return split_strategy_; }
void Region::SetSplitStrategy(pb::raft::SplitStrategy split_strategy) { split_strategy_ = split_strategy; }

int64_t Region::LastSplitTimestamp() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.last_split_timestamp();
}

void Region::UpdateLastSplitTimestamp() {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_last_split_timestamp(Helper::TimestampMs());
}

int64_t Region::ParentId() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.parent_id();
}
void Region::SetParentId(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_parent_id(region_id);
}

int64_t Region::PartitionId() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.definition().part_id();
}

int64_t Region::SnapshotEpochVersion() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.snapshot_epoch_version();
}

pb::store_internal::Region Region::InnerRegion() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_;
}

pb::common::RegionDefinition Region::Definition() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.definition();
}

pb::common::RawEngine Region::GetRawEngineType() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.definition().raw_engine();
}

void Region::SetLastChangeJobId(int64_t job_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_last_change_job_id(job_id);
}

int64_t Region::LastChangeJobId() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.last_change_job_id();
}

bool Region::LatchesAcquire(Lock* lock, uint64_t who) {
  CHECK(lock != nullptr);
  CHECK(who != 0);

  return this->latches_.Acquire(lock, who);
}

void Region::LatchesRelease(Lock* lock, uint64_t who,
                            std::optional<std::pair<uint64_t, Lock*>> keep_latches_for_next_cmd) {
  auto wakeup = this->latches_.Release(lock, who, keep_latches_for_next_cmd);
  for (const auto& cid : wakeup) {
    CHECK(cid != 0);
    BthreadCond* cond = (BthreadCond*)cid;
    cond->DecreaseSignal();
  }
}

RaftMeta::RaftMeta(int64_t region_id) {
  raft_meta_.set_region_id(region_id);
  raft_meta_.set_term(0);
  raft_meta_.set_applied_index(0);
  bthread_mutex_init(&mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.RaftMata][id({})]", region_id);
}

RaftMeta::~RaftMeta() {
  DINGO_LOG(DEBUG) << fmt::format("[delete.RaftMata][id({})]", RegionId());
  bthread_mutex_destroy(&mutex_);
}

std::shared_ptr<RaftMeta> RaftMeta::New(int64_t region_id) { return std::make_shared<RaftMeta>(region_id); }

int64_t RaftMeta::RegionId() {
  BAIDU_SCOPED_LOCK(mutex_);

  return raft_meta_.region_id();
}

int64_t RaftMeta::Term() {
  BAIDU_SCOPED_LOCK(mutex_);

  return raft_meta_.term();
}

int64_t RaftMeta::AppliedId() {
  BAIDU_SCOPED_LOCK(mutex_);

  return raft_meta_.applied_index();
}

void RaftMeta::SetTermAndAppliedId(int64_t term, int64_t applied_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  raft_meta_.set_term(term);
  raft_meta_.set_applied_index(applied_id);
}

std::string RaftMeta::Serialize() {
  BAIDU_SCOPED_LOCK(mutex_);

  return raft_meta_.SerializeAsString();
}

void RaftMeta::DeSerialize(const std::string& data) {
  BAIDU_SCOPED_LOCK(mutex_);

  raft_meta_.ParsePartialFromArray(data.data(), data.size());
}

pb::store_internal::RaftMeta RaftMeta::InnerRaftMeta() {
  BAIDU_SCOPED_LOCK(mutex_);

  return raft_meta_;
}

}  // namespace store

RegionChangeRecorder::RegionChangeRecorder(MetaReaderPtr meta_reader, MetaWriterPtr meta_writer)
    : TransformKvAble(Constant::kStoreRegionChangeRecordPrefix), meta_reader_(meta_reader), meta_writer_(meta_writer) {
  bthread_mutex_init(&mutex_, nullptr);
};

RegionChangeRecorder::~RegionChangeRecorder() { bthread_mutex_destroy(&mutex_); }

bool RegionChangeRecorder::Init() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    DINGO_LOG(ERROR) << "[region.record] Scan region change record failed!";
    return false;
  }
  DINGO_LOG(INFO) << "[region.record] Init region change record num: " << kvs.size();

  if (!kvs.empty()) {
    TransformFromKv(kvs);
  }

  return true;
}

void RegionChangeRecorder::AddChangeRecord(const pb::coordinator::RegionCmd& cmd) {
  if (cmd.job_id() == 0) return;

  pb::store_internal::RegionChangeRecord record;
  record.set_region_id(cmd.region_id());
  record.set_job_type(cmd.region_cmd_type());
  record.set_job_id(cmd.job_id());
  record.set_begin_time(Helper::NowTime());

  std::string event;
  switch (cmd.region_cmd_type()) {
    case pb::coordinator::CMD_CREATE: {
      const auto& request = cmd.create_request();
      const auto& region_definition = request.region_definition();
      event = fmt::format("Pre create region, region({}) for region({})", region_definition.id(),
                          request.split_from_region_id());
      record.set_job_content(event);
      break;
    }
    case pb::coordinator::CMD_HOLD_VECTOR_INDEX: {
      const auto& request = cmd.hold_vector_index_request();
      event = fmt::format("Hold vector index, region({}) is_hold({})", request.region_id(), request.is_hold());
      record.set_job_content(event);
      break;
    }
    case pb::coordinator::CMD_SPLIT: {
      const auto& request = cmd.split_request();
      event = fmt::format("Split region, {} -> {} split_key({}) post_create_region({})", request.split_from_region_id(),
                          request.split_to_region_id(), Helper::StringToHex(request.split_watershed_key()),
                          request.store_create_region());
      record.set_job_content(event);
      break;
    }
    case pb::coordinator::CMD_MERGE: {
      const auto& request = cmd.merge_request();
      event = fmt::format("Merge region, {} -> {}", request.source_region_id(), request.target_region_id());
      record.set_job_content(event);
      break;
    }
    case pb::coordinator::CMD_CHANGE_PEER: {
      const auto& request = cmd.change_peer_request();
      event = fmt::format("Change peer, {}",
                          Helper::PeersToString(Helper::PbRepeatedToVector(request.region_definition().peers())));
      record.set_job_content(event);
      break;
    }
    default:
      return;
  }

  Upsert(record, event);
}

void RegionChangeRecorder::AddChangeRecord(const pb::raft::SplitRequest& request) {
  if (request.job_id() == 0) return;

  pb::store_internal::RegionChangeRecord record;
  record.set_region_id(request.from_region_id());
  record.set_begin_time(Helper::NowTime());
  record.set_job_type(pb::coordinator::CMD_SPLIT);
  record.set_job_id(request.job_id());

  record.set_job_content(
      fmt::format("Split region, {} -> {} epoch({}) split_key({}) strategy({})", request.from_region_id(),
                  request.to_region_id(), Helper::RegionEpochToString(request.epoch()),
                  Helper::StringToHex(request.split_key()), pb::raft::SplitStrategy_Name(request.split_strategy())));

  Upsert(record, record.job_content());
}

void RegionChangeRecorder::AddChangeRecord(const pb::raft::PrepareMergeRequest& request, int64_t source_id) {
  if (request.job_id() == 0) return;

  pb::store_internal::RegionChangeRecord record;
  record.set_region_id(source_id);
  record.set_begin_time(Helper::NowTime());
  record.set_job_type(pb::coordinator::CMD_MERGE);
  record.set_job_id(request.job_id());
  record.set_job_content(fmt::format("Merge region, {} -> {} target_region_epoch({}) ", source_id,
                                     request.target_region_id(),
                                     Helper::RegionEpochToString(request.target_region_epoch())));

  Upsert(record, record.job_content());
}

void RegionChangeRecorder::AddChangeRecord(const pb::raft::CommitMergeRequest& request, int64_t target_id) {
  if (request.job_id() == 0) return;

  pb::store_internal::RegionChangeRecord record;
  record.set_begin_time(Helper::NowTime());
  record.set_region_id(request.source_region_id());
  record.set_job_type(pb::coordinator::CMD_MERGE);
  record.set_job_id(request.job_id());
  record.set_job_content(
      fmt::format("Merge region, {} -> {} source_region_epoch({}) prepare_merge_log_id({}) entries_size({})",
                  request.source_region_id(), target_id, Helper::RegionEpochToString(request.source_region_epoch()),
                  request.prepare_merge_log_id(), request.entries_size()));

  Upsert(record, record.job_content());
}

void RegionChangeRecorder::AddChangeRecordTimePoint(int64_t job_id, const std::string& event) {
  if (job_id == 0) return;

  pb::store_internal::RegionChangeRecord record_for_save;
  {
    BAIDU_SCOPED_LOCK(mutex_);

    auto it = records_.find(job_id);
    if (it == records_.end()) {
      return;
    }

    auto* time_point = it->second.add_timeline();
    time_point->set_time(Helper::NowTime());
    time_point->set_event(event);
    record_for_save = it->second;
  }

  Save(record_for_save);
}

pb::store_internal::RegionChangeRecord RegionChangeRecorder::ChangeRecord(int64_t job_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = records_.find(job_id);
  if (it != records_.end()) {
    return it->second;
  }

  return {};
}

static bool CompareRecord(const pb::store_internal::RegionChangeRecord& record1,
                          const pb::store_internal::RegionChangeRecord& record2) {
  return record1.job_id() < record2.job_id();
}

std::vector<pb::store_internal::RegionChangeRecord> RegionChangeRecorder::GetChangeRecord(int64_t region_id) {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(GenKey(region_id), kvs)) {
    return {};
  }

  std::vector<pb::store_internal::RegionChangeRecord> records;
  records.reserve(kvs.size());
  for (const auto& kv : kvs) {
    pb::store_internal::RegionChangeRecord record;
    record.ParseFromString(kv.value());
    records.push_back(record);
  }

  std::sort(records.begin(), records.end(), CompareRecord);

  return records;
}

std::vector<pb::store_internal::RegionChangeRecord> RegionChangeRecorder::GetAllChangeRecord() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    return {};
  }

  std::vector<pb::store_internal::RegionChangeRecord> records;
  records.reserve(kvs.size());
  for (const auto& kv : kvs) {
    pb::store_internal::RegionChangeRecord record;
    record.ParseFromString(kv.value());
    records.push_back(record);
  }

  std::sort(records.begin(), records.end(), CompareRecord);

  return records;
}

bool RegionChangeRecorder::IsExist(int64_t job_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return records_.find(job_id) != records_.end();
}

void RegionChangeRecorder::Upsert(const pb::store_internal::RegionChangeRecord& record, const std::string& event) {
  pb::store_internal::RegionChangeRecord record_for_save;
  {
    BAIDU_SCOPED_LOCK(mutex_);

    auto it = records_.find(record.job_id());
    if (it == records_.end()) {
      records_.insert(std::make_pair(record.job_id(), record));
      record_for_save = record;
    } else {
      auto* time_point = it->second.add_timeline();
      time_point->set_time(Helper::NowTime());
      time_point->set_event(event);
      record_for_save = it->second;
    }
  }

  Save(record_for_save);
}

void RegionChangeRecorder::Save(const pb::store_internal::RegionChangeRecord& record) {
  if (record.job_id() > 0) {
    meta_writer_->Put({TransformToKv(record)});
  }
}

std::shared_ptr<pb::common::KeyValue> RegionChangeRecorder::TransformToKv(std::any obj) {
  auto& record = std::any_cast<pb::store_internal::RegionChangeRecord&>(obj);

  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(record.region_id(), record.job_id()));
  kv->set_value(record.SerializeAsString());

  return kv;
}

void RegionChangeRecorder::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);

  for (const auto& kv : kvs) {
    pb::store_internal::RegionChangeRecord record;
    record.ParseFromString(kv.value());

    records_.insert_or_assign(record.job_id(), record);
  }
}

bool StoreServerMeta::Init() {
  auto& server = Server::GetInstance();

  std::shared_ptr<pb::common::Store> store = std::make_shared<pb::common::Store>();
  store->set_id(server.Id());
  store->mutable_keyring()->assign(server.Keyring());
  store->set_epoch(0);
  store->set_state(pb::common::STORE_NORMAL);

  if (GetRole() == pb::common::ClusterRole::STORE) {
    store->set_store_type(::dingodb::pb::common::StoreType::NODE_TYPE_STORE);
  } else if (GetRole() == pb::common::ClusterRole::INDEX) {
    store->set_store_type(::dingodb::pb::common::StoreType::NODE_TYPE_INDEX);
  } else {
    DINGO_LOG(FATAL) << "[store.meta] unknown server role: " << GetRole();
  }

  auto* server_location = store->mutable_server_location();
  server_location->set_host(butil::ip2str(server.ServerEndpoint().ip).c_str());
  server_location->set_port(server.ServerEndpoint().port);
  auto* raf_location = store->mutable_raft_location();
  raf_location->set_host(butil::ip2str(server.RaftEndpoint().ip).c_str());
  raf_location->set_port(server.RaftEndpoint().port);

  DINGO_LOG(INFO) << "[store.meta] store server meta: " << store->ShortDebugString();
  AddStore(store);

  return true;
}

int64_t StoreServerMeta::GetEpoch() const { return epoch_; }

StoreServerMeta& StoreServerMeta::SetEpoch(int64_t epoch) {
  epoch_ = epoch;
  return *this;
}

bool StoreServerMeta::IsExist(int64_t store_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return stores_.find(store_id) != stores_.end();
}

void StoreServerMeta::AddStore(std::shared_ptr<pb::common::Store> store) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (stores_.find(store->id()) != stores_.end()) {
    DINGO_LOG(WARNING) << fmt::format("[store.meta][store_id({})] store already exist!", store->id());
    return;
  }

  stores_.insert(std::make_pair(store->id(), store));
}

void StoreServerMeta::UpdateStore(std::shared_ptr<pb::common::Store> store) {
  BAIDU_SCOPED_LOCK(mutex_);
  epoch_ += 1;
  stores_.insert_or_assign(store->id(), store);
}
void StoreServerMeta::DeleteStore(int64_t store_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  stores_.erase(store_id);
}

std::shared_ptr<pb::common::Store> StoreServerMeta::GetStore(int64_t store_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = stores_.find(store_id);
  if (it == stores_.end()) {
    DINGO_LOG(WARNING) << fmt::format("[store.meta][store_id({})] store not exist!", store_id);
    return nullptr;
  }

  return it->second;
}

std::map<int64_t, std::shared_ptr<pb::common::Store>> StoreServerMeta::GetAllStore() {
  BAIDU_SCOPED_LOCK(mutex_);
  return stores_;
}

pb::node::NodeInfo StoreServerMeta::GetNodeInfoByRaftEndPoint(const butil::EndPoint& endpoint) {
  BAIDU_SCOPED_LOCK(mutex_);

  pb::node::NodeInfo node_info;
  std::string host(butil::ip2str(endpoint.ip).c_str());
  for (const auto& [store_id, store] : stores_) {
    if (store->raft_location().host() == host && store->raft_location().port() == endpoint.port) {
      node_info.set_id(store->id());
      *node_info.mutable_server_location() = store->server_location();
      *node_info.mutable_raft_location() = store->raft_location();
      break;
    }
  }

  return node_info;
}

pb::node::NodeInfo StoreServerMeta::GetNodeInfoByServerEndPoint(const butil::EndPoint& endpoint) {
  BAIDU_SCOPED_LOCK(mutex_);

  pb::node::NodeInfo node_info;
  std::string host(butil::ip2str(endpoint.ip).c_str());
  for (const auto& [store_id, store] : stores_) {
    if (store->server_location().host() == host && store->server_location().port() == endpoint.port) {
      node_info.set_id(store->id());
      *node_info.mutable_server_location() = store->server_location();
      *node_info.mutable_raft_location() = store->raft_location();
      break;
    }
  }

  return node_info;
}

bool StoreRegionMeta::Init() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    DINGO_LOG(ERROR) << "[region.meta] Scan store region meta failed!";
    return false;
  }
  DINGO_LOG(INFO) << "[region.meta] Init store region meta num: " << kvs.size();

  if (!kvs.empty()) {
    TransformFromKv(kvs);
  }
  return true;
}

int64_t StoreRegionMeta::GetEpoch() { return 0; }

void StoreRegionMeta::AddRegion(store::RegionPtr region) {
  if (regions_.Get(region->Id()) != nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[region.meta][region({})] region already exist!", region->Id());
    return;
  }

  region->AppendHistoryState(pb::common::StoreRegionState::NEW);
  regions_.Put(region->Id(), region);

  if (meta_writer_ != nullptr) {
    meta_writer_->Put(TransformToKv(region));
  }
}

void StoreRegionMeta::DeleteRegion(int64_t region_id) {
  regions_.Erase(region_id);

  if (meta_writer_ != nullptr) {
    meta_writer_->Delete(GenKey(region_id));
  }
}

void StoreRegionMeta::UpdateRegion(store::RegionPtr region) {
  regions_.Put(region->Id(), region);

  if (meta_writer_ != nullptr) {
    meta_writer_->Put(TransformToKv(region));
  }
}

void StoreRegionMeta::UpdateState(store::RegionPtr region, pb::common::StoreRegionState new_state) {
  assert(region != nullptr);

  bool successed = false;
  auto cur_state = region->State();
  switch (cur_state) {
    case pb::common::StoreRegionState::NEW:
      if (new_state == pb::common::StoreRegionState::NORMAL || new_state == pb::common::StoreRegionState::STANDBY) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::NORMAL:
      if (new_state == pb::common::StoreRegionState::STANDBY || new_state == pb::common::StoreRegionState::SPLITTING ||
          new_state == pb::common::StoreRegionState::MERGING || new_state == pb::common::StoreRegionState::DELETING ||
          new_state == pb::common::StoreRegionState::ORPHAN || new_state == pb::common::StoreRegionState::TOMBSTONE) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::STANDBY:
      if (new_state == pb::common::StoreRegionState::NORMAL || new_state == pb::common::StoreRegionState::ORPHAN) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::SPLITTING:
      if (new_state == pb::common::StoreRegionState::NORMAL) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::MERGING:
      if (new_state == pb::common::StoreRegionState::NORMAL || new_state == pb::common::StoreRegionState::TOMBSTONE) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::DELETING:
      if (new_state == pb::common::StoreRegionState::DELETED) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::DELETED:
      break;
    case pb::common::StoreRegionState::TOMBSTONE:
      break;
    default:
      DINGO_LOG(ERROR) << fmt::format("[region.meta][region({})] unknown region state ", region->Id(),
                                      pb::common::StoreRegionState_Name(cur_state));
      break;
  }

  if (successed) {
    region->AppendHistoryState(new_state);
    if (meta_writer_ != nullptr) {
      meta_writer_->Put(TransformToKv(region));
    } else {
      DINGO_LOG(WARNING) << fmt::format(
          "[region.meta][region({})] update region state persistence failed, state {} to {}", region->Id(),
          pb::common::StoreRegionState_Name(cur_state), pb::common::StoreRegionState_Name(new_state));
    }
  }

  DINGO_LOG(DEBUG) << fmt::format("[region.meta][region({})] update region state {} to {} {}", region->Id(),
                                  pb::common::StoreRegionState_Name(cur_state),
                                  pb::common::StoreRegionState_Name(new_state), (successed ? "true" : "false"));
}

void StoreRegionMeta::UpdateState(int64_t region_id, pb::common::StoreRegionState new_state) {
  auto region = GetRegion(region_id);
  if (region != nullptr) {
    UpdateState(region, new_state);
  }
}

void StoreRegionMeta::UpdateLeaderId(store::RegionPtr region, int64_t leader_id) {
  assert(region != nullptr);

  region->SetLeaderId(leader_id);
}

void StoreRegionMeta::UpdateLeaderId(int64_t region_id, int64_t leader_id) {
  auto region = GetRegion(region_id);
  if (region != nullptr) {
    UpdateLeaderId(region, leader_id);
  }
}

void StoreRegionMeta::UpdatePeers(store::RegionPtr region, std::vector<pb::common::Peer>& peers) {
  assert(region != nullptr);
  region->SetPeers(peers);
  meta_writer_->Put(TransformToKv(region));
}

void StoreRegionMeta::UpdatePeers(int64_t region_id, std::vector<pb::common::Peer>& peers) {
  auto region = GetRegion(region_id);
  if (region != nullptr) {
    UpdatePeers(region, peers);
  }
}

void StoreRegionMeta::UpdateEpochVersionAndRange(store::RegionPtr region, int64_t version,
                                                 const pb::common::Range& range, const std::string& trace) {
  assert(region != nullptr);
  if (version <= region->Epoch().version()) {
    return;
  }

  DINGO_LOG(INFO) << fmt::format("[region.meta][region({})][trace({})] update epoch({}) and range({})", region->Id(),
                                 trace, version, Helper::RangeToString(range));

  region->SetEpochVersionAndRange(version, range);
  meta_writer_->Put(TransformToKv(region));
}

void StoreRegionMeta::UpdateSnapshotEpochVersion(store::RegionPtr region, int64_t version, const std::string& trace) {
  assert(region != nullptr);
  if (version <= region->SnapshotEpochVersion()) {
    return;
  }

  DINGO_LOG(INFO) << fmt::format("[region.meta][region({})][trace({})] update epoch({})", region->Id(), trace, version);

  region->SetSnapshotEpochVersion(version);
  meta_writer_->Put(TransformToKv(region));
}

void StoreRegionMeta::UpdateEpochVersionAndRange(int64_t region_id, int64_t version, const pb::common::Range& range,
                                                 const std::string& trace) {
  auto region = GetRegion(region_id);
  if (region != nullptr) {
    UpdateEpochVersionAndRange(region, version, range, trace);
  }
}

void StoreRegionMeta::UpdateEpochConfVersion(store::RegionPtr region, int64_t version) {
  assert(region != nullptr);
  if (version <= region->Epoch().conf_version()) {
    return;
  }

  region->SetEpochConfVersion(version);
  meta_writer_->Put(TransformToKv(region));
}

void StoreRegionMeta::UpdateEpochConfVersion(int64_t region_id, int64_t version) {
  auto region = GetRegion(region_id);
  if (region != nullptr) {
    UpdateEpochConfVersion(region, version);
  }
}

void StoreRegionMeta::UpdateNeedBootstrapDoSnapshot(store::RegionPtr region, bool need_do_snapshot) {
  assert(region != nullptr);

  region->SetNeedBootstrapDoSnapshot(need_do_snapshot);
  meta_writer_->Put(TransformToKv(region));
}

void StoreRegionMeta::UpdateDisableChange(store::RegionPtr region, bool disable_change) {
  assert(region != nullptr);

  region->SetDisableChange(disable_change);
  meta_writer_->Put(TransformToKv(region));
}

void StoreRegionMeta::UpdateTemporaryDisableChange(store::RegionPtr region, bool disable_change) {
  assert(region != nullptr);

  region->SetTemporaryDisableChange(disable_change);
  meta_writer_->Put(TransformToKv(region));
}

void StoreRegionMeta::UpdateLastChangeJobId(store::RegionPtr region, int64_t job_id) {
  assert(region != nullptr);

  region->SetLastChangeJobId(job_id);
  meta_writer_->Put(TransformToKv(region));
}

bool StoreRegionMeta::IsExistRegion(int64_t region_id) { return GetRegion(region_id) != nullptr; }

store::RegionPtr StoreRegionMeta::GetRegion(int64_t region_id) {
  auto region = regions_.Get(region_id);
  if (region == nullptr) {
    return nullptr;
  }

  return region;
}

std::vector<store::RegionPtr> StoreRegionMeta::GetAllRegion() {
  std::vector<store::RegionPtr> regions;
  regions.reserve(regions_.Size());

  if (regions_.GetAllValues(regions) < 0) {
    DINGO_LOG(ERROR) << "[region.meta] get all regions failed!";
    return regions;
  }

  return regions;
}

std::vector<store::RegionPtr> StoreRegionMeta::GetAllAliveRegion() {
  std::vector<store::RegionPtr> regions;
  regions.reserve(regions_.Size());

  if (regions_.GetAllValues(regions, [](store::RegionPtr region) -> bool {
        return region->State() != pb::common::StoreRegionState::DELETED;
      }) < 0) {
    DINGO_LOG(ERROR) << "[region.meta] get all regions failed!";
    return regions;
  }

  return regions;
}

std::vector<store::RegionPtr> StoreRegionMeta::GetAllMetricsRegion() {
  std::vector<store::RegionPtr> regions;
  regions.reserve(regions_.Size());

  if (regions_.GetAllValues(regions, [](store::RegionPtr region) -> bool {
        return region->State() != pb::common::StoreRegionState::NORMAL ||
               region->State() != pb::common::StoreRegionState::SPLITTING ||
               region->State() != pb::common::StoreRegionState::MERGING;
      }) < 0) {
    DINGO_LOG(ERROR) << "[region.meta] get all regions failed!";
    return regions;
  }

  return regions;
}

std::shared_ptr<pb::common::KeyValue> StoreRegionMeta::TransformToKv(std::any obj) {
  auto region = std::any_cast<store::RegionPtr>(obj);
  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(region->Id()));
  kv->set_value(region->Serialize());

  return kv;
}

void StoreRegionMeta::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  for (const auto& kv : kvs) {
    int64_t region_id = ParseRegionId(kv.key());
    auto region = store::Region::New(region_id);
    region->DeSerialize(kv.value());
    region->Recover();

    regions_.Put(region_id, region);
  }
}

bool StoreRaftMeta::Init() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    DINGO_LOG(ERROR) << "[raft.meta] scan store raft meta failed!";
    return false;
  }
  DINGO_LOG(INFO) << "[raft.meta] init store raft meta num: " << kvs.size();

  if (!kvs.empty()) {
    TransformFromKv(kvs);
  }
  return true;
}

void StoreRaftMeta::AddRaftMeta(store::RaftMetaPtr raft_meta) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    DINGO_LOG(INFO) << fmt::format("[raft.meta][region({})] Add raft meta.", raft_meta->RegionId());
    if (raft_metas_.find(raft_meta->RegionId()) != raft_metas_.end()) {
      DINGO_LOG(WARNING) << fmt::format("[raft.meta][region({})] raft meta already exist!", raft_meta->RegionId());
      return;
    }

    raft_metas_.insert(std::make_pair(raft_meta->RegionId(), raft_meta));
  }

  meta_writer_->Put(TransformToKv(raft_meta));
}

void StoreRaftMeta::UpdateRaftMeta(store::RaftMetaPtr raft_meta) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    raft_metas_.insert_or_assign(raft_meta->RegionId(), raft_meta);
  }

  meta_writer_->Put(TransformToKv(raft_meta));
}

void StoreRaftMeta::SaveRaftMeta(int64_t region_id) {
  auto raft_meta = GetRaftMeta(region_id);
  if (raft_meta != nullptr) {
    meta_writer_->Put(TransformToKv(raft_meta));
  }
}

void StoreRaftMeta::DeleteRaftMeta(int64_t region_id) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    raft_metas_.erase(region_id);
  }

  meta_writer_->Delete(GenKey(region_id));
}

store::RaftMetaPtr StoreRaftMeta::GetRaftMeta(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = raft_metas_.find(region_id);
  if (it == raft_metas_.end()) {
    DINGO_LOG(WARNING) << fmt::format("[raft.meta][region({})] raft meta not exist!", region_id);
    return nullptr;
  }

  return it->second;
}

std::vector<store::RaftMetaPtr> StoreRaftMeta::GetAllRaftMeta() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<store::RaftMetaPtr> raft_metas;
  for (auto& [_, raft_meta] : raft_metas_) {
    raft_metas.push_back(raft_meta);
  }

  return raft_metas;
}

std::shared_ptr<pb::common::KeyValue> StoreRaftMeta::TransformToKv(std::any obj) {
  auto raft_meta = std::any_cast<store::RaftMetaPtr>(obj);
  std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(raft_meta->RegionId()));
  kv->set_value(raft_meta->Serialize());

  return kv;
}

void StoreRaftMeta::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& kv : kvs) {
    int64_t region_id = ParseRegionId(kv.key());
    auto raft_meta = store::RaftMeta::New(region_id);
    raft_meta->DeSerialize(kv.value());
    raft_metas_.insert_or_assign(region_id, raft_meta);
  }
}

bool StoreMetaManager::Init() {
  if (!server_meta_->Init()) {
    DINGO_LOG(ERROR) << "Init store server meta failed!";
    return false;
  }

  if (!raft_meta_->Init()) {
    DINGO_LOG(ERROR) << "Init store raft meta failed!";
    return false;
  }

  if (!region_meta_->Init()) {
    DINGO_LOG(ERROR) << "Init store region meta failed!";
    return false;
  }

  if (!region_change_recorder_->Init()) {
    DINGO_LOG(ERROR) << "Init region change recorder failed!";
    return false;
  }

  return true;
}

std::shared_ptr<StoreServerMeta> StoreMetaManager::GetStoreServerMeta() {
  assert(server_meta_ != nullptr);
  return server_meta_;
}

std::shared_ptr<StoreRegionMeta> StoreMetaManager::GetStoreRegionMeta() {
  assert(region_meta_ != nullptr);
  return region_meta_;
}

std::shared_ptr<StoreRaftMeta> StoreMetaManager::GetStoreRaftMeta() {
  assert(raft_meta_ != nullptr);
  return raft_meta_;
}

std::shared_ptr<RegionChangeRecorder> StoreMetaManager::GetRegionChangeRecorder() {
  assert(region_change_recorder_ != nullptr);
  return region_change_recorder_;
}

std::shared_ptr<GCSafePoint> StoreMetaManager::GetGCSafePoint() {
  assert(gc_safe_point_ != nullptr);
  return gc_safe_point_;
}

}  // namespace dingodb
