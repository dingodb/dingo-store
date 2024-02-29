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

#include <sys/types.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "brpc/closure_guard.h"
#include "bthread/mutex.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/logging.h"
#include "coordinator/kv_control.h"
#include "gflags/gflags.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/version.pb.h"

namespace dingodb {

DEFINE_int64(version_watch_max_count, 50000, "max count of version watch");

DEFINE_bool(dingo_log_switch_coor_watch, false, "switch for dingo log of kv control lease");

void WatchCancelCallback(KvControl* kv_control, uint64_t closure_id) {
  kv_control->CancelOneTimeWatchClosure(closure_id);
  // kv_control->RemoveOneTimeWatch(closure_id);
}

butil::Status KvControl::OneTimeWatch(const std::string& watch_key, int64_t start_revision, bool no_put_event,
                                      bool no_delete_event, bool need_prev_kv, bool wait_on_not_exist_key,
                                      google::protobuf::Closure* done, pb::version::WatchResponse* response,
                                      [[maybe_unused]] brpc::Controller* cntl) {
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(INFO) << "OneTimeWatch, watch_key:" << watch_key << ", hex_key: " << Helper::StringToHex(watch_key)
                  << ", start_revision:" << start_revision << ", no_put_event:" << no_put_event
                  << ", no_delete_event:" << no_delete_event << ", need_prev_kv:" << need_prev_kv
                  << ", wait_on_not_exist_key:" << wait_on_not_exist_key << ", done:" << done;

  BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

  if (one_time_watch_map_.size() > FLAGS_version_watch_max_count) {
    DINGO_LOG(ERROR) << "OneTimeWatch, one_time_watch_map_.size() > FLAGS_version_watch_max_count, watch_key:"
                     << watch_key << ", start_revision:" << start_revision << ", no_put_event:" << no_put_event
                     << ", no_delete_event:" << no_delete_event << ", need_prev_kv:" << need_prev_kv
                     << ", wait_on_not_exist_key:" << wait_on_not_exist_key << ", done:" << done
                     << ", one_time_watch_map_.size:" << one_time_watch_map_.size();
    return butil::Status(pb::error::Errno::EWATCH_COUNT_EXCEEDS_LIMIT,
                         "OneTimeWatch, one_time_watch_map_.size() > FLAGS_version_watch_max_count");
  }

  if (start_revision == 0) {
    start_revision = GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION);
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch)
        << "OneTimeWatch, start_revision is 0, set to next revision, watch_key:" << watch_key
        << ", hex_key:" << Helper::StringToHex(watch_key) << ", start_revision:" << start_revision;
  }

  // check if need to send back immediately
  std::vector<pb::version::Kv> kvs_temp;
  int64_t total_count_in_range = 0;
  bool has_more = false;
  KvRange(watch_key, std::string(), 1, false, false, kvs_temp, total_count_in_range, has_more);

  // if key is not exists, and no wait, send response
  if (kvs_temp.empty() && !wait_on_not_exist_key) {
    // not exist, no wait, send response
    auto* event = response->add_events();
    event->set_type(::dingodb::pb::version::Event_EventType::Event_EventType_NOT_EXISTS);
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch)
        << "OneTimeWatch, key not exists, no wait, send response, watch_key:" << watch_key
        << ", hex_key:" << Helper::StringToHex(watch_key) << ", start_revision:" << start_revision;
    return butil::Status::OK();
  }

  if (!kvs_temp.empty()) {
    // compare start_revision and current revision
    if (kvs_temp[0].mod_revision() > start_revision) {
      auto* event = response->add_events();
      event->set_type(::dingodb::pb::version::Event_EventType::Event_EventType_PUT);
      *(event->mutable_kv()) = kvs_temp[0];
      return butil::Status::OK();
    }
  }

  // add to watch
  auto closure_id = one_time_watch_closure_seq_.fetch_add(1, std::memory_order_relaxed);
  DeferDone defer_done(closure_id, watch_key, done_guard.release(), response);

  // add defer_done to done_map
  one_time_watch_closure_status_map_.Put(closure_id, false);
  one_time_watch_closure_map_.insert_or_assign(closure_id, defer_done);

  AddOneTimeWatch(watch_key, start_revision, no_put_event, no_delete_event, need_prev_kv, closure_id);

  // add NotifyOnCancel callback
  cntl->NotifyOnCancel(brpc::NewCallback(&WatchCancelCallback, this, closure_id));

  return butil::Status::OK();
}

// caller must hold one_time_watch_map_mutex_
butil::Status KvControl::AddOneTimeWatch(const std::string& watch_key, int64_t start_revision, bool no_put_event,
                                         bool no_delete_event, bool need_prev_kv, uint64_t closure_id) {
  // add to watch
  KvWatchNode watch_node(closure_id, start_revision, no_put_event, no_delete_event, need_prev_kv);

  DINGO_LOG(INFO) << "AddOneTimeWatch, watch_key:" << watch_key << ", hex_key: " << Helper::StringToHex(watch_key)
                  << ", start_revision:" << start_revision << ", no_put_event:" << no_put_event
                  << ", no_delete_event:" << no_delete_event << ", need_prev_kv:" << need_prev_kv
                  << ", closure_id:" << closure_id;

  auto it = one_time_watch_map_.find(watch_key);
  if (it == one_time_watch_map_.end()) {
    std::map<uint64_t, KvWatchNode> watch_node_map;
    watch_node_map.insert_or_assign(closure_id, watch_node);
    one_time_watch_map_.insert_or_assign(watch_key, watch_node_map);
    DINGO_LOG(INFO) << "AddOneTimeWatch, watch_key not found, insert, watch_key:" << watch_key
                    << ", hex_key: " << Helper::StringToHex(watch_key)
                    << ", watch_node_map.size:" << watch_node_map.size();
  } else {
    auto& exist_watch_node_map = it->second;
    exist_watch_node_map.insert_or_assign(closure_id, watch_node);
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch)
        << "AddOneTimeWatch, watch_key found, insert, watch_key:" << watch_key
        << ", hex_key: " << Helper::StringToHex(watch_key) << ", watch_node_map.size:" << exist_watch_node_map.size();
  }

  return butil::Status::OK();
}

butil::Status KvControl::RemoveOneTimeWatchWithLock(uint64_t closure_id) {
  one_time_watch_closure_status_map_.Erase(closure_id);

  auto it_defer_done = one_time_watch_closure_map_.find(closure_id);
  if (it_defer_done == one_time_watch_closure_map_.end()) {
    DINGO_LOG(INFO) << "RemoveOneTimeWatch not found, closure_id:" << closure_id;
    return butil::Status(EINVAL, "RemoveOneTimeWatch not found");
  }
  auto& defer_done = it_defer_done->second;

  DINGO_LOG(INFO) << "RemoveOneTimeWatch, closure_id:" << closure_id << ", done->Run() start";
  int64_t start_ts = butil::gettimeofday_ms();
  defer_done.Done();
  DINGO_LOG(INFO) << "RemoveOneTimeWatch, closure_id:" << closure_id
                  << ", done->Run() finish, cost: " << butil::gettimeofday_ms() - start_ts << " ms";

  auto watch_key = defer_done.GetWatchKey();
  if (watch_key.empty()) {
    DINGO_LOG(ERROR) << "RemoveOneTimeWatch watch_key is empty, closure_id:" << closure_id
                     << ", remove from one_time_watch_closure_map_";
    return butil::Status(EINVAL, "RemoveOneTimeWatch watch_key is empty");
  }

  auto it_watch_key = one_time_watch_map_.find(watch_key);
  if (it_watch_key == one_time_watch_map_.end()) {
    DINGO_LOG(ERROR) << "RemoveOneTimeWatch watch_key not found, closure_id:" << closure_id
                     << ", watch_key:" << watch_key << ", remove from one_time_watch_closure_map_";
    return butil::Status(EINVAL, "RemoveOneTimeWatch watch_key not found");
  }

  auto& watch_node_map = it_watch_key->second;
  auto it_watch_node = watch_node_map.find(closure_id);
  if (it_watch_node == watch_node_map.end()) {
    DINGO_LOG(ERROR) << "RemoveOneTimeWatch done not found, closure_id:" << closure_id << ", watch_key:" << watch_key
                     << ", remove from one_time_watch_closure_map_";
    return butil::Status(EINVAL, "RemoveOneTimeWatch done not found");
  }

  watch_node_map.erase(it_watch_node);
  DINGO_LOG(INFO) << "RemoveOneTimeWatch, done->Run: " << closure_id << ", watch_key:" << watch_key
                  << ", cost: " << butil::gettimeofday_ms() - start_ts << " ms";

  if (watch_node_map.empty()) {
    DINGO_LOG(INFO) << "RemoveOneTimeWatch, watch_node_map is empty, watch_key:" << watch_key;
    one_time_watch_map_.erase(it_watch_key);
  }

  return butil::Status::OK();
}

// this function is called by WatchCancelCallback
butil::Status KvControl::CancelOneTimeWatchClosure(uint64_t closure_id) {
  DINGO_LOG(INFO) << "CancelOneTimeWatchClosure, closure_id:" << closure_id;
  one_time_watch_closure_status_map_.Put(closure_id, true);
  return butil::Status::OK();
}

// this function is called by crontab
butil::Status KvControl::RemoveOneTimeWatch() {
  DINGO_LOG(INFO) << "CRONTAB RemoveOneTimeWatch for canceled";

  std::map<uint64_t, bool> closure_status_map;

  one_time_watch_closure_status_map_.GetRawMapCopy(closure_status_map);

  if (closure_status_map.empty()) {
    DINGO_LOG(DEBUG) << "CRONTAB RemoveOneTimeWatch, done_map is empty";
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << "CRONTAB RemoveOneTimeWatch, closure_status_map.size:" << closure_status_map.size();

  BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

  int64_t start_ts = butil::gettimeofday_ms();
  int64_t count = 0;
  for (auto& it : closure_status_map) {
    auto closure_id = it.first;

    if (!it.second) {
      continue;
    }

    DINGO_LOG(INFO) << "CRONTAB RemoveOneTimeWatch, closure_id:" << closure_id;

    auto ret = RemoveOneTimeWatchWithLock(closure_id);
    if (ret.ok()) {
      DINGO_LOG(INFO) << "CRONTAB RemoveOneTimeWatchWithLock success, closure_id:" << closure_id;
    }
    count++;
  }

  DINGO_LOG(INFO) << "CRONTAB RemoveOneTimeWatch success, clean count: " << closure_status_map.size()
                  << ", cost: " << butil::gettimeofday_ms() - start_ts << " ms";

  return butil::Status::OK();
}

butil::Status KvControl::RemoveOneTimeWatch(uint64_t closure_id) {
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch) << "RemoveOneTimeWatch closure_id:" << closure_id;

  int64_t start_ts = butil::gettimeofday_ms();
  auto ret1 = bthread_mutex_trylock(&one_time_watch_map_mutex_);
  if (ret1 != 0) {
    DINGO_LOG(WARNING) << "RemoveOneTimeWatch, bthread_mutex_trylock failed, closure_id:" << closure_id
                       << ", ret1:" << ret1 << ", cost: " << butil::gettimeofday_ms() - start_ts << " ms";
    return butil::Status(EINVAL, "RemoveOneTimeWatch, bthread_mutex_trylock failed");
  }

  auto ret = RemoveOneTimeWatchWithLock(closure_id);
  if (ret.ok()) {
    DINGO_LOG(INFO) << "RemoveOneTimeWatchWithLock success, closure_id:" << closure_id;
  } else {
    DINGO_LOG(ERROR) << "RemoveOneTimeWatchWithLock failed, closure_id:" << closure_id
                     << ", errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
  }

  bthread_mutex_unlock(&one_time_watch_map_mutex_);

  return butil::Status::OK();
}

butil::Status KvControl::TriggerOneWatch(const std::string& key, pb::version::Event::EventType event_type,
                                         pb::version::Kv& new_kv, pb::version::Kv& prev_kv) {
  DINGO_LOG(INFO) << "TriggerOneWatch, key:" << key << ", event_type:" << event_type
                  << ", new_kv:" << new_kv.ShortDebugString() << ", prev_kv:" << prev_kv.ShortDebugString();

  BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

  auto it = one_time_watch_map_.find(key);
  if (it == one_time_watch_map_.end()) {
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch) << "TriggerOneWatch not found, key:" << key;
    return butil::Status::OK();
  }

  auto& watch_node_map = it->second;

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch)
      << "TriggerOneWatch, key:" << key << ", watch_node_map.size:" << watch_node_map.size();

  std::vector<uint64_t> done_list;
  for (auto& watch_node : watch_node_map) {
    if (watch_node.second.no_put_event && event_type == pb::version::Event::EventType::Event_EventType_PUT) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch) << "TriggerOneWatch skip, no_put_event, key:" << key;
      continue;
    }

    if (watch_node.second.no_delete_event && event_type == pb::version::Event::EventType::Event_EventType_DELETE) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch) << "TriggerOneWatch skip, no_delete_event, key:" << key;
      continue;
    }

    if (watch_node.second.start_revision > new_kv.mod_revision()) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch)
          << "TriggerOneWatch skip, start_revision > new_kv.mod_revision(), key:" << key
          << ", start_revision:" << watch_node.second.start_revision
          << ", new_kv.mod_revision:" << new_kv.mod_revision();
      continue;
    }

    DINGO_LOG(INFO) << "TriggerOneWatch will GetResponse, key:" << key << ", event_type:" << event_type
                    << ", watch_node.first:" << watch_node.first
                    << ", watch_detail: start_revision=" << watch_node.second.start_revision
                    << ", no_put_event=" << watch_node.second.no_put_event
                    << ", no_delete_event=" << watch_node.second.no_delete_event
                    << ", need_prev_kv=" << watch_node.second.need_prev_kv;

    pb::version::WatchResponse* response = nullptr;
    auto closure_id = watch_node.first;
    {
      // BAIDU_SCOPED_LOCK(one_time_watch_closure_map_mutex_);
      const auto& it_closure = one_time_watch_closure_map_.find(closure_id);
      if (it_closure == one_time_watch_closure_map_.end()) {
        DINGO_LOG(FATAL) << "TriggerOneWatch, one_time_watch_closure_map_ not found, key:" << key
                         << ", closure_id:" << closure_id;
      }

      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch) << "TriggerOneWatch will GetResponse ptr, key:" << key;

      response = it_closure->second.GetResponse();
      if (response == nullptr) {
        DINGO_LOG(ERROR) << "TriggerOneWatch, response is nullptr, key:" << key << ", closure_id:" << closure_id;
      } else {
        auto* event = response->add_events();
        event->set_type(event_type);
        auto* kv = event->mutable_kv();
        *kv = new_kv;
        if (watch_node.second.need_prev_kv) {
          auto* old_kv = event->mutable_prev_kv();
          *old_kv = prev_kv;
        }
      }
    }

    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch)
        << "TriggerOneWatch will RemoveOneTimeWatch, key:" << key << ", event_type:" << event_type
        << ", watch_node.first:" << watch_node.first
        << ", watch_detail: start_revision=" << watch_node.second.start_revision
        << ", no_put_event=" << watch_node.second.no_put_event
        << ", no_delete_event=" << watch_node.second.no_delete_event
        << ", need_prev_kv=" << watch_node.second.need_prev_kv;

    done_list.push_back(closure_id);

    DINGO_LOG(INFO) << "TriggerOneWatch success, key:" << key << ", event_type:" << event_type
                    << ", closure_id:" << closure_id;
  }

  for (auto& closure_id : done_list) {
    auto ret = RemoveOneTimeWatchWithLock(closure_id);
    if (ret.ok()) {
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_coor_watch)
          << "RemoveOneTimeWatchWithLock success, do done->Run(), closure_id:" << closure_id;
    } else {
      DINGO_LOG(ERROR) << "RemoveOneTimeWatchWithLock failed, do done->Run(), closure_id:" << closure_id
                       << ", errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb
