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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "brpc/closure_guard.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "common/logging.h"
#include "coordinator/kv_control.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/version.pb.h"

namespace dingodb {

void WatchCancelCallback(KvControl* kv_control, google::protobuf::Closure* done) {
  DINGO_LOG(INFO) << "WatchCancelCallback, done:" << done;

  kv_control->CancelOneTimeWatchClosure(done);
}

butil::Status KvControl::OneTimeWatch(const std::string& watch_key, int64_t start_revision, bool no_put_event,
                                      bool no_delete_event, bool need_prev_kv, bool wait_on_not_exist_key,
                                      google::protobuf::Closure* done, pb::version::WatchResponse* response,
                                      [[maybe_unused]] brpc::Controller* cntl) {
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(INFO) << "OneTimeWatch, watch_key:" << watch_key << ", start_revision:" << start_revision
                  << ", no_put_event:" << no_put_event << ", no_delete_event:" << no_delete_event
                  << ", need_prev_kv:" << need_prev_kv << ", wait_on_not_exist_key:" << wait_on_not_exist_key
                  << ", done:" << done;

  BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

  if (start_revision == 0) {
    start_revision = GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION);
    DINGO_LOG(INFO) << "OneTimeWatch, start_revision is 0, set to next revision, watch_key:" << watch_key
                    << ", start_revision:" << start_revision;
  }

  // check if need to send back immediately
  std::vector<pb::version::Kv> kvs_temp;
  int64_t total_count_in_range = 0;
  KvRange(watch_key, std::string(), 1, false, false, kvs_temp, total_count_in_range);

  // if key is not exists, and no wait, send response
  if (kvs_temp.empty() && !wait_on_not_exist_key) {
    // not exist, no wait, send response
    auto* event = response->add_events();
    event->set_type(::dingodb::pb::version::Event_EventType::Event_EventType_NOT_EXISTS);
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
  auto* defer_done = done_guard.release();
  AddOneTimeWatch(watch_key, start_revision, no_put_event, no_delete_event, need_prev_kv, defer_done, response);

  // add NotifyOnCancel callback
  cntl->NotifyOnCancel(brpc::NewCallback(&WatchCancelCallback, this, defer_done));

  return butil::Status::OK();
}

// caller must hold one_time_watch_map_mutex_
butil::Status KvControl::AddOneTimeWatch(const std::string& watch_key, int64_t start_revision, bool no_put_event,
                                         bool no_delete_event, bool need_prev_kv, google::protobuf::Closure* done,
                                         pb::version::WatchResponse* response) {
  // add to watch
  KvWatchNode watch_node(done, response, start_revision, no_put_event, no_delete_event, need_prev_kv);

  DINGO_LOG(INFO) << "AddOneTimeWatch, watch_key:" << watch_key << ", start_revision:" << start_revision
                  << ", no_put_event:" << no_put_event << ", no_delete_event:" << no_delete_event
                  << ", need_prev_kv:" << need_prev_kv << ", done:" << done;

  auto it = one_time_watch_map_.find(watch_key);
  if (it == one_time_watch_map_.end()) {
    std::map<google::protobuf::Closure*, KvWatchNode> watch_node_map;
    watch_node_map.insert_or_assign(done, watch_node);
    one_time_watch_map_.insert_or_assign(watch_key, watch_node_map);
    DINGO_LOG(INFO) << "AddOneTimeWatch, watch_key not found, insert, watch_key:" << watch_key;
  } else {
    auto& exist_watch_node_map = it->second;
    exist_watch_node_map.insert_or_assign(done, watch_node);
    DINGO_LOG(INFO) << "AddOneTimeWatch, watch_key found, insert, watch_key:" << watch_key;
  }

  one_time_watch_closure_map_.insert_or_assign(done, watch_key);

  return butil::Status::OK();
}

butil::Status KvControl::RemoveOneTimeWatchWithLock(google::protobuf::Closure* done) {
  auto it = one_time_watch_closure_map_.find(done);
  if (it == one_time_watch_closure_map_.end()) {
    DINGO_LOG(INFO) << "RemoveOneTimeWatch not found, done:" << done;
    return butil::Status(EINVAL, "RemoveOneTimeWatch not found");
  }

  auto& watch_key = it->second;
  if (watch_key.empty()) {
    DINGO_LOG(ERROR) << "RemoveOneTimeWatch watch_key is empty, done:" << done
                     << ", remove from one_time_watch_closure_map_";
    one_time_watch_closure_map_.erase(it);
    return butil::Status(EINVAL, "RemoveOneTimeWatch watch_key is empty");
  }

  auto it2 = one_time_watch_map_.find(watch_key);
  if (it2 == one_time_watch_map_.end()) {
    DINGO_LOG(ERROR) << "RemoveOneTimeWatch watch_key not found, done:" << done << ", watch_key:" << watch_key
                     << ", remove from one_time_watch_closure_map_";
    one_time_watch_closure_map_.erase(it);
    return butil::Status(EINVAL, "RemoveOneTimeWatch watch_key not found");
  }

  auto& watch_node_map = it2->second;
  auto it3 = watch_node_map.find(done);
  if (it3 == watch_node_map.end()) {
    DINGO_LOG(ERROR) << "RemoveOneTimeWatch done not found, done:" << done << ", watch_key:" << watch_key
                     << ", remove from one_time_watch_closure_map_";
    one_time_watch_closure_map_.erase(it);
    return butil::Status(EINVAL, "RemoveOneTimeWatch done not found");
  }

  DINGO_LOG(INFO) << "RemoveOneTimeWatch, done:" << done << ", watch_key:" << watch_key;
  watch_node_map.erase(it3);

  return butil::Status::OK();
}

// this function is called by crontab
butil::Status KvControl::RemoveOneTimeWatch() {
  DINGO_LOG(INFO) << "RemoveOneTimeWatch";

  BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

  std::map<google::protobuf::Closure*, bool> done_map;

  one_time_watch_closure_status_map_.GetRawMapCopy(done_map);

  if (done_map.empty()) {
    DINGO_LOG(INFO) << "RemoveOneTimeWatch, done_map is empty";
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << "RemoveOneTimeWatch, done_map.size:" << done_map.size();

  for (auto& it : done_map) {
    auto* done = it.first;
    DINGO_LOG(INFO) << "RemoveOneTimeWatch, done:" << done;

    auto ret = RemoveOneTimeWatchWithLock(done);
    if (ret.ok()) {
      DINGO_LOG(INFO) << "RemoveOneTimeWatchWithLock success, do done->Run(), done:" << done;
      done->Run();
    }

    one_time_watch_closure_status_map_.Erase(done);
  }

  DINGO_LOG(INFO) << "RemoveOneTimeWatch finish, done_map.size:" << done_map.size();

  return butil::Status::OK();
}

// this function is called by WatchCancelCallback
butil::Status KvControl::CancelOneTimeWatchClosure(google::protobuf::Closure* done) {
  DINGO_LOG(INFO) << "CancelOneTimeWatchClosure, done:" << done;
  one_time_watch_closure_status_map_.Put(done, true);
  return butil::Status::OK();
}

butil::Status KvControl::TriggerOneWatch(const std::string& key, pb::version::Event::EventType event_type,
                                         pb::version::Kv& new_kv, pb::version::Kv& prev_kv) {
  DINGO_LOG(INFO) << "TriggerOneWatch, key:" << key << ", event_type:" << event_type
                  << ", new_kv:" << new_kv.ShortDebugString() << ", prev_kv:" << prev_kv.ShortDebugString();

  BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

  auto it = one_time_watch_map_.find(key);
  if (it == one_time_watch_map_.end()) {
    DINGO_LOG(INFO) << "TriggerOneWatch not found, key:" << key;
    return butil::Status::OK();
  }

  auto& watch_node_map = it->second;

  std::vector<google::protobuf::Closure*> done_list;
  for (auto& watch_node : watch_node_map) {
    if (watch_node.second.no_put_event && event_type == pb::version::Event::EventType::Event_EventType_PUT) {
      DINGO_LOG(INFO) << "TriggerOneWatch skip, no_put_event, key:" << key;
      continue;
    }

    if (watch_node.second.no_delete_event && event_type == pb::version::Event::EventType::Event_EventType_DELETE) {
      DINGO_LOG(INFO) << "TriggerOneWatch skip, no_delete_event, key:" << key;
      continue;
    }

    if (watch_node.second.start_revision > new_kv.mod_revision()) {
      DINGO_LOG(INFO) << "TriggerOneWatch skip, start_revision > new_kv.mod_revision(), key:" << key
                      << ", start_revision:" << watch_node.second.start_revision
                      << ", new_kv.mod_revision:" << new_kv.mod_revision();
      continue;
    }

    auto* response = watch_node.second.response;
    auto* event = response->add_events();
    event->set_type(event_type);
    auto* kv = event->mutable_kv();
    *kv = new_kv;
    if (watch_node.second.need_prev_kv) {
      auto* old_kv = event->mutable_prev_kv();
      *old_kv = prev_kv;
    }

    DINGO_LOG(INFO) << "TriggerOneWatch will RemoveOneTimeWatch, key:" << key << ", event_type:" << event_type
                    << ", watch_node.first:" << watch_node.first
                    << ", watch_detail: start_revision=" << watch_node.second.start_revision
                    << ", no_put_event=" << watch_node.second.no_put_event
                    << ", no_delete_event=" << watch_node.second.no_delete_event
                    << ", need_prev_kv=" << watch_node.second.need_prev_kv
                    << ", response:" << response->ShortDebugString();

    auto* done = watch_node.first;
    done_list.push_back(done);

    DINGO_LOG(INFO) << "TriggerOneWatch success, key:" << key << ", event_type:" << event_type << ", done:" << done
                    << ", response:" << response->ShortDebugString();
  }

  for (auto& done : done_list) {
    done->Run();
    RemoveOneTimeWatchWithLock(done);
  }

  return butil::Status::OK();
}

}  // namespace dingodb
