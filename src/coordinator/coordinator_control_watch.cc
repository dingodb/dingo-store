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

#include "braft/closure_helper.h"
#include "braft/configuration.h"
#include "brpc/callback.h"
#include "brpc/closure_guard.h"
#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "gflags/gflags.h"
#include "metrics/coordinator_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "proto/version.pb.h"
#include "serial/buf.h"

namespace dingodb {

void WatchCancelCallback(CoordinatorControl* coordinator_control, google::protobuf::Closure* done) {
  DINGO_LOG(INFO) << "WatchCancelCallback, done:" << done;
  coordinator_control->RemoveOneTimeWatch(done);
}

butil::Status CoordinatorControl::OneTimeWatch(const std::string& watch_key, uint64_t start_revision, bool no_put_event,
                                               bool no_delete_event, bool need_prev_kv, bool wait_on_not_exist_key,
                                               google::protobuf::Closure* done, pb::version::WatchResponse* response,
                                               brpc::Controller* cntl) {
  brpc::ClosureGuard done_guard(done);

  BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

  // check if need to send back immediately
  std::vector<pb::version::Kv> kvs_temp;
  uint64_t total_count_in_range = 0;
  this->KvRange(watch_key, std::string(), 1, false, false, kvs_temp, total_count_in_range);

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
      event->mutable_kv()->CopyFrom(kvs_temp[0]);
      return butil::Status::OK();
    }
  }

  // add to watch
  AddOneTimeWatch(watch_key, start_revision, no_put_event, no_delete_event, need_prev_kv, done, response);

  // add NotifyOnCancel callback
  cntl->NotifyOnCancel(brpc::NewCallback(&WatchCancelCallback, this, done));

  return butil::Status::OK();
}

butil::Status CoordinatorControl::AddOneTimeWatch(const std::string& watch_key, uint64_t start_revision,
                                                  bool no_put_event, bool no_delete_event, bool need_prev_kv,
                                                  google::protobuf::Closure* done,
                                                  pb::version::WatchResponse* response) {
  // add to watch
  WatchNode watch_node(done, response, start_revision, no_put_event, no_delete_event, need_prev_kv);

  {
    BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);
    auto it = one_time_watch_map_.find(watch_key);
    if (it == one_time_watch_map_.end()) {
      std::map<google::protobuf::Closure*, WatchNode> watch_node_map;
      watch_node_map.insert_or_assign(done, watch_node);
      one_time_watch_map_.insert_or_assign(watch_key, watch_node_map);
    } else {
      auto& exist_watch_node_map = it->second;
      exist_watch_node_map.insert_or_assign(done, watch_node);
    }
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::RemoveOneTimeWatchWithLock(google::protobuf::Closure* done) {
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

// this function is called by WatchCancelCallback
butil::Status CoordinatorControl::RemoveOneTimeWatch(google::protobuf::Closure* done) {
  BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

  auto ret = RemoveOneTimeWatch(done);
  if (ret.ok()) {
    DINGO_LOG(INFO) << "RemoveOneTimeWatch success, do done->Run(), done:" << done;
    brpc::ClosureGuard done_guard(done);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::TriggerOneWatch(const std::string& key, pb::version::Event::EventType event_type,
                                                  pb::version::Kv& new_kv, pb::version::Kv& prev_kv) {
  BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

  auto it = one_time_watch_map_.find(key);
  if (it == one_time_watch_map_.end()) {
    DINGO_LOG(INFO) << "TriggerOneWatch not found, key:" << key;
    return butil::Status::OK();
  }

  auto& watch_node_map = it->second;
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
    kv->Swap(&new_kv);
    if (watch_node.second.need_prev_kv) {
      auto* old_kv = event->mutable_prev_kv();
      old_kv->Swap(&prev_kv);
    }

    RemoveOneTimeWatchWithLock(watch_node.first);

    brpc::ClosureGuard done_guard(watch_node.first);

    DINGO_LOG(INFO) << "TriggerOneWatch success, key:" << key << ", event_type:" << event_type
                    << ", watch_node.first:" << watch_node.first
                    << ", watch_detail: start_revision=" << watch_node.second.start_revision
                    << ", no_put_event=" << watch_node.second.no_put_event
                    << ", no_delete_event=" << watch_node.second.no_delete_event
                    << ", need_prev_kv=" << watch_node.second.need_prev_kv
                    << ", response:" << response->ShortDebugString();
  }
  return butil::Status::OK();
}

}  // namespace dingodb
