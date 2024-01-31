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

#include <bitset>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "brpc/closure_guard.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "server/service_helper.h"

namespace dingodb {

DEFINE_int64(meta_watch_outdate_time_ms, 300 * 1000, "meta watch outdate time in ms");
DEFINE_int64(meta_watch_max_event_list_count, 10000, "meta watch max event list count");

butil::Status CoordinatorControl::MetaWatchGetEventsForRevisions(const std::vector<int64_t> &event_revisions,
                                                                 pb::meta::WatchResponse &event_response) {
  // get event list and push repsonse to caller
  for (const auto event_revision : event_revisions) {
    std::shared_ptr<std::vector<pb::meta::MetaEvent>> event_list;
    auto ret = meta_event_map_.Get(event_revision, event_list);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "Get event list failed, event_revision: " << event_revision;
      event_response.set_canceled(true);
      event_response.set_cancel_reason("Get event list failed for watch progress, revision: " +
                                       std::to_string(event_revision));

      continue;
    } else if (event_list == nullptr) {
      DINGO_LOG(ERROR) << "Get event list failed, get nullptr, event_revision: " << event_revision;
      ServiceHelper::SetError(event_response.mutable_error(), pb::error::Errno::EINTERNAL,
                              "Get event list failed, get nullptr");
    }

    for (const auto &event : *event_list) {
      auto *meta_event = event_response.add_events();
      *meta_event = event;
    }

    DINGO_LOG(INFO) << "Get event list success single, event_revision: " << event_revision
                    << ", event_size: " << event_list->size();
  }

  DINGO_LOG(INFO) << "Get event list success full, revision_size: " << event_revisions.size()
                  << ", event_size: " << event_response.events_size();

  return butil::Status::OK();
}

butil::Status CoordinatorControl::MetaWatchSendEvents(int64_t watch_id, std::bitset<WATCH_BITSET_SIZE> watch_bitset,
                                                      const pb::meta::WatchResponse &event_response,
                                                      pb::meta::WatchResponse *response,
                                                      google::protobuf::Closure *done) {
  DINGO_LOG(INFO) << "MetaWatchSendEvents, watch_id: " << watch_id << ", event_size: " << event_response.events_size();

  brpc::ClosureGuard done_guard(done);

  for (const auto &event : event_response.events()) {
    if (watch_bitset.test(event.event_type())) {
      auto *meta_event = response->add_events();
      *meta_event = event;
    }
  }

  if (event_response.canceled()) {
    response->set_canceled(true);
    response->set_cancel_reason(event_response.cancel_reason());
  }

  if (event_response.error().errcode() != 0) {
    *response->mutable_error() = event_response.error();
  }

  response->set_watch_id(watch_id);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::MetaWatchProgress(const pb::meta::WatchRequest *request,
                                                    pb::meta::WatchResponse *response,
                                                    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  // find watch node
  auto watch_id = request->progress_request().watch_id();
  std::shared_ptr<MetaWatchNode> node;
  auto ret = watch_node_map_.Get(watch_id, node);
  if (ret < 0) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::EWATCH_NOT_EXIST, "Get watch node failed");
    return butil::Status(pb::error::Errno::EWATCH_NOT_EXIST, "Get watch node failed");
  } else if (node == nullptr) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::EINTERNAL,
                            "Get watch node failed, get nullptr");
    return butil::Status(pb::error::Errno::EINTERNAL, "Get watch node failed, get nullptr");
  }

  // swap pending event revisions
  std::vector<int64_t> event_revisions;
  std::vector<MetaWatchInstance> watch_instances;
  std::bitset<WATCH_BITSET_SIZE> watch_bitset;
  {
    BAIDU_SCOPED_LOCK(node->node_mutex);
    event_revisions.swap(node->pending_event_revisions);
    if (!event_revisions.empty()) {
      node->watched_revision = event_revisions.at(event_revisions.size() - 1);
      node->last_send_timestamp_ms = Helper::TimestampMs();
      watch_instances.swap(node->watch_instances);
    } else {
      // if no event, create a new watch_instance and push_back into watch_instance_list
      MetaWatchInstance watch_instance(done_guard.release(), request, response);
      node->watch_instances.push_back(watch_instance);
      DINGO_LOG(INFO) << "No event, create a new watch_instance and push_back into watch_instance_list, watch_id: "
                      << watch_id;
      return butil::Status::OK();
    }
  }

  // get event list and push repsonse to caller
  pb::meta::WatchResponse event_response;
  auto ret1 = MetaWatchGetEventsForRevisions(event_revisions, event_response);
  if (!ret1.ok()) {
    DINGO_LOG(ERROR) << "Get event list failed, watch_id: " << watch_id;
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::EINTERNAL,
                            "Get event list failed for watch progress");
    return ret1;
  }
  DINGO_LOG(INFO) << "Get event list success, watch_id: " << watch_id
                  << ", event_size: " << event_response.events_size();

  // send this response to caller
  auto ret2 = MetaWatchSendEvents(watch_id, node->watch_bitset, event_response, response, nullptr);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "Send event list failed, watch_id: " << watch_id;
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::EINTERNAL, "Send event list failed");
  }

  // send previous response to all watch_instances
  for (auto &watch_instance : watch_instances) {
    auto ret3 =
        MetaWatchSendEvents(watch_id, node->watch_bitset, event_response, watch_instance.response, watch_instance.done);
    if (!ret3.ok()) {
      DINGO_LOG(ERROR) << "Send event list failed, watch_id: " << watch_id;
    }
  }

  if (event_response.canceled()) {
    DINGO_LOG(INFO) << "Watch canceled, watch_id: " << watch_id;
    response->set_canceled(true);
    MetaWatchCancel(watch_id);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::MetaWatchCreate(const pb::meta::WatchRequest *request,
                                                  pb::meta::WatchResponse *response) {
  if (request->create_request().event_types_size() > WATCH_BITSET_SIZE) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::EILLEGAL_PARAMTETERS,
                            "event_types size is too large, the max is 64");
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "event_types size is too large, the max is 64");
  }

  if (request->create_request().start_revision() <= 0) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::EILLEGAL_PARAMTETERS,
                            "start_revision is less than 0 or equal to 0");
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "start_revision is less than 0 or equal to 0");
  }

  auto watch_id = request->create_request().watch_id();

  if (watch_id != 0) {
    std::shared_ptr<MetaWatchNode> node;
    auto ret = watch_node_map_.Get(watch_id, node);
    if (ret > 0) {
      return butil::Status(pb::error::Errno::EWATCH_EXIST, "watch_id is already exists, cannot create again");
    } else if (node != nullptr) {
      return butil::Status(pb::error::Errno::EINTERNAL, "Watch node already exists");
    }
  }

  pb::coordinator_internal::MetaIncrement meta_increment;
  watch_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_META_WATCH, meta_increment);

  if (watch_id <= 0) {
    return butil::Status(pb::error::Errno::EINTERNAL, "Generate watch_id failed");
  }

  std::shared_ptr<MetaWatchNode> node = std::make_shared<MetaWatchNode>();
  node->watch_id = watch_id;
  node->start_revision = request->create_request().start_revision();
  auto watch_bitset = GenWatchBitSet(request->create_request());
  node->watch_bitset = watch_bitset;

  watch_node_map_.Put(watch_id, node);
  {
    BAIDU_SCOPED_LOCK(meta_watch_bitmap_mutex_);
    meta_watch_bitmap_.insert_or_assign(watch_id, watch_bitset);
  }

  response->set_watch_id(watch_id);
  response->set_created(true);

  DINGO_LOG(INFO) << "Create meta_watch success, watch_id: " << watch_id;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::MetaWatchCancel(int64_t watch_id) {
  {
    BAIDU_SCOPED_LOCK(meta_watch_bitmap_mutex_);
    meta_watch_bitmap_.erase(watch_id);
  }

  std::shared_ptr<MetaWatchNode> node;
  auto ret = watch_node_map_.Get(watch_id, node);
  if (ret < 0) {
    return butil::Status(pb::error::Errno::EWATCH_NOT_EXIST, "Get watch node failed");
  } else if (node == nullptr) {
    return butil::Status(pb::error::Errno::EINTERNAL, "Get watch node failed, get nullptr");
  }
  watch_node_map_.Erase(watch_id);

  std::vector<MetaWatchInstance> watch_instances;
  {
    BAIDU_SCOPED_LOCK(node->node_mutex);
    watch_instances.swap(node->watch_instances);
    node->last_send_timestamp_ms = Helper::TimestampMs();
  }

  pb::meta::WatchResponse event_response;
  for (const auto &watch_instance : watch_instances) {
    event_response.set_canceled(true);
    event_response.set_cancel_reason("Watch canceled by coordinator");
    auto ret =
        MetaWatchSendEvents(watch_id, node->watch_bitset, event_response, watch_instance.response, watch_instance.done);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "Send event list failed, watch_id: " << watch_id;
    }
  }

  DINGO_LOG(INFO) << "Cancel meta_watch success, watch_id: " << watch_id;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::ListWatch(int64_t watch_id, pb::meta::ListWatchResponse *response) {
  if (watch_id == 0) {
    std::map<int64_t, std::shared_ptr<MetaWatchNode>> temp_watch_node_map;
    watch_node_map_.GetRawMapCopy(temp_watch_node_map);

    if (temp_watch_node_map.empty()) {
      return butil::Status::OK();
    }

    for (const auto &[watch_id, node] : temp_watch_node_map) {
      auto *watch_node = response->add_watch_nodes();

      std::vector<int64_t> event_revisions;
      {
        BAIDU_SCOPED_LOCK(node->node_mutex);
        watch_node->set_watch_id(watch_id);
        watch_node->set_is_watching(node->watch_instances.size());
        watch_node->set_start_revision(node->start_revision);
        watch_node->set_watched_revision(node->watched_revision);
        for (const auto &event_type : node->event_types) {
          watch_node->add_event_types(event_type);
        }
        watch_node->set_last_send_timestamp_ms(node->last_send_timestamp_ms);
        event_revisions = node->pending_event_revisions;
      }

      for (const auto event_revision : event_revisions) {
        std::shared_ptr<std::vector<pb::meta::MetaEvent>> event_list;
        auto ret = meta_event_map_.Get(event_revision, event_list);
        if (ret < 0) {
          DINGO_LOG(WARNING) << "Get event list failed, event_revision: " << event_revision;
          continue;
        } else if (event_list == nullptr) {
          DINGO_LOG(ERROR) << "Get event list failed, get nullptr, event_revision: " << event_revision;
          continue;
        }

        for (const auto &event : *event_list) {
          auto *meta_event = watch_node->add_pending_events();
          *meta_event = event;
        }
      }
    }
  } else {
    std::shared_ptr<MetaWatchNode> node;
    auto ret = watch_node_map_.Get(watch_id, node);
    if (ret < 0) {
      return butil::Status(pb::error::Errno::EWATCH_NOT_EXIST, "Get watch node failed");
    } else if (node == nullptr) {
      return butil::Status(pb::error::Errno::EINTERNAL, "Get watch node failed, get nullptr");
    }

    auto *watch_node = response->add_watch_nodes();

    std::vector<int64_t> event_revisions;
    {
      BAIDU_SCOPED_LOCK(node->node_mutex);
      watch_node->set_watch_id(watch_id);
      watch_node->set_is_watching(node->watch_instances.size());
      watch_node->set_start_revision(node->start_revision);
      watch_node->set_watched_revision(node->watched_revision);
      for (const auto &event_type : node->event_types) {
        watch_node->add_event_types(event_type);
      }
      watch_node->set_last_send_timestamp_ms(node->last_send_timestamp_ms);
      event_revisions = node->pending_event_revisions;
    }

    for (const auto event_revision : event_revisions) {
      std::shared_ptr<std::vector<pb::meta::MetaEvent>> event_list;
      auto ret = meta_event_map_.Get(event_revision, event_list);
      if (ret < 0) {
        DINGO_LOG(WARNING) << "Get event list failed, event_revision: " << event_revision;
        continue;
      } else if (event_list == nullptr) {
        DINGO_LOG(ERROR) << "Get event list failed, get nullptr, event_revision: " << event_revision;
        continue;
      }

      for (const auto &event : *event_list) {
        auto *meta_event = watch_node->add_pending_events();
        *meta_event = event;
      }
    }
  }

  return butil::Status::OK();
}

std::bitset<WATCH_BITSET_SIZE> CoordinatorControl::GenWatchBitSet(const pb::meta::WatchCreateRequest &create_request) {
  CHECK(create_request.event_types_size() <= WATCH_BITSET_SIZE);

  std::bitset<WATCH_BITSET_SIZE> watch_bitset;

  for (const auto &event_type : create_request.event_types()) {
    watch_bitset.set(event_type);
  }

  return watch_bitset;
}

std::bitset<WATCH_BITSET_SIZE> CoordinatorControl::GenWatchBitSet(
    const std::set<pb::meta::MetaEventType> &event_types) {
  CHECK(event_types.size() <= WATCH_BITSET_SIZE);

  std::bitset<WATCH_BITSET_SIZE> watch_bitset;

  for (const auto &event_type : event_types) {
    watch_bitset.set(event_type);
  }

  return watch_bitset;
}

void CoordinatorControl::AddEventList(int64_t meta_revision,
                                      std::shared_ptr<std::vector<pb::meta::MetaEvent>> event_list,
                                      std::bitset<WATCH_BITSET_SIZE> watch_bitset) {
  // add event list to meta_event_map_
  meta_event_map_.Put(meta_revision, event_list);

  // check watch_bitset and push event list to pending_event_revisions
  {
    BAIDU_SCOPED_LOCK(meta_watch_bitmap_mutex_);
    for (const auto &[watch_id, bit_set] : meta_watch_bitmap_) {
      auto bit_set_result = bit_set & watch_bitset;
      if (!bit_set_result.none()) {
        std::shared_ptr<MetaWatchNode> node;
        auto ret = watch_node_map_.Get(watch_id, node);
        if (ret < 0) {
          DINGO_LOG(WARNING) << "Get watch node failed, watch_id: " << watch_id;
          continue;
        } else if (node == nullptr) {
          DINGO_LOG(ERROR) << "Get watch node failed, get nullptr, watch_id: " << watch_id;
          continue;
        }

        std::vector<int64_t> event_revisions;
        std::vector<MetaWatchInstance> watch_instances;
        {
          BAIDU_SCOPED_LOCK(node->node_mutex);
          node->watched_revision = meta_revision;

          if (node->watch_instances.empty()) {
            if (node->start_revision < meta_revision) {
              node->pending_event_revisions.push_back(meta_revision);
            }
          } else {
            watch_instances.swap(node->watch_instances);
            node->last_send_timestamp_ms = Helper::TimestampMs();
            event_revisions.swap(node->pending_event_revisions);
            event_revisions.push_back(meta_revision);
          }
        }

        if (!watch_instances.empty()) {
          pb::meta::WatchResponse event_response;
          auto ret = MetaWatchGetEventsForRevisions(event_revisions, event_response);
          if (!ret.ok()) {
            DINGO_LOG(ERROR) << "Get event list failed, watch_id: " << watch_id;
            continue;
          }

          for (auto &watch_instance : watch_instances) {
            auto ret = MetaWatchSendEvents(watch_id, node->watch_bitset, event_response, watch_instance.response,
                                           watch_instance.done);
            if (!ret.ok()) {
              DINGO_LOG(ERROR) << "Send event list failed, watch_id: " << watch_id;
            }
          }
        }
      }
    }
  }
}

void CoordinatorControl::RecycleOutdatedMetaWatcher() {
  std::map<int64_t, std::bitset<WATCH_BITSET_SIZE>> temp_meta_watch_bitmap;
  {
    BAIDU_SCOPED_LOCK(meta_watch_bitmap_mutex_);
    temp_meta_watch_bitmap = meta_watch_bitmap_;
  }

  DINGO_LOG(INFO) << "RecycleOutdatedMetaWatcher, meta_watch_bitmap_size: " << temp_meta_watch_bitmap.size();

  std::vector<int64_t> watch_id_cancel_list;

  {
    for (const auto &[watch_id, bit_set] : temp_meta_watch_bitmap) {
      std::shared_ptr<MetaWatchNode> node;
      auto ret = watch_node_map_.Get(watch_id, node);
      if (ret < 0) {
        DINGO_LOG(WARNING) << "Get watch node failed, watch_id: " << watch_id;
        continue;
      } else if (node == nullptr) {
        DINGO_LOG(ERROR) << "Get watch node failed, get nullptr, watch_id: " << watch_id;
        continue;
      }

      std::vector<int64_t> event_revisions;
      std::vector<MetaWatchInstance> watch_instances;
      {
        BAIDU_SCOPED_LOCK(node->node_mutex);
        if (node->watch_instances.empty()) {
          if (node->last_send_timestamp_ms + FLAGS_meta_watch_outdate_time_ms < Helper::TimestampMs()) {
            DINGO_LOG(INFO) << "RecycleOutdatedMetaWatcher do remove outdated watch_id, watch_id: " << watch_id
                            << ", last_send_timestamp_ms: " << node->last_send_timestamp_ms
                            << ", outdate_time_ms: " << FLAGS_meta_watch_outdate_time_ms
                            << ", now: " << Helper::TimestampMs() << ", push_back into watch_id_cancel_list";
            watch_id_cancel_list.push_back(watch_id);
          }
        } else {
          watch_instances.swap(node->watch_instances);
          event_revisions.swap(node->pending_event_revisions);
        }
      }

      if (!watch_instances.empty()) {
        pb::meta::WatchResponse event_response;
        auto ret = MetaWatchGetEventsForRevisions(event_revisions, event_response);
        if (!ret.ok()) {
          DINGO_LOG(ERROR) << "Get event list failed, watch_id: " << watch_id;
          continue;
        }

        DINGO_LOG(INFO) << "RecycleOutdatedMetaWatcher do idle send, watch_id: " << watch_id
                        << ", revision_size: " << event_revisions.size()
                        << ", event_size: " << event_response.events_size()
                        << ", watch_instance_size: " << watch_instances.size()
                        << ", last_send_timestamp_ms: " << node->last_send_timestamp_ms
                        << ", send event list to watch_instances";

        for (auto &watch_instance : watch_instances) {
          auto ret = MetaWatchSendEvents(watch_id, node->watch_bitset, event_response, watch_instance.response,
                                         watch_instance.done);
          if (!ret.ok()) {
            DINGO_LOG(ERROR) << "Send event list failed, watch_id: " << watch_id;
          }
        }
      } else {
        DINGO_LOG(INFO) << "RecycleOutdatedMetaWatcher no watch_instance and not outdated, watch_id: " << watch_id
                        << ", revision_size: " << event_revisions.size()
                        << ", last_send_timestamp_ms: " << node->last_send_timestamp_ms
                        << ", outdate_time_ms: " << FLAGS_meta_watch_outdate_time_ms
                        << ", now: " << Helper::TimestampMs();
      }
    }
  }
}

void CoordinatorControl::TrimMetaWatchEventList() {
  auto now_count = meta_event_map_.Size();
  DINGO_LOG(INFO) << "TrimMetaWatchEventList trim start, meta_event_map_size: " << now_count
                  << ", max: " << FLAGS_meta_watch_max_event_list_count;

  if (now_count > FLAGS_meta_watch_max_event_list_count) {
    auto trim_count = now_count - FLAGS_meta_watch_max_event_list_count;

    DINGO_LOG(INFO) << "TrimMetaWatchEventList, meta_event_map_size: " << now_count
                    << ", max: " << FLAGS_meta_watch_max_event_list_count << ", do trim, trim_count: " << trim_count;

    std::set<int64_t> ids_to_trim;
    meta_event_map_.GetAllKeys(ids_to_trim);

    if (ids_to_trim.size() < FLAGS_meta_watch_max_event_list_count) {
      DINGO_LOG(INFO) << "TrimMetaWatchEventList, meta_event_map_size: " << now_count
                      << ", max: " << FLAGS_meta_watch_max_event_list_count
                      << ", ids_to_trim_size: " << ids_to_trim.size() << ", less than max, do not trim";
      return;
    }

    for (const auto &id : ids_to_trim) {
      if (trim_count <= 0) {
        break;
      }

      auto ret = meta_event_map_.Erase(id);
      if (ret < 0) {
        DINGO_LOG(WARNING) << "TrimMetaWatchEventList, erase failed, id: " << id;
      } else {
        DINGO_LOG(INFO) << "TrimMetaWatchEventList, erase success, id: " << id;
        trim_count--;
      }
    }

    DINGO_LOG(INFO) << "TrimMetaWatchEventList, trim success, meta_event_map_size: " << meta_event_map_.Size()
                    << ", max: " << FLAGS_meta_watch_max_event_list_count;
  }
}

}  // namespace dingodb