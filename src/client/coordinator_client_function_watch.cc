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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "client/coordinator_client_function.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "gflags/gflags_declare.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/version.pb.h"

DECLARE_string(key);
DECLARE_int64(revision);
DECLARE_bool(need_prev_kv);
DECLARE_bool(no_put);
DECLARE_bool(no_delete);
DECLARE_bool(wait_on_not_exist_key);
DECLARE_int32(max_watch_count);
DECLARE_string(lock_name);
DECLARE_string(client_uuid);

void SendOneTimeWatch(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::WatchRequest request;
  dingodb::pb::version::WatchResponse response;

  if (FLAGS_key.empty()) {
    DINGO_LOG(WARNING) << "key is empty, please input key";
    return;
  }

  if (FLAGS_revision == 0) {
    DINGO_LOG(WARNING) << "revision is 0, watch from now";
  }

  auto* one_time_watch_req = request.mutable_one_time_request();

  one_time_watch_req->set_key(FLAGS_key);
  one_time_watch_req->set_need_prev_kv(FLAGS_need_prev_kv);
  one_time_watch_req->set_wait_on_not_exist_key(FLAGS_wait_on_not_exist_key);
  one_time_watch_req->set_start_revision(FLAGS_revision);

  if (FLAGS_no_delete) {
    one_time_watch_req->add_filters(::dingodb::pb::version::EventFilterType::NODELETE);
  }

  if (FLAGS_no_put) {
    one_time_watch_req->add_filters(::dingodb::pb::version::EventFilterType::NOPUT);
  }

  DINGO_LOG(INFO) << "wait_on_not_exist_key=" << FLAGS_wait_on_not_exist_key << ", no_put=" << FLAGS_no_put
                  << ", no_delete=" << FLAGS_no_delete << ", need_prev_kv=" << FLAGS_need_prev_kv
                  << ", max_watch_count=" << FLAGS_max_watch_count << ", revision=" << FLAGS_revision
                  << ", key=" << FLAGS_key;

  for (uint32_t i = 0; i < FLAGS_max_watch_count; i++) {
    // wait 600s for event
    DINGO_LOG(INFO) << "SendRequest watch_count=" << i;
    auto status = coordinator_interaction->SendRequest("Watch", request, response, 600000);
    DINGO_LOG(INFO) << "SendRequest status=" << status << ", watch_count=" << i;
    DINGO_LOG(INFO) << response.DebugString();
  }
}

butil::Status CoorKvPut(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction,
                        const std::string& key, const ::std::string& value, int64_t lease, int64_t& revision) {
  dingodb::pb::version::PutRequest request;
  dingodb::pb::version::PutResponse response;

  auto* key_value = request.mutable_key_value();
  key_value->set_key(key);
  key_value->set_value(value);

  request.set_lease(lease);

  auto status = coordinator_interaction->SendRequest("KvPut", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.error().errcode() == dingodb::pb::error::OK) {
    revision = response.header().revision();
    DINGO_LOG(INFO) << "put revision=" << revision;
    return butil::Status::OK();
  } else {
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }
}

butil::Status CoorKvRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction,
                          const std::string& key, const std::string& range_end, int64_t limit,
                          std::vector<dingodb::pb::version::Kv>& kvs) {
  dingodb::pb::version::RangeRequest request;
  dingodb::pb::version::RangeResponse response;

  request.set_key(key);
  request.set_range_end(range_end);
  request.set_limit(limit);
  // request.set_keys_only(FLAGS_keys_only);
  // request.set_count_only(FLAGS_count_only);

  auto status = coordinator_interaction->SendRequest("KvRange", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.error().errcode() == dingodb::pb::error::OK) {
    for (int i = 0; i < response.kvs_size(); i++) {
      kvs.push_back(response.kvs(i));
    }

    return butil::Status::OK();
  } else {
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }
}

butil::Status CoorWatch(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction,
                        const std::string& key, int64_t start_revision, bool need_prev_kv, bool no_put, bool no_delete,
                        bool wait_on_not_exist_key, std::vector<dingodb::pb::version::Event>& events) {
  dingodb::pb::version::WatchRequest request;
  dingodb::pb::version::WatchResponse response;

  auto* one_time_watch_req = request.mutable_one_time_request();

  one_time_watch_req->set_key(key);
  one_time_watch_req->set_need_prev_kv(need_prev_kv);
  one_time_watch_req->set_wait_on_not_exist_key(wait_on_not_exist_key);
  one_time_watch_req->set_start_revision(start_revision);

  if (no_delete) {
    one_time_watch_req->add_filters(::dingodb::pb::version::EventFilterType::NODELETE);
  }

  if (no_put) {
    one_time_watch_req->add_filters(::dingodb::pb::version::EventFilterType::NOPUT);
  }

  DINGO_LOG(INFO) << "wait_on_not_exist_key=" << wait_on_not_exist_key << ", no_put=" << no_put
                  << ", no_delete=" << no_delete << ", need_prev_kv=" << need_prev_kv
                  << ", max_watch_count=" << FLAGS_max_watch_count << ", revision=" << start_revision
                  << ", key=" << key;

  // wait 600s for event
  DINGO_LOG(INFO) << "SendRequest watch";
  auto status = coordinator_interaction->SendRequest("Watch", request, response, 600000);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.error().errcode() == dingodb::pb::error::OK) {
    for (const auto& event : response.events()) {
      events.push_back(event);
    }
    return butil::Status::OK();
  } else {
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }
}

butil::Status CoorLeaseGrant(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction,
                             int64_t& lease_id, int64_t& ttl) {
  dingodb::pb::version::LeaseGrantRequest request;
  dingodb::pb::version::LeaseGrantResponse response;

  request.set_id(lease_id);
  request.set_ttl(ttl);

  auto status = coordinator_interaction->SendRequest("LeaseGrant", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.error().errcode() == dingodb::pb::error::OK) {
    lease_id = response.id();
    ttl = response.ttl();
    return butil::Status::OK();
  } else {
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }
}

butil::Status CoorLeaseRenew(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction,
                             int64_t lease_id) {
  dingodb::pb::version::LeaseRenewRequest request;
  dingodb::pb::version::LeaseRenewResponse response;

  request.set_id(lease_id);

  auto status = coordinator_interaction->SendRequest("LeaseRenew", request, response);
  // DINGO_LOG(INFO) << "SendRequest status=" << status;
  // DINGO_LOG(INFO) << response.DebugString();

  if (response.error().errcode() == dingodb::pb::error::OK) {
    return butil::Status::OK();
  } else {
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }
}

void PeriodRenwLease(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, int64_t lease_id) {
  for (;;) {
    auto ret = CoorLeaseRenew(coordinator_interaction, lease_id);
    bthread_usleep(3 * 900L * 1000L);
  }
}

void GetWatchKeyAndRevision(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction,
                            const std::string& lock_prefix, const std::string& lock_key, std::string& watch_key,
                            int64_t& watch_revision) {
  DINGO_LOG(INFO) << "lock_prefix=" << lock_prefix << ", lock_key=" << lock_key;
  // check if lock success
  std::vector<dingodb::pb::version::Kv> kvs;
  auto ret = CoorKvRange(coordinator_interaction, lock_prefix, lock_prefix + "\xFF", 0, kvs);
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << "CoorKvRange failed, ret=" << ret;
    return;
  }

  if (kvs.empty()) {
    DINGO_LOG(WARNING) << "CoorKvRange failed, kvs is empty";
    return;
  }

  struct KeyWithCreateRevision {
    std::string key;
    int64_t mod_revision;
  };

  std::vector<KeyWithCreateRevision> key_with_create_revisions;
  key_with_create_revisions.reserve(kvs.size());

  for (const auto& kv : kvs) {
    DINGO_LOG(INFO) << "lock_key=" << kv.kv().key() << ", lock_value=" << kv.kv().value()
                    << ", lock_revision=" << kv.mod_revision();
    KeyWithCreateRevision key_with_create_revision;
    key_with_create_revision.key = kv.kv().key();
    key_with_create_revision.mod_revision = kv.mod_revision();
    key_with_create_revisions.push_back(key_with_create_revision);
  }

  std::sort(
      key_with_create_revisions.begin(), key_with_create_revisions.end(),
      [](const KeyWithCreateRevision& a, const KeyWithCreateRevision& b) { return a.mod_revision < b.mod_revision; });

  DINGO_LOG(INFO) << "key_with_create_revisions.size()=" << key_with_create_revisions.size();

  int self_index = 0;
  int watch_index = 0;
  for (int i = 0; i < key_with_create_revisions.size(); i++) {
    DINGO_LOG(INFO) << "key=" << key_with_create_revisions[i].key
                    << ", mod_revision=" << key_with_create_revisions[i].mod_revision;
    if (key_with_create_revisions[i].key == lock_key) {
      self_index = i;
      DINGO_LOG(INFO) << "self_index=" << self_index;
    }
  }
  if (self_index > 0) {
    watch_index = self_index - 1;
  }

  int64_t min_revision = key_with_create_revisions[0].mod_revision;
  watch_revision = key_with_create_revisions[watch_index].mod_revision;
  watch_key = key_with_create_revisions[watch_index].key;

  DINGO_LOG(INFO) << "min_revision=" << min_revision << ", lock_key=" << key_with_create_revisions[0].key
                  << ", lock_value=1";

  if (key_with_create_revisions[0].key == lock_key) {
    DINGO_LOG(INFO) << "Lock success";
    watch_key = lock_key;
  }
}

void SendLock(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  if (FLAGS_lock_name.empty()) {
    DINGO_LOG(WARNING) << "lock_name is empty, please input lock_name";
    return;
  }

  if (FLAGS_client_uuid.empty()) {
    DINGO_LOG(WARNING) << "client_uuid is empty, please input client_uuid";
    return;
  }

  std::string lock_prefix = FLAGS_lock_name + "_lock_";
  std::string lock_key = lock_prefix + FLAGS_client_uuid;

  DINGO_LOG(INFO) << "lock_key=" << lock_key;

  // create lease
  int64_t lease_id = 0;
  int64_t ttl = 3;
  auto ret = CoorLeaseGrant(coordinator_interaction, lease_id, ttl);
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << "CoorLeaseGrant failed, ret=" << ret;
    return;
  }

  // renew lease in background
  Bthread bt(nullptr, PeriodRenwLease, coordinator_interaction, lease_id);

  // write lock key
  int64_t revision = 0;
  ret = CoorKvPut(coordinator_interaction, lock_key, "1", lease_id, revision);
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << "CoorKvPut failed, ret=" << ret;
    return;
  }

  // watch lock key
  do {
    // check if lock success
    std::string watch_key;
    int64_t watch_revision = 0;
    GetWatchKeyAndRevision(coordinator_interaction, lock_prefix, lock_key, watch_key, watch_revision);

    if (watch_key == lock_key) {
      DINGO_LOG(INFO) << "Get Lock success";
      bthread_usleep(3600 * 1000L * 1000L);
    }

    DINGO_LOG(WARNING) << "Lock failed, watch for key=" << watch_key << ", watch_revision=" << watch_revision;
    std::vector<dingodb::pb::version::Event> events;
    ret = CoorWatch(coordinator_interaction, watch_key, watch_revision, true, false, false, false, events);
    if (!ret.ok()) {
      DINGO_LOG(WARNING) << "CoorWatch failed, ret=" << ret;
      return;
    }

    for (const auto& event : events) {
      DINGO_LOG(INFO) << "event_type=" << event.type() << ", event_kv.key=" << event.kv().kv().key()
                      << ", event_kv.value=" << event.kv().kv().value()
                      << ", event_kv.create_revision=" << event.kv().create_revision()
                      << ", event_kv.mod_revision=" << event.kv().mod_revision()
                      << ", event_kv.version=" << event.kv().version() << ", event_kv.lease=" << event.kv().lease()
                      << ", event_prev_kv.key=" << event.prev_kv().kv().key()
                      << ", event_prev_kv.value=" << event.prev_kv().kv().value()
                      << ", event_prev_kv.create_revision=" << event.prev_kv().create_revision()
                      << ", event_prev_kv.mod_revision=" << event.prev_kv().mod_revision()
                      << ", event_prev_kv.version=" << event.prev_kv().version()
                      << ", event_prev_kv.lease=" << event.prev_kv().lease();
    }

    if (events.empty()) {
      continue;
    }

    DINGO_LOG(INFO) << "watch get event=" << events[0].DebugString();

    // if (events[0].type() == dingodb::pb::version::Event::PUT) {
    //   if (events[0].kv().mod_revision() > watch_revision) {
    //     DINGO_LOG(INFO) << "Lock success";
    //     bthread_usleep(600 * 1000L * 1000L);
    //     break;
    //   } else {
    //     DINGO_LOG(INFO) << "Continue to watch, event=" << events[0].DebugString();
    //     continue;
    //   }
    // }
    // else if (events[0].type() == dingodb::pb::version::Event::DELETE) {
    //   DINGO_LOG(INFO) << "Lock success";
    //   bthread_usleep(600 * 1000L * 1000L);
    //   break;
    // } else if (events[0].type() == dingodb::pb::version::Event::NOT_EXISTS) {
    //   DINGO_LOG(INFO) << "Lock success";
    //   bthread_usleep(600 * 1000L * 1000L);
    //   break;
    // }

  } while (true);
}
