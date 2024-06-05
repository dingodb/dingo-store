
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

#include <iostream>

#include "client_v2/client_helper.h"
#include "client_v2/subcommand_helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "proto/version.pb.h"
#include "subcommand_coordinator.h"
namespace client_v2 {

butil::Status CoorKvRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version,
                          const std::string& key, const std::string& range_end, int64_t limit,
                          std::vector<dingodb::pb::version::Kv>& kvs) {
  dingodb::pb::version::RangeRequest request;
  dingodb::pb::version::RangeResponse response;

  request.set_key(key);
  request.set_range_end(range_end);
  request.set_limit(limit);
  // request.set_keys_only(FLAGS_keys_only);
  // request.set_count_only(FLAGS_count_only);

  auto status = coordinator_interaction_version->SendRequest("KvRange", request, response);
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

void GetWatchKeyAndRevision(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version,
                            const std::string& lock_prefix, const std::string& lock_key, std::string& watch_key,
                            int64_t& watch_revision) {
  DINGO_LOG(INFO) << "lock_prefix=" << lock_prefix << ", lock_key=" << lock_key;
  // check if lock success
  std::vector<dingodb::pb::version::Kv> kvs;
  auto ret = CoorKvRange(coordinator_interaction_version, lock_prefix, lock_prefix + "\xFF", 0, kvs);
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

butil::Status CoorKvPut(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version,
                        const std::string& key, const ::std::string& value, int64_t lease, int64_t& revision) {
  dingodb::pb::version::PutRequest request;
  dingodb::pb::version::PutResponse response;

  auto* key_value = request.mutable_key_value();
  key_value->set_key(key);
  key_value->set_value(value);

  request.set_lease(lease);

  auto status = coordinator_interaction_version->SendRequest("KvPut", request, response);
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

butil::Status CoorLeaseGrant(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version,
                             int64_t& lease_id, int64_t& ttl) {
  dingodb::pb::version::LeaseGrantRequest request;
  dingodb::pb::version::LeaseGrantResponse response;

  request.set_id(lease_id);
  request.set_ttl(ttl);

  auto status = coordinator_interaction_version->SendRequest("LeaseGrant", request, response);
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

butil::Status CoorLeaseRenew(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version,
                             int64_t lease_id) {
  dingodb::pb::version::LeaseRenewRequest request;
  dingodb::pb::version::LeaseRenewResponse response;

  request.set_id(lease_id);

  auto status = coordinator_interaction_version->SendRequest("LeaseRenew", request, response);
  // DINGO_LOG(INFO) << "SendRequest status=" << status;
  // DINGO_LOG(INFO) << response.DebugString();

  if (response.error().errcode() == dingodb::pb::error::OK) {
    return butil::Status::OK();
  } else {
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }
}
void PeriodRenwLease(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version,
                     int64_t lease_id) {
  for (;;) {
    auto ret = CoorLeaseRenew(coordinator_interaction_version, lease_id);
    bthread_usleep(3 * 900L * 1000L);
  }
}
butil::Status CoorWatch(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version,
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
                  << ", no_delete=" << no_delete << ", need_prev_kv=" << need_prev_kv << ", revision=" << start_revision
                  << ", key=" << key;

  // wait 600s for event
  DINGO_LOG(INFO) << "SendRequest watch";
  auto status = coordinator_interaction_version->SendRequest("Watch", request, response, 600000);
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

void SetUpSubcommandKvHello(CLI::App& app) {
  auto opt = std::make_shared<KvHelloOptions>();
  auto coor = app.add_subcommand("KvHello", "Kv hello")->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandKvHello(*opt); });
}

void RunSubcommandKvHello(KvHelloOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::HelloRequest request;
  dingodb::pb::version::HelloResponse response;

  std::string const key = "Hello";
  // const char* op = nullptr;
  request.set_hello(0);
  request.set_get_memory_info(true);

  auto status = coordinator_interaction_version->SendRequest("Hello", request, response);
  DINGO_LOG(INFO) << "SendRequest status: " << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandGetRawKvIndex(CLI::App& app) {
  auto opt = std::make_shared<GetRawKvIndexOptions>();
  auto coor = app.add_subcommand("GetRawKvIndex", "Get raw kv index ")->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandGetRawKvIndex(*opt); });
}

void RunSubcommandGetRawKvIndex(GetRawKvIndexOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::GetRawKvIndexRequest request;
  dingodb::pb::version::GetRawKvIndexResponse response;

  request.set_key(opt.key);

  auto status = coordinator_interaction_version->SendRequest("GetRawKvIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandGetRawKvRev(CLI::App& app) {
  auto opt = std::make_shared<GetRawKvRevOptions>();
  auto coor = app.add_subcommand("GetRawKvRev", "Get raw kv rev ")->group("Coordinator Manager Commands");
  coor->add_option("--rversion", opt->revision, "Request parameter rversion")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--sub_rversion", opt->sub_revision, "Request parameter sub_rversion")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandGetRawKvRev(*opt); });
}

void RunSubcommandGetRawKvRev(GetRawKvRevOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::GetRawKvRevRequest request;
  dingodb::pb::version::GetRawKvRevResponse response;

  request.mutable_revision()->set_main(opt.revision);
  request.mutable_revision()->set_sub(opt.sub_revision);

  auto status = coordinator_interaction_version->SendRequest("GetRawKvRev", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandCoorKvRange(CLI::App& app) {
  auto opt = std::make_shared<CoorKvRangeOptions>();
  auto coor = app.add_subcommand("CoorKvRange", "Coor kv range ")->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--range_end", opt->range_end, "Request parameter range_end")
      ->default_val("")
      ->group("Coordinator Manager Commands");
  coor->add_option("--sub_rversion", opt->limit, "Request parameter sub_rversion")
      ->default_val(50)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--keys_only", opt->keys_only, "Request parameter keys_only")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--count_only", opt->count_only, "Request parameter keys_count_only")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandCoorKvRange(*opt); });
}

void RunSubcommandCoorKvRange(CoorKvRangeOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::RangeRequest request;
  dingodb::pb::version::RangeResponse response;

  request.set_key(opt.key);
  request.set_range_end(opt.range_end);
  request.set_limit(opt.limit);
  request.set_keys_only(opt.keys_only);
  request.set_count_only(opt.count_only);

  auto status = coordinator_interaction_version->SendRequest("KvRange", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandCoorKvPut(CLI::App& app) {
  auto opt = std::make_shared<CoorKvPutOptions>();
  auto coor = app.add_subcommand("CoorKvPut", "Coor kv put ")->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--value", opt->value, "Request parameter value")->group("Coordinator Manager Commands");
  coor->add_option("--lease", opt->lease, "Request parameter lease")
      ->default_val(0)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--ignore_lease", opt->ignore_lease, "Request parameter ignore_lease")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--ignore_value", opt->ignore_value, "Request parameter ignore_value")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--need_prev_kv", opt->need_prev_kv, "Request parameter need_prev_kv")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandCoorKvPut(*opt); });
}

void RunSubcommandCoorKvPut(CoorKvPutOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::PutRequest request;
  dingodb::pb::version::PutResponse response;

  auto* key_value = request.mutable_key_value();
  key_value->set_key(opt.key);
  key_value->set_value(opt.value);

  request.set_lease(opt.lease);
  request.set_ignore_lease(opt.ignore_lease);
  request.set_ignore_value(opt.ignore_value);
  request.set_need_prev_kv(opt.need_prev_kv);

  auto status = coordinator_interaction_version->SendRequest("KvPut", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandCoorKvDeleteRange(CLI::App& app) {
  auto opt = std::make_shared<CoorKvDeleteRangeOptions>();
  auto coor = app.add_subcommand("CoorKvDeleteRange", "Coor kv delete range ")->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--range_end", opt->range_end, "Request parameter range_end")
      ->default_val("")
      ->group("Coordinator Manager Commands");
  coor->add_flag("--need_prev_kv", opt->need_prev_kv, "Request parameter need_prev_kv")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandCoorKvDeleteRange(*opt); });
}

void RunSubcommandCoorKvDeleteRange(CoorKvDeleteRangeOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::DeleteRangeRequest request;
  dingodb::pb::version::DeleteRangeResponse response;

  request.set_key(opt.key);
  request.set_range_end(opt.range_end);
  request.set_need_prev_kv(opt.need_prev_kv);

  auto status = coordinator_interaction_version->SendRequest("KvDeleteRange", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandCoorKvCompaction(CLI::App& app) {
  auto opt = std::make_shared<CoorKvCompactionOptions>();
  auto coor = app.add_subcommand("CoorKvDeleteRange", "Coor kv delete range ")->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--range_end", opt->range_end, "Request parameter range_end")
      ->default_val("")
      ->group("Coordinator Manager Commands");
  coor->add_option("--revision", opt->revision, "Request parameter revision")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandCoorKvCompaction(*opt); });
}

void RunSubcommandCoorKvCompaction(CoorKvCompactionOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::CompactionRequest request;
  dingodb::pb::version::CompactionResponse response;

  request.set_key(opt.key);
  request.set_range_end(opt.range_end);
  request.set_compact_revision(opt.revision);

  auto status = coordinator_interaction_version->SendRequest("KvCompaction", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandOneTimeWatch(CLI::App& app) {
  auto opt = std::make_shared<OneTimeWatchOptions>();
  auto coor = app.add_subcommand("OneTimeWatch", "One time watch ")->group("Coordinator Manager Commands");
  coor->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  coor->add_option("--revision", opt->revision, "Request parameter revision")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_flag("--need_prev_kv", opt->need_prev_kv, "Request parameter need_prev_kv")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--wait_on_not_exist_key", opt->wait_on_not_exist_key, "Request parameter wait_on_not_exist_key")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--no_put", opt->no_put, "Request parameter no_put")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_flag("--no_delete", opt->no_delete, "Request parameter no_delete")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  coor->add_option("--max_watch_count", opt->max_watch_count, "Request parameter max_watch_count")
      ->default_val(10)
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandOneTimeWatch(*opt); });
}

void RunSubcommandOneTimeWatch(OneTimeWatchOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::WatchRequest request;
  dingodb::pb::version::WatchResponse response;

  auto* one_time_watch_req = request.mutable_one_time_request();

  one_time_watch_req->set_key(opt.key);
  one_time_watch_req->set_need_prev_kv(opt.need_prev_kv);
  one_time_watch_req->set_wait_on_not_exist_key(opt.wait_on_not_exist_key);
  one_time_watch_req->set_start_revision(opt.revision);

  if (opt.no_delete) {
    one_time_watch_req->add_filters(::dingodb::pb::version::EventFilterType::NODELETE);
  }

  if (opt.no_put) {
    one_time_watch_req->add_filters(::dingodb::pb::version::EventFilterType::NOPUT);
  }

  DINGO_LOG(INFO) << "wait_on_not_exist_key=" << opt.wait_on_not_exist_key << ", no_put=" << opt.no_put
                  << ", no_delete=" << opt.no_delete << ", need_prev_kv=" << opt.need_prev_kv
                  << ", max_watch_count=" << opt.max_watch_count << ", revision=" << opt.revision
                  << ", key=" << opt.key;

  for (uint32_t i = 0; i < opt.max_watch_count; i++) {
    // wait 600s for event
    DINGO_LOG(INFO) << "SendRequest watch_count=" << i;
    auto status = coordinator_interaction_version->SendRequest("Watch", request, response, 600000);
    DINGO_LOG(INFO) << "SendRequest status=" << status << ", watch_count=" << i;
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SetUpSubcommandLock(CLI::App& app) {
  auto opt = std::make_shared<LockOptions>();
  auto coor = app.add_subcommand("Lock", "Lock")->group("Coordinator Manager Commands");
  coor->add_option("--lock_name", opt->lock_name, "Request parameter lock_name")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--client_uuid", opt->client_uuid, "Request parameter client_uuid")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandLock(*opt); });
}

void RunSubcommandLock(LockOptions const& opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }

  std::string lock_prefix = opt.lock_name + "_lock_";
  std::string lock_key = lock_prefix + opt.client_uuid;

  DINGO_LOG(INFO) << "lock_key=" << lock_key;

  // create lease
  int64_t lease_id = 0;
  int64_t ttl = 3;
  auto ret = CoorLeaseGrant(coordinator_interaction_version, lease_id, ttl);
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << "CoorLeaseGrant failed, ret=" << ret;
    return;
  }

  // renew lease in background
  Bthread bt(nullptr, PeriodRenwLease, coordinator_interaction_version, lease_id);

  // write lock key
  int64_t revision = 0;
  ret = CoorKvPut(coordinator_interaction_version, lock_key, "1", lease_id, revision);
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << "CoorKvPut failed, ret=" << ret;
    return;
  }

  // watch lock key
  do {
    // check if lock success
    std::string watch_key;
    int64_t watch_revision = 0;
    GetWatchKeyAndRevision(coordinator_interaction_version, lock_prefix, lock_key, watch_key, watch_revision);

    if (watch_key == lock_key) {
      DINGO_LOG(INFO) << "Get Lock success";
      bthread_usleep(3600 * 1000L * 1000L);
    }

    DINGO_LOG(WARNING) << "Lock failed, watch for key=" << watch_key << ", watch_revision=" << watch_revision;
    std::vector<dingodb::pb::version::Event> events;
    ret = CoorWatch(coordinator_interaction_version, watch_key, watch_revision, true, false, false, false, events);
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
  } while (true);
}

}  // namespace client_v2