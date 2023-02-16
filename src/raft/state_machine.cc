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



#include "raft/state_machine.h"

#include "butil/strings/stringprintf.h"

namespace dingodb {


void StoreClosure::Run() {
  brpc::ClosureGuard done_guard(done_);
  if (!status().ok()) {
    cntl_->SetFailed(5000, "Raft closure failed");
    LOG(ERROR) << butil::StringPrintf("Store server closure failed, error_code:%d, error_mas:%s ",
      status().error_code(), status().error_cstr());
  }
}

StoreStateMachine::StoreStateMachine(std::shared_ptr<Engine> engine)
  : engine_(engine) {
}

void StoreStateMachine::on_apply(braft::Iterator& iter) {

}

void StoreStateMachine::on_shutdown() {

}

void StoreStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {

}

int StoreStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {

}

void StoreStateMachine::on_leader_start() {

}

void StoreStateMachine::on_leader_start(int64_t term) {

}

void StoreStateMachine::on_leader_stop() {

}

void StoreStateMachine::on_leader_stop(const butil::Status& status) {

}

void StoreStateMachine::on_error(const ::braft::Error& e) {

}

void StoreStateMachine::on_configuration_committed(const ::braft::Configuration& conf) {

}


} // namespace dingodb