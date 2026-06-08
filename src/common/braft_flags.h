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

#ifndef DINGODB_COMMON_BRAFT_FLAGS_H_
#define DINGODB_COMMON_BRAFT_FLAGS_H_

#include "gflags/gflags_declare.h"

// Single place to declare the braft gflags that dingo's ControlConfig exposes for
// runtime control, so the declarations do not get scattered across translation
// units. Two categories live here:
//   * raft_sync             -- braft PUBLIC flag (also declared in braft/storage.h).
//                              Re-declared here so callers can reach it without
//                              pulling in the heavy braft storage header.
//   * raft_meta_force_no_sync -- braft INTERNAL flag, NOT exposed by any braft
//                              public header (defined in braft/raft_meta.cpp). The
//                              symbol exists in libbraft.a, so the declaration links;
//                              if braft is upgraded and renames/removes it, only this
//                              file (plus a link error) needs attention.
namespace braft {

// true  => braft fsyncs raft data per raft_sync_* settings (normal, safe).
// false => braft skips the fsync (faster, weaker durability).
DECLARE_bool(raft_sync);

// true  => braft will NOT fsync raft meta (vote records) on every write.
//          Lowers write latency but a machine *power failure* can lose unsynced
//          vote records (a process crash is still safe). This is a DANGEROUS,
//          durability-weakening setting -- see braft/raft_meta.cpp.
// false => normal behaviour (raft meta is fsync'd per raft_sync settings).
DECLARE_bool(raft_meta_force_no_sync);

}  // namespace braft

#endif  // DINGODB_COMMON_BRAFT_FLAGS_H_
