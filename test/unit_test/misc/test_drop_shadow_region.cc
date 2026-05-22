// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

// =============================================================================
// NOTE: DropRegionPermanently orphan-verification + state gate
//
// DoDropRegionPermanently (src/server/coordinator_service.cc) refuses to erase
// coordinator metadata when --enable_store_verify=true and the region is still
// reported by any peer store -- see CheckRegionIsOrphanForDrop. The underlying
// state-gate predicate in CoordinatorControl::DropRegionPermanently emits a
// DELETE op_type for REGION_DELETED entries (heartbeat-driven shadows) while
// skipping REGION_DELETE / REGION_DELETING (already in flight) -- see
// src/coordinator/coordinator_control_coor.cc.
//
// This behavior is validated end-to-end against a running coordinator via
// dingodb_client / dingodb_cli. A unit-level test requires a CoordinatorControl
// fixture (raft / kv dependencies) that is not yet available in this repo;
// tracked as follow-up.
//
// E2E reproduction:
//   ./dingodb_client --method=DropRegionPermanently --id=80002 --enable_store_verify=false
//   ./dingodb_client --method=DropRegionPermanently --id=80001
//   ./dingodb_client --method=DropRegionPermanently --id=80001 --enable_store_verify=false
//   ./dingodb_client --method=DropRegionPermanently --id=80003
//   ./dingodb_client --method=DropRegionPermanently --id=80004
//
//   ./dingodb_cli DropRegionPermanently --id=80002
// =============================================================================
