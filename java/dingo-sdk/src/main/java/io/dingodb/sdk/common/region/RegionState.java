/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.sdk.common.region;

public enum RegionState {
    REGION_NEW,  // create new region
    REGION_NORMAL,

    REGION_EXPAND,     // start to expand
    REGION_EXPANDING,  // leader start to expand region
    REGION_EXPANDED,   // new peer joined raft

    REGION_SHRINK,      // start to shrink
    REGION_SHIRINKING,  // leader start to shrink region
    REGION_SHRANK,      // shrink finish, maybe we don't need this state

    REGION_DELETE,    // region need to delete
    REGION_DELETING,  // region is deleting
    REGION_DELETED,  // region is deleted

    REGION_SPLIT,      // region need to split
    REGION_SPLITTING,  // region is splitting
    REGION_SPLITED,    // region is splited (split's past tense is split, not splited, use as a symbol here)

    REGION_MERGE,    // region need to merge
    REGION_MERGING,  // region is mergting
    REGION_MERGED,   // region is merged

    // other state add here
    REGION_ILLEGAL,  // region is not create by coordinator
    REGION_STANDBY
}
