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

#ifndef DINGODB_COORDINATOR_PREFIX_H_
#define DINGODB_COORDINATOR_PREFIX_H_

#include <string>

namespace dingodb {

inline static const std::string kPrefixKvIdEpoch = "KVIDEPO";
inline static const std::string kPrefixKvLease = "KVLEASE";
inline static const std::string kPrefixKvIndex = "KVINDEX";
inline static const std::string kPrefixKvRev = "KVREVIS";
inline static const std::string kPrefixDeletedTable = "DTABLE_";
inline static const std::string kPrefixDeletedIndex = "DINDEX_";
inline static const std::string kPrefixDeletedRegion = "DREGION";

inline static const std::string kPrefixCoordinator = "COORDIN";
inline static const std::string kPrefixStore = "STORE__";
inline static const std::string kPrefixSchema = "SCHEMA_";
inline static const std::string kPrefixRegion = "REGION_";
inline static const std::string kPrefixRegionMetrics = "REGIONM";
inline static const std::string kPrefixTable = "TABLE__";
inline static const std::string kPrefixIdEpoch = "IDEPOCH";
inline static const std::string kPrefixExecutor = "EXECUTO";
inline static const std::string kPrefixStoreMetrics = "STOREME";
inline static const std::string kPrefixTableMetrics = "TABLEME";
inline static const std::string kPrefixStoreOperation = "STOREOP";
inline static const std::string kPrefixRegionCmd = "REGICMD";
inline static const std::string kPrefixExecutorUser = "EXECUSR";
inline static const std::string kPrefixTaskList = "TASKLST";
inline static const std::string kPrefixIndex = "INDEX__";
inline static const std::string kPrefixIndexMetrics = "INDEXME";
inline static const std::string kPrefixTableIndex = "TBLINDE";

inline static const std::string kPrefixCommonDisk = "COMDISK";
inline static const std::string kPrefixCommonMem = "COMMEM_";

inline static const std::string kPrefixTenant = "TENANT_";

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_PREFIX_H_
