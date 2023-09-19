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

#ifndef DINGODB_COMMON_CONSTANT_H_
#define DINGODB_COMMON_CONSTANT_H_

#include <cstdint>
#include <map>
#include <string>

namespace dingodb {

class Constant {
 public:
  // Define Global Region Id for Coordinator(As only One)
  static const int64_t kCoordinatorRegionId = 0;

  // Define Global Region Id for auto increment
  static const int64_t kAutoIncrementRegionId = 1;

  // Define Global Region Id for tso
  static const int64_t kTsoRegionId = 2;

  // Define Global TableID for Coordinator(As only one)
  static const int64_t kCoordinatorTableId = 0;

  // Define Global SchemaId for Coordinator(As only one)
  static const int64_t kCoordinatorSchemaId = 0;

  // Define mbvar metrics number
  static const int kBvarMaxDumpMultiDimensionMetricNumberDefault = 100;

  // Define Store data column family.
  inline static const std::string kStoreDataCF = "default";
  // Define Store meta column family.
  inline static const std::string kStoreMetaCF = "meta";
  // Txn column families.
  inline static const std::string kStoreTxnDataCF = "data";
  inline static const std::string kStoreTxnLockCF = "lock";
  inline static const std::string kStoreTxnWriteCF = "lock";
  // Define store meta prefix.
  inline static const std::string kStoreRegionMetaPrefix = "META_REGION";
  // Define store raft prefix.
  inline static const std::string kStoreRaftMetaPrefix = "META_RAFT";
  // Define store region metrics prefix.
  inline static const std::string kStoreRegionMetricsPrefix = "METRICS_REGION";
  // Define region controller prefix.
  inline static const std::string kStoreRegionControlCommandPrefix = "CONTROL_CMD";
  // Define vector index apply max log prefix.
  inline static const std::string kVectorIndexApplyLogIdPrefix = "VECTOR_INDEX_APPLY_LOG";
  // Define vector index snapshot max log prefix.
  inline static const std::string kVectorIndexSnapshotLogIdPrefix = "VECTOR_INDEX_SNAPSHOT_LOG";

  // Define default raft snapshot policy
  inline static const std::string kDefaultRaftSnapshotPolicy = "checkpoint";

  // flat map init capacity
  static const int64_t kStoreRegionMetaInitCapacity = 1024;

  // internal schema name
  inline static const std::string kRootSchemaName = "ROOT";
  inline static const std::string kMetaSchemaName = "META";
  inline static const std::string kDingoSchemaName = "DINGO";
  inline static const std::string kMySQLSchemaName = "MYSQL";
  inline static const std::string kInformationSchemaName = "INFORMATION_SCHEMA";

  // rocksdb config
  inline static const std::string kStorePathConfigName = "store.path";
  inline static const std::string kColumnFamilies = "store.column_families";
  inline static const std::string kBaseColumnFamily = "store.base";

  inline static const std::string kBlockSize = "block_size";
  inline static const std::string kBlockCache = "block_cache";
  inline static const std::string kArenaBlockSize = "arena_block_size";
  inline static const std::string kMinWriteBufferNumberToMerge = "min_write_buffer_number_to_merge";
  inline static const std::string kMaxWriteBufferNumber = "max_write_buffer_number";
  inline static const std::string kMaxCompactionBytes = "max_compaction_bytes";
  inline static const std::string kWriteBufferSize = "write_buffer_size";
  inline static const std::string kPrefixExtractor = "prefix_extractor";
  inline static const std::string kMaxBytesForLevelBase = "max_bytes_for_level_base";
  inline static const std::string kTargetFileSizeBase = "target_file_size_base";
  inline static const std::string kMaxBytesForLevelMultiplier = "max_bytes_for_level_multiplier";

  static const int kRocksdbBackgroundThreadNumDefault = 16;
  static const int kStatsDumpPeriodSecDefault = 600;

  // scan config
  inline static const std::string kStoreScan = "store.scan";
  inline static const std::string kStoreScanTimeoutS = "timeout_s";
  inline static const std::string kStoreScanMaxBytesRpc = "max_bytes_rpc";
  inline static const std::string kStoreScanMaxFetchCntByServer = "max_fetch_cnt_by_server";
  inline static const std::string kStoreScanScanIntervalS = "scan_interval_s";

  inline static const std::string kMetaRegionName = "0-COORDINATOR";
  inline static const std::string kAutoIncrementRegionName = "1-AUTO_INCREMENT";
  inline static const std::string kTsoRegionName = "2-TSO";

  // segment log
  static const uint32_t kSegmentLogDefaultMaxSegmentSize = 8 * 1024 * 1024;  // 8M
  static constexpr bool kSegmentLogSync = true;
  static const uint32_t kSegmentLogSyncPerBytes = INT32_MAX;

  // vector data number, e.g. data/scalar/table
  static const uint32_t kVectorDataCategoryNum = 3;
  // vector key prefix
  static const uint8_t kVectorDataPrefix = 0x01;
  static const uint8_t kVectorScalarPrefix = 0x02;
  static const uint8_t kVectorTablePrefix = 0x03;

  // File transport chunk size
  static const uint32_t kFileTransportChunkSize = 1024 * 1024;  // 1M

  // vector limitations
  static const uint32_t kVectorMaxDimension = 32768;
  static constexpr int64_t kVectorIndexSaveSnapshotThresholdWriteKeyNum = 100000;

  static const uint32_t kLoadOrBuildVectorIndexConcurrency = 5;

  static const uint32_t kBuildVectorIndexBatchSize = 8192;

  static constexpr int32_t kCreateIvfFlatParamNcentroids = 2048;
  static constexpr int32_t kSearchIvfFlatParamNprobe = 80;

  // split region
  static constexpr int kSplitDoSnapshotRetryTimes = 5;
  inline static const std::string kSplitStrategy = "PRE_CREATE_REGION";
  static constexpr uint32_t kDefaultSplitCheckConcurrency = 3;
  inline static const std::string kDefaultSplitPolicy = "HALF";
  static constexpr uint32_t kRegionMaxSizeDefaultValue = 67108864;  // 64M
  static constexpr float kDefaultSplitCheckApproximateSizeRatio = 0.8;
  static constexpr uint32_t kSplitChunkSizeDefaultValue = 1048576;  // 1M
  static constexpr float kSplitRatioDefaultValue = 0.5;
  static constexpr uint32_t kSplitKeysNumberDefaultValue = 100000;
  static constexpr float kSplitKeysRatioDefaultValue = 0.5;

  static const int32_t kRaftLogFallBehindThreshold = 1000;

  // transaction cf names
  inline static const std::string kTxnDataCF = "data";
  inline static const std::string kTxnLockCF = "lock";
  inline static const std::string kTxnWriteCF = "write";
  static constexpr uint32_t kTxnDataCfId = 0;
  static constexpr uint32_t kTxnLockCfId = 1;
  static constexpr uint32_t kTxnWriteCfId = 2;

  static constexpr uint64_t kLockVer = UINT64_MAX;

  // hnsw max elements expand number
  static const uint32_t kHnswMaxElementsExpandNum = 10000;

  // crontab default interval
  static const int32_t kHeartbeatIntervalS = 10;
  static const int32_t kScanIntervalS = 30;
  static const int32_t kPushIntervalS = 1;
  static const int32_t kUpdateStateIntervalS = 10;
  static const int32_t kTaskListIntervalS = 1;
  static const int32_t kCalcMetricsIntervalS = 60;
  static const int32_t kRecycleOrphanIntervalS = 60;
  static const int32_t kLeaseIntervalS = 60;
  static const int32_t kCompactionIntervalS = 300;
  static const int32_t kScrubVectorIndexIntervalS = 60;
  static const int32_t kApproximateSizeMetricsCollectIntervalS = 50;
  static const int32_t kStoreMetricsCollectIntervalS = 30;
  static const int32_t kRegionMetricsCollectIntervalS = 300;
  static const int32_t kDefaultSplitCheckIntervalS = 120;

  // raft snapshot
  inline static const std::string kRaftSnapshotRegionMetaFileName = "region_meta";

  static constexpr uint32_t kCollectApproximateSizeBatchSize = 1024;
  static constexpr int64_t kVectorIndexSnapshotCatchupMargin = 4000;

  static constexpr uint32_t kRaftElectionTimeoutSDefaultValue = 6;

  static constexpr int32_t kVectorIndexTaskRunningNumExpectValue = 6;

  static constexpr int32_t kPullVectorIndexSnapshotMinApplyLogId = 66;

  static constexpr int32_t kVectorIndexSaveTaskRunningNumMaxValue = 5;

  static constexpr int32_t kVectorIndexRebuildTaskRunningNumMaxValue = 5;
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_CONSTANT_H_
