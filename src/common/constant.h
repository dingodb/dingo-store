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
#include <string>

namespace dingodb {

class Constant {
 public:
  // Define Global Region Id for Coordinator(As only One)
  static const uint64_t kCoordinatorRegionId = 0;

  // Define Global Region Id for auto increment
  static const uint64_t kAutoIncrementRegionId = 1;

  // Define Global Region Id for tso
  static const uint64_t kTsoRegionId = 2;

  // Define Global TableID for Coordinator(As only one)
  static const uint64_t kCoordinatorTableId = 0;

  // Define Global SchemaId for Coordinator(As only one)
  static const uint64_t kCoordinatorSchemaId = 0;

  // Define mbvar metrics number
  static const int kBvarMaxDumpMultiDimensionMetricNumberDefault = 100;

  // Define Store data column family.
  inline static const std::string kStoreDataCF = "default";
  // Define Store meta column family.
  inline static const std::string kStoreMetaCF = "meta";
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

  // Define loading snapshot flag.
  inline static const std::string kIsLoadingSnapshot = "IS_LOADING_SNAPSHOT";

  // Define default raft snapshot policy
  inline static const std::string kDefaultRaftSnapshotPolicy = "checkpoint";

  // flat map init capacity
  static const uint64_t kStoreRegionMetaInitCapacity = 1024;

  // rocksdb config
  inline static const std::string kDbPath = "store.path";
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
  static const bool kSegmentLogSync = true;
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

  static const uint32_t kBuildVectorIndexBatchSize = 4096;

  // split region
  static const uint32_t kDefaultSplitCheckConcurrency = 5;
  inline static const std::string kDefaultSplitPolicy = "HALF";
  static const uint32_t kDefaultRegionMaxSize = 134217728;  // 128M
  static constexpr float kDefaultSplitCheckApproximateSizeRatio = 0.8;
  static const uint32_t kDefaultSplitChunkSize = 1048576;  // 1M
  static constexpr float kDefaultSplitRatio = 0.5;
  static const uint32_t kDefaultSplitKeysNumber = 200000;
  static constexpr float kDefaultSplitKeysRatio = 0.5;

  // hnsw max elements expand number
  static const uint32_t kHnswMaxElementsExpandNum = 10000;

  // system resource usage
  static constexpr double kSystemDiskCapacityFreeRatio = 0.05;
  static constexpr double kSystemMemoryCapacityFreeRatio = 0.05;

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
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_CONSTANT_H_
