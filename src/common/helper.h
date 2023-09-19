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

#ifndef DINGODB_COMMON_HELPER_H_
#define DINGODB_COMMON_HELPER_H_

#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "braft/configuration.h"
#include "butil/endpoint.h"
#include "butil/status.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

extern const std::map<std::string, uint32_t> kTxnCf2Id;

class Helper {
  using Errno = pb::error::Errno;
  using PbError = pb::error::Error;

 public:
  static int GetCoreNum();
  static bool IsIp(const std::string& s);

  static butil::EndPoint GetEndPoint(const std::string& host, int port);
  static butil::EndPoint GetEndPoint(const std::string& addr);

  static std::string Ip2HostName(const std::string& ip);

  static bool IsDifferenceLocation(const pb::common::Location& location, const pb::common::Location& other_location);
  static bool IsDifferencePeers(const std::vector<pb::common::Peer>& peers,
                                const std::vector<pb::common::Peer>& other_peers);

  static void SortPeers(std::vector<pb::common::Peer>& peers);
  static std::vector<pb::common::Location> ExtractLocations(
      const google::protobuf::RepeatedPtrField<pb::common::Peer>& peers);
  static std::vector<pb::common::Location> ExtractLocations(const std::vector<pb::common::Peer>& peers);

  static std::string PeersToString(const std::vector<pb::common::Peer>& peers);

  // format: 127.0.0.1:8201:0
  static std::string LocationToString(const pb::common::Location& location);

  // transform braft::PeerId to Location
  // return 0 or -1
  static int PeerIdToLocation(braft::PeerId peer_id, pb::common::Location& location);

  static butil::EndPoint LocationToEndPoint(const pb::common::Location& location);
  static pb::common::Location EndPointToLocation(const butil::EndPoint& endpoint);

  static braft::PeerId LocationToPeer(const pb::common::Location& location);

  // format: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
  static std::string FormatPeers(const std::vector<pb::common::Location>& locations);

  static std::string FormatPeers(const braft::Configuration& conf);

  // 127.0.0.1:8201,127.0.0.1:8202,127.0.0.1:8203 to EndPoint
  static butil::EndPoint StrToEndPoint(std::string str);
  static std::vector<butil::EndPoint> StrToEndpoints(const std::string& str);

  static std::string EndPointToStr(const butil::EndPoint& end_point);

  static std::shared_ptr<PbError> Error(Errno errcode, const std::string& errmsg);
  static bool Error(Errno errcode, const std::string& errmsg, PbError& err);
  static bool Error(Errno errcode, const std::string& errmsg, std::shared_ptr<PbError> err);

  static bool IsEqualIgnoreCase(const std::string& str1, const std::string& str2);

  // protobuf transform
  template <typename T>
  static std::vector<T> PbRepeatedToVector(const google::protobuf::RepeatedPtrField<T>& data) {
    std::vector<T> vec;
    vec.reserve(data.size());
    for (auto& item : data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(google::protobuf::RepeatedPtrField<T>* data) {
    std::vector<T> vec;
    vec.reserve(data->size());
    for (auto& item : *data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(const google::protobuf::RepeatedField<T>& data) {
    std::vector<T> vec;
    for (auto& item : data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(google::protobuf::RepeatedField<T>* data) {
    std::vector<T> vec;
    for (auto& item : *data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec, google::protobuf::RepeatedPtrField<T>* out) {
    for (auto& item : vec) {
      *(out->Add()) = item;
    }
  }

  static std::string PrefixNext(const std::string& input);
  static std::string PrefixNext(const std::string_view& input);

  // generate min and max start key of dingo-store
  // partition_id cannot be 0 and INT64_MAX
  static std::string GenMaxStartKey();
  static std::string GenMinStartKey();

  // Transform RangeWithOptions to Range for scan/deleteRange
  static pb::common::Range TransformRangeWithOptions(const pb::common::RangeWithOptions& range_with_options);

  // Take range intersection
  static pb::common::Range IntersectRange(const pb::common::Range& range1, const pb::common::Range& range2);

  static std::string StringToHex(const std::string& str);
  static std::string StringToHex(const std::string_view& str);
  static std::string HexToString(const std::string& hex_str);

  static void SetPbMessageError(butil::Status status, google::protobuf::Message* message);

  template <typename T>
  static void SetPbMessageErrorLeader(const pb::node::NodeInfo& node_info, T* message) {
    message->mutable_error()->set_store_id(node_info.id());
    auto leader_location = message->mutable_error()->mutable_leader_location();
    *leader_location = node_info.server_location();
  }

  static std::string MessageToJsonString(const google::protobuf::Message& message);

  // use raft_location to get server_location
  // in: raft_location
  // out: server_location
  static void GetNodeInfoByRaftLocation(const pb::common::Location& raft_location, pb::node::NodeInfo& node_info);
  static void GetNodeInfoByServerLocation(const pb::common::Location& server_location, pb::node::NodeInfo& node_info);
  static void GetServerLocation(const pb::common::Location& raft_location, pb::common::Location& server_location);
  static void GetRaftLocation(const pb::common::Location& server_location, pb::common::Location& raft_location);
  static pb::common::Peer GetPeerInfo(const butil::EndPoint& endpoint);

  // generate random string for keyring
  static std::string GenerateRandomString(int length);
  static int64_t GenerateRealRandomInteger(int64_t min_value, int64_t max_value);
  static int64_t GenerateRandomInteger(int64_t min_value, int64_t max_value);
  static float GenerateRandomFloat(float min_value, float max_value);
  static int64_t GenId();

  // calc hnsw count from memory
  static uint32_t CalcHnswCountFromMemory(int64_t memory_size_limit, int64_t dimension, int64_t nlinks);

  // Gen coordinator new_table_check_name
  static std::string GenNewTableCheckName(int64_t schema_id, const std::string& table_name);

  // Gen mvcc cf vector for transaction
  static std::vector<std::string> GenMvccCfVector();

  // Create hard link
  static bool Link(const std::string& old_path, const std::string& new_path);

  // nanosecond timestamp
  static int64_t TimestampNs();
  // millisecond timestamp
  static int64_t TimestampMs();
  // second timestamp
  static int64_t Timestamp();
  static std::string NowTime();

  // format millisecond timestamp
  static std::string FormatMsTime(int64_t timestamp, const std::string& format);
  // format second timestamp
  static std::string FormatTime(int64_t timestamp, const std::string& format);

  // format: "2021-01-01T00:00:00.000Z"
  static std::string GetNowFormatMsTime();

  // end key of all table
  // We are based on this assumption. In general, it is rare to see all 0xFF
  static bool KeyIsEndOfAllTable(const std::string& key);

  static bool GetSystemDiskCapacity(const std::string& path, std::map<std::string, int64_t>& output);
  static bool GetSystemMemoryInfo(std::map<std::string, int64_t>& output);
  static bool GetSystemCpuUsage(std::map<std::string, int64_t>& output);
  static bool GetSystemDiskIoUtil(const std::string& device_name, std::map<std::string, int64_t>& output);
  static bool GetProcessMemoryInfo(std::map<std::string, int64_t>& output);

  static void AlignByteArrays(std::string& a, std::string& b);
  // Notice: String will add one element as a prefix of the result, this element is for the carry
  // if you want the equal length of your input, you need to do substr by yourself
  static std::string StringAdd(const std::string& input_a, const std::string& input_b);
  static std::string StringSubtract(const std::string& input_a, const std::string& input_b);

  // Notice: if array % 2 != 0, the result size is array.size() + 1
  static std::string StringDivideByTwo(const std::string& array);

  static void RightAlignByteArrays(std::string& a, std::string& b);
  static std::string StringAddRightAlign(const std::string& input_a, const std::string& input_b);
  static std::string StringSubtractRightAlign(const std::string& input_a, const std::string& input_b);
  static std::string StringDivideByTwoRightAlign(const std::string& array);

  static std::string CalculateMiddleKey(const std::string& start_key, const std::string& end_key);

  static std::vector<uint8_t> SubtractByteArrays(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b);
  static std::vector<uint8_t> DivideByteArrayByTwo(const std::vector<uint8_t>& array);
  // Notice: AddByteArrays will add one element as a prefix of the result, this element is for the carry
  // if you want the equal length of your input, you need to do substr by yourself
  static std::vector<uint8_t> AddByteArrays(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b);

  // filesystem operations
  static std::vector<std::string> TraverseDirectory(const std::string& path, bool ignore_dir = false,
                                                    bool ignore_file = false);
  static std::vector<std::string> TraverseDirectory(const std::string& path, const std::string& prefix,
                                                    bool ignore_dir = false, bool ignore_file = false);
  static std::string FindFileInDirectory(const std::string& dirpath, const std::string& prefix);
  static bool CreateDirectory(const std::string& path);
  static butil::Status CreateDirectories(const std::string& path);
  static bool RemoveFileOrDirectory(const std::string& path);
  static bool RemoveAllFileOrDirectory(const std::string& path);
  static butil::Status Rename(const std::string& src_path, const std::string& dst_path, bool is_force = true);
  static bool IsExistPath(const std::string& path);

  // vector scalar index value
  static bool IsEqualVectorScalarValue(const pb::common::ScalarValue& value1, const pb::common::ScalarValue& value2);
  // for index region, encode raw_range
  // static std::string EncodeIndexRegionHeader(int64_t partition_id, int64_t vector_id);

  // for index region transaction, decode vector_id from key
  static uint64_t DecodeVectorId(const std::string& value);

  // for txn, encode start_ts/commit_ts to std::string
  static std::string EncodeTso(uint64_t ts);
  // for txn, padding user key
  static std::string PaddingUserKey(const std::string& key);
  static std::string UnpaddingUserKey(const std::string& padding_key);
  // for txn, encode data/write key
  static std::string EncodeTxnKey(const std::string& key, uint64_t ts);
  static std::string EncodeTxnKey(const std::string_view& key, uint64_t ts);
  // for txn, encode data/write key
  static butil::Status DecodeTxnKey(const std::string& txn_key, std::string& key, uint64_t& ts);
  // for txn, encode data/write key
  static butil::Status DecodeTxnKey(const std::string_view& txn_key, std::string& key, uint64_t& ts);

  // Upper string
  static std::string ToUpper(const std::string& str);
  // Lower string
  static std::string ToLower(const std::string& str);
  // String trim
  static std::string Ltrim(const std::string& s, const std::string& delete_str);
  static std::string Rtrim(const std::string& s, const std::string& delete_str);
  static std::string Trim(const std::string& s, const std::string& delete_str);

  // Clean string first slash, e.g. /name.txt -> name.txt
  static std::string CleanFirstSlash(const std::string& str);

  // Parallel run task, e.g. load vector index.
  using TaskFunctor = void* (*)(void*);
  static bool ParallelRunTask(TaskFunctor task, void* arg, int concurrency);

  // Validate raft status whether suitable or not region split.
  static butil::Status ValidateRaftStatusForSplit(std::shared_ptr<pb::common::BRaftStatus> raft_status);

  static butil::Status ParseRaftSnapshotRegionMeta(const std::string& snapshot_path,
                                                   pb::store_internal::RaftSnapshotRegionMeta& meta);
};

}  // namespace dingodb

#endif
