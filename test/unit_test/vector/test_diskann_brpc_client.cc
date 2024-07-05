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

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <vector>

#include "brpc/channel.h"
#include "butil/status.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "proto/diskann.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class DiskANNBrpcClientTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 5000;  // 5s
    // options.protocol = brpc::AdaptiveProtocolType(brpc::ProtocolType::PROTOCOL_H2);
    options.protocol = "h2:grpc";
    options.timeout_ms = 0x7fffffff /*milliseconds*/;
    options.max_retry = 3;
    options.connection_type = brpc::ConnectionType::CONNECTION_TYPE_SINGLE;
    if (channel.Init(kServerPort.c_str(), kLoadBalancer.c_str(), &options) != 0) {
      LOG(ERROR) << "Fail to initialize channel";
      exit(1);
    }
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}

  inline static brpc::Channel channel;
  inline static const std::string kServerPort = "0.0.0.0:34001";
  inline static const std::string kLoadBalancer;
  inline static int64_t vector_index_id_l2 = 1;
  inline static int64_t vector_index_id_ip = 2;
  inline static int64_t vector_index_id_cosine = 3;
  inline static int64_t vector_index_id_l2_two = 4;
  inline static int dimension = 8;
  inline static int data_base_size = 10;
  inline static std::vector<float> data_base;
};

TEST_F(DiskANNBrpcClientTest, Destroy) {
  butil::Status status;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, VectorNew) {
  butil::Status status;

  // diskann invalid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorNewRequest request;
      pb::diskann::VectorNewResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      request.mutable_vector_index_parameter()->CopyFrom(index_parameter);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorNew(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  // diskann invalid param IP
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(0);
    index_parameter.mutable_diskann_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorNewRequest request;
      pb::diskann::VectorNewResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      request.mutable_vector_index_parameter()->CopyFrom(index_parameter);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorNew(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
        break;
      }
      usleep(1 * 1000L);
    }
  }

  // diskann invalid param cosine
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(1000);

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorNewRequest request;
      pb::diskann::VectorNewResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      request.mutable_vector_index_parameter()->CopyFrom(index_parameter);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorNew(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  ////////////////////////////////////////////////diskann//////////////////////////////////////////////////////////////////////////
  // diskann valid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorNewRequest request;
      pb::diskann::VectorNewResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      request.mutable_vector_index_parameter()->CopyFrom(index_parameter);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorNew(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  // diskann valid param IP
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorNewRequest request;
      pb::diskann::VectorNewResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);
      request.mutable_vector_index_parameter()->CopyFrom(index_parameter);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorNew(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  // diskann valid param cosine
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorNewRequest request;
      pb::diskann::VectorNewResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);
      request.mutable_vector_index_parameter()->CopyFrom(index_parameter);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorNew(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  // repeat diskann invalid param L2
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorNewRequest request;
      pb::diskann::VectorNewResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      request.mutable_vector_index_parameter()->CopyFrom(index_parameter);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorNew(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_EXISTS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  // diskann valid param L2 two
  {
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN);
    index_parameter.mutable_diskann_parameter()->set_dimension(dimension);
    index_parameter.mutable_diskann_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_diskann_parameter()->set_value_type(pb::common::ValueType::FLOAT);
    index_parameter.mutable_diskann_parameter()->set_max_degree(64);
    index_parameter.mutable_diskann_parameter()->set_search_list_size(100);

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorNewRequest request;
      pb::diskann::VectorNewResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2_two);
      request.mutable_vector_index_parameter()->CopyFrom(index_parameter);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorNew(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Prepare) {
  butil::Status status;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dimension * data_base_size, 0.0f);

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dimension; j++) data_base[dimension * i + j] = distrib(rng);
      data_base[dimension * i] += i / 1000.;
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Import) {
  butil::Status status;
  std::vector<pb::common::Vector> vectors;
  std::vector<int64_t> vector_ids;

  {
    for (int i = 0; i < data_base_size; i++) {
      pb::common::Vector vector;
      vector.set_dimension(dimension);
      for (int j = 0; j < dimension; j++) {
        vector.add_float_values(*(data_base.data() + i * dimension + j));
      }
      vectors.push_back(vector);
      vector_ids.push_back(i + data_base_size);
    }
  }

  //  invalid param vector_index_id == 0
  {
    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorPushDataRequest request;
      pb::diskann::VectorPushDataResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      for (int i = 0; i < vectors.size(); i++) {
        request.add_vectors()->CopyFrom(vectors[i]);
        request.add_vector_ids(vector_ids[i]);
      }
      request.set_has_more(has_more);
      request.set_error(::dingodb::pb::error::Errno::OK);
      request.set_force_to_load_data_if_exist(force_to_load_data_if_exist);
      request.set_already_send_vector_count(already_send_vector_count);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorPushData(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorPushDataRequest request;
      pb::diskann::VectorPushDataResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);
      request.set_ts(10);
      request.set_tso(100);
      for (int i = 0; i < vectors.size(); i++) {
        request.add_vectors()->CopyFrom(vectors[i]);
        request.add_vector_ids(vector_ids[i]);
      }
      request.set_has_more(has_more);
      request.set_error(::dingodb::pb::error::Errno::OK);
      request.set_force_to_load_data_if_exist(force_to_load_data_if_exist);
      request.set_already_send_vector_count(already_send_vector_count);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorPushData(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param already_send_vector_count wrong
  {
    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorPushDataRequest request;
      pb::diskann::VectorPushDataResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2_two);
      request.set_ts(10);
      request.set_tso(100);
      for (int i = 0; i < vectors.size(); i++) {
        request.add_vectors()->CopyFrom(vectors[i]);
        request.add_vector_ids(vector_ids[i]);
      }
      request.set_has_more(has_more);
      request.set_error(::dingodb::pb::error::Errno::OK);
      request.set_force_to_load_data_if_exist(force_to_load_data_if_exist);
      request.set_already_send_vector_count(already_send_vector_count + 10);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorPushData(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(ERROR) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::EDISKANN_FILE_TRANSFER_QUANTITY_MISMATCH);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param add_vectors != vector_ids  wrong
  {
    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorPushDataRequest request;
      pb::diskann::VectorPushDataResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      for (int i = 0; i < vectors.size(); i++) {
        request.add_vectors()->CopyFrom(vectors[i]);
        request.add_vector_ids(vector_ids[i]);
      }

      request.add_vector_ids(20);

      request.set_has_more(has_more);
      request.set_error(::dingodb::pb::error::Errno::OK);
      request.set_force_to_load_data_if_exist(force_to_load_data_if_exist);
      request.set_already_send_vector_count(already_send_vector_count);
      request.set_ts(10);
      request.set_tso(100);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorPushData(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(ERROR) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorPushDataRequest request;
      pb::diskann::VectorPushDataResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      for (int i = 0; i < vectors.size(); i++) {
        request.add_vectors()->CopyFrom(vectors[i]);
        request.add_vector_ids(vector_ids[i]);
      }
      request.set_has_more(has_more);
      request.set_error(::dingodb::pb::error::Errno::OK);
      request.set_force_to_load_data_if_exist(force_to_load_data_if_exist);
      request.set_already_send_vector_count(already_send_vector_count);
      request.set_ts(10);
      request.set_tso(100);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorPushData(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorPushDataRequest request;
      pb::diskann::VectorPushDataResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);
      for (int i = 0; i < vectors.size(); i++) {
        request.add_vectors()->CopyFrom(vectors[i]);
        request.add_vector_ids(vector_ids[i]);
      }
      request.set_has_more(has_more);
      request.set_error(::dingodb::pb::error::Errno::OK);
      request.set_force_to_load_data_if_exist(force_to_load_data_if_exist);
      request.set_already_send_vector_count(already_send_vector_count);
      request.set_ts(10);
      request.set_tso(100);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorPushData(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    bool has_more = false;
    bool force_to_load_data_if_exist = false;
    int64_t already_send_vector_count = 0;
    int64_t already_recv_vector_count = 0;

    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorPushDataRequest request;
      pb::diskann::VectorPushDataResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);
      for (int i = 0; i < vectors.size(); i++) {
        request.add_vectors()->CopyFrom(vectors[i]);
        request.add_vector_ids(vector_ids[i]);
      }
      request.set_has_more(has_more);
      request.set_error(::dingodb::pb::error::Errno::OK);
      request.set_force_to_load_data_if_exist(force_to_load_data_if_exist);
      request.set_already_send_vector_count(already_send_vector_count);
      request.set_ts(10);
      request.set_tso(100);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorPushData(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Build) {
  butil::Status status;

  // If true, force to build even if file already exist.
  bool force_to_build_if_exist = false;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorBuildRequest request;
      pb::diskann::VectorBuildResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      request.set_force_to_build_if_exist(force_to_build_if_exist);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorBuild(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorBuildRequest request;
      pb::diskann::VectorBuildResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);
      request.set_force_to_build_if_exist(force_to_build_if_exist);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorBuild(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorBuildRequest request;
      pb::diskann::VectorBuildResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      request.set_force_to_build_if_exist(force_to_build_if_exist);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorBuild(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorBuildRequest request;
      pb::diskann::VectorBuildResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);
      request.set_force_to_build_if_exist(force_to_build_if_exist);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorBuild(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorBuildRequest request;
      pb::diskann::VectorBuildResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);
      request.set_force_to_build_if_exist(force_to_build_if_exist);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorBuild(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Load) {
  butil::Status status;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorLoadRequest request;
      pb::diskann::VectorLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorLoadRequest request;
      pb::diskann::VectorLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorLoadRequest request;
      pb::diskann::VectorLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorLoadRequest request;
      pb::diskann::VectorLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorLoadRequest request;
      pb::diskann::VectorLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Search) {
  butil::Status status;

  std::vector<pb::common::Vector> vectors;
  for (int i = 0; i < data_base_size; i++) {
    pb::common::Vector vector;
    vector.set_dimension(dimension);
    vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector.add_float_values(data_base[i * dimension + j]);
    }

    vectors.push_back(vector);
  }
  uint32_t top_n = 3;
  pb::common::SearchDiskAnnParam search_param;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      request.set_top_n(top_n);
      request.mutable_search_param()->CopyFrom(search_param);
      for (const auto& vector : vectors) {
        request.add_vectors()->CopyFrom(vector);
      }

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);
      request.set_top_n(top_n);
      request.mutable_search_param()->CopyFrom(search_param);
      for (const auto& vector : vectors) {
        request.add_vectors()->CopyFrom(vector);
      }

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      request.set_top_n(top_n);
      request.mutable_search_param()->CopyFrom(search_param);
      for (const auto& vector : vectors) {
        request.add_vectors()->CopyFrom(vector);
      }

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << response.DebugString();
        DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << response.batch_results().size();
        for (size_t i = 0; i < response.batch_results().size(); i++) {
          DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << response.batch_results()[i].DebugString();
        }
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);
      request.set_top_n(top_n);
      request.mutable_search_param()->CopyFrom(search_param);
      for (const auto& vector : vectors) {
        request.add_vectors()->CopyFrom(vector);
      }

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << response.batch_results().size();
        for (size_t i = 0; i < response.batch_results().size(); i++) {
          DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << response.batch_results()[i].DebugString();
        }
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << response.batch_results().size();
        for (size_t i = 0; i < response.batch_results().size(); i++) {
          DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << response.batch_results()[i].DebugString();
        }
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Status) {
  butil::Status status;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorStatusRequest request;
      pb::diskann::VectorStatusResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorStatus(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorStatusRequest request;
      pb::diskann::VectorStatusResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorStatus(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorStatusRequest request;
      pb::diskann::VectorStatusResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorStatus(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 status: " << pb::common::DiskANNCoreState_Name(response.state());
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorStatusRequest request;
      pb::diskann::VectorStatusResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorStatus(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 status: " << pb::common::DiskANNCoreState_Name(response.state());
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorStatusRequest request;
      pb::diskann::VectorStatusResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorStatus(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 status: " << pb::common::DiskANNCoreState_Name(response.state());
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Close) {
  butil::Status status;

  // If true, force to build even if file already exist.
  bool force_to_build_if_exist = false;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCloseRequest request;
      pb::diskann::VectorCloseResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorClose(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCloseRequest request;
      pb::diskann::VectorCloseResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorClose(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCloseRequest request;
      pb::diskann::VectorCloseResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorClose(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCloseRequest request;
      pb::diskann::VectorCloseResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorClose(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCloseRequest request;
      pb::diskann::VectorCloseResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorClose(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, TryLoad) {
  butil::Status status;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorTryLoadRequest request;
      pb::diskann::VectorTryLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorTryLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorTryLoadRequest request;
      pb::diskann::VectorTryLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorTryLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorTryLoadRequest request;
      pb::diskann::VectorTryLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorTryLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorTryLoadRequest request;
      pb::diskann::VectorTryLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorTryLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorTryLoadRequest request;
      pb::diskann::VectorTryLoadResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);
      request.mutable_load_param()->set_num_nodes_to_cache(2);
      request.mutable_load_param()->set_warmup(true);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorTryLoad(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, SearchAgain) {
  butil::Status status;

  std::vector<pb::common::Vector> vectors;
  for (int i = 0; i < data_base_size; i++) {
    pb::common::Vector vector;
    vector.set_dimension(dimension);
    vector.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (int j = 0; j < dimension; j++) {
      vector.add_float_values(data_base[i * dimension + j]);
    }

    vectors.push_back(vector);
  }
  uint32_t top_n = 3;
  pb::common::SearchDiskAnnParam search_param;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      request.set_top_n(top_n);
      request.mutable_search_param()->CopyFrom(search_param);
      for (const auto& vector : vectors) {
        request.add_vectors()->CopyFrom(vector);
      }

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);
      request.set_top_n(top_n);
      request.mutable_search_param()->CopyFrom(search_param);
      for (const auto& vector : vectors) {
        request.add_vectors()->CopyFrom(vector);
      }

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      request.set_top_n(top_n);
      request.mutable_search_param()->CopyFrom(search_param);
      for (const auto& vector : vectors) {
        request.add_vectors()->CopyFrom(vector);
      }

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 results size: " << response.batch_results().size();
        for (size_t i = 0; i < response.batch_results().size(); i++) {
          DINGO_LOG(INFO) << "disk_ann_item_l2 result: " << i << " " << response.batch_results()[i].DebugString();
        }
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);
      request.set_top_n(top_n);
      request.mutable_search_param()->CopyFrom(search_param);
      for (const auto& vector : vectors) {
        request.add_vectors()->CopyFrom(vector);
      }

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << response.batch_results().size();
        for (size_t i = 0; i < response.batch_results().size(); i++) {
          DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << response.batch_results()[i].DebugString();
        }
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorSearchRequest request;
      pb::diskann::VectorSearchResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorSearch(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_ip results size: " << response.batch_results().size();
        for (size_t i = 0; i < response.batch_results().size(); i++) {
          DINGO_LOG(INFO) << "disk_ann_item_ip result: " << i << " " << response.batch_results()[i].DebugString();
        }
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Dump) {
  butil::Status status;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDumpRequest request;
      pb::diskann::VectorDumpResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDump(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDumpRequest request;
      pb::diskann::VectorDumpResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDump(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDumpRequest request;
      pb::diskann::VectorDumpResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDump(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 status: " << response.dump_data();
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDumpRequest request;
      pb::diskann::VectorDumpResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDump(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 status: " << response.dump_data();
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDumpRequest request;
      pb::diskann::VectorDumpResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDump(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 status: " << response.dump_data();
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, DumpAll) {
  butil::Status status;

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDumpAllRequest request;
      pb::diskann::VectorDumpAllResponse response;
      brpc::Controller cntl;

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDumpAll(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 " << response.DebugString();
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Count) {
  butil::Status status;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCountRequest request;
      pb::diskann::VectorCountResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorCount(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCountRequest request;
      pb::diskann::VectorCountResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorCount(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCountRequest request;
      pb::diskann::VectorCountResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorCount(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 status: " << response.DebugString();
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCountRequest request;
      pb::diskann::VectorCountResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorCount(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 status: " << response.DebugString();
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorCountRequest request;
      pb::diskann::VectorCountResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorCount(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        DINGO_LOG(INFO) << "disk_ann_item_l2 status: " << response.DebugString();
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, Reset) {
  butil::Status status;
  bool delete_data_file = false;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorResetRequest request;
      pb::diskann::VectorResetResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);
      request.set_delete_data_file(delete_data_file);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorReset(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorResetRequest request;
      pb::diskann::VectorResetResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);
      request.set_delete_data_file(delete_data_file);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorReset(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorResetRequest request;
      pb::diskann::VectorResetResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      request.set_delete_data_file(delete_data_file);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorReset(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorResetRequest request;
      pb::diskann::VectorResetResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);
      request.set_delete_data_file(delete_data_file);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorReset(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorResetRequest request;
      pb::diskann::VectorResetResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);
      request.set_delete_data_file(delete_data_file);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorReset(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  delete_data_file = true;

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorResetRequest request;
      pb::diskann::VectorResetResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);
      request.set_delete_data_file(delete_data_file);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorReset(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorResetRequest request;
      pb::diskann::VectorResetResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);
      request.set_delete_data_file(delete_data_file);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorReset(&cntl, &request, &response, nullptr);
      DINGO_LOG(INFO) << response.DebugString();
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorResetRequest request;
      pb::diskann::VectorResetResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);
      request.set_delete_data_file(delete_data_file);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorReset(&cntl, &request, &response, nullptr);
      DINGO_LOG(INFO) << response.DebugString();
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

TEST_F(DiskANNBrpcClientTest, DestroyAgain) {
  butil::Status status;

  //  invalid param vector_index_id == 0
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(0);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EILLEGAL_PARAMTETERS);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  invalid param vector_index_id == 10
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(10);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        EXPECT_EQ(response.error().errcode(), pb::error::EINDEX_NOT_FOUND);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  l2 ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_l2);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  ip ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_ip);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }

  //  cosine ok
  {
    pb::diskann::DiskAnnService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit()) {
      pb::diskann::VectorDestroyRequest request;
      pb::diskann::VectorDestroyResponse response;
      brpc::Controller cntl;

      request.set_vector_index_id(vector_index_id_cosine);

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.VectorDestroy(&cntl, &request, &response, nullptr);
      if (!cntl.Failed()) {
        DINGO_LOG(INFO) << "Received response from " << cntl.remote_side() << " to " << cntl.local_side() << ": "
                        << static_cast<int>(response.error().errcode()) << " latency=" << cntl.latency_us() << "us";
        DINGO_LOG(INFO) << response.DebugString();
        EXPECT_EQ(response.error().errcode(), pb::error::OK);
        break;
      } else {
        DINGO_LOG(ERROR) << cntl.ErrorText() << ", error : " << response.DebugString();
      }
      usleep(1 * 1000L);
    }
  }
}

}  // namespace dingodb
