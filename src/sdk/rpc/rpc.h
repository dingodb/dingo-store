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

#ifndef DINGODB_SDK_RPC_H_
#define DINGODB_SDK_RPC_H_

#include <string>

#include "brpc/callback.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "butil/fast_rand.h"
#include "common/logging.h"
#include "google/protobuf/message.h"
#include "sdk/common/param_config.h"
#include "sdk/status.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

class Rpc {
 public:
  Rpc(const std::string& p_cmd) : cmd(p_cmd) {}

  virtual ~Rpc() = default;

  brpc::Controller* MutableController() { return &controller; }

  const butil::EndPoint& GetEndPoint() const { return end_point; }

  void SetEndPoint(const butil::EndPoint& p_end_point) { end_point = p_end_point; }

  const brpc::Controller* Controller() const { return &controller; }

  Status GetStatus() { return status; }

  void SetStatus(const Status& s) { status = s; }

  virtual google::protobuf::Message* RawMutableRequest() = 0;

  virtual const google::protobuf::Message* RawRequest() const = 0;

  virtual google::protobuf::Message* RawMutableResponse() = 0;

  virtual const google::protobuf::Message* RawResponse() const = 0;

  virtual std::string ServiceName() = 0;

  virtual std::string ServiceFullName() = 0;

  virtual std::string Method() const = 0;

  virtual void Reset() = 0;

  virtual void Call(brpc::Channel* channel, RpcCallback cb) = 0;

 protected:
  std::string cmd;
  brpc::Controller controller;
  butil::EndPoint end_point;
  Status status;
};

template <class RequestType, class ResponseType, class ServiceType, class StubType>
class ClientRpc : public Rpc {
 public:
  ClientRpc(const std::string& cmd) : Rpc(cmd) {
    request = new RequestType;
    response = new ResponseType;
  }

  ~ClientRpc() override {
    delete request;
    delete response;
  }

  RequestType* MutableRequest() { return request; }

  const RequestType* Request() const { return request; }

  ResponseType* MutableResponse() { return response; }

  const ResponseType* Response() const { return response; }

  google::protobuf::Message* RawMutableRequest() override { return request; }

  const google::protobuf::Message* RawRequest() const override { return request; }

  google::protobuf::Message* RawMutableResponse() override { return response; }

  const google::protobuf::Message* RawResponse() const override { return response; }

  std::string ServiceName() override { return ServiceType::descriptor()->name(); }

  std::string ServiceFullName() override { return ServiceType::descriptor()->full_name(); }

  void OnClientRpcDone(RpcCallback cb) {
    if (controller.Failed()) {
      DINGO_LOG(WARNING) << "Fail send rpc: " << Method() << ", log_id:" << controller.log_id()
                         << " endpoint:" << butil::endpoint2str(controller.remote_side()).c_str()
                         << " error_code:" << controller.ErrorCode() << " error_text:" << controller.ErrorText();

      Status err = Status::NetworkError(controller.ErrorCode(), controller.ErrorText());
      SetStatus(err);
    } else {
      DINGO_LOG(DEBUG) << "Success send rpc: " << Method() << ", log_id:" << controller.log_id()
                       << " endpoint:" << butil::endpoint2str(controller.remote_side()).c_str() << ", request: \n"
                       << request->DebugString() << ", response:\n"
                       << response->DebugString();
    }

    cb();
  }

  void Reset() override {
    response->Clear();
    controller.Reset();
    controller.set_log_id(butil::fast_rand());
    controller.set_timeout_ms(FLAGS_rpc_time_out_ms);
    controller.set_max_retry(FLAGS_rpc_max_retry);
    status = Status::OK();
  }

  void Call(brpc::Channel* channel, RpcCallback cb) override {
    StubType stub(channel);
    Send(stub, brpc::NewCallback(this, &ClientRpc::OnClientRpcDone, cb));
  }

  virtual void Send(StubType& stub, google::protobuf::Closure* done) = 0;

 protected:
  RequestType* request;
  ResponseType* response;
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_RPC_H_