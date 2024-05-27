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

#ifndef DINGODB_SDK_BRPC_UNARY_RPC_H_
#define DINGODB_SDK_BRPC_UNARY_RPC_H_

#include <sys/stat.h>

#include <cstdint>
#include <string>

#include "brpc/callback.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "butil/fast_rand.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "sdk/common/param_config.h"
#include "sdk/rpc/rpc.h"
#include "sdk/status.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

struct BrpcContext : public RpcContext {
  BrpcContext() = default;
  ~BrpcContext() override = default;

  std::shared_ptr<brpc::Channel> channel;
};

template <class RequestType, class ResponseType, class ServiceType, class StubType>
class UnaryRpc : public Rpc {
 public:
  UnaryRpc(const std::string& cmd) : Rpc(cmd) {
    request = new RequestType;
    response = new ResponseType;
    brpc_ctx = nullptr;
  }

  ~UnaryRpc() override {
    delete request;
    delete response;
    delete brpc_ctx;
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

  brpc::Controller* MutableController() { return &controller; }

  const brpc::Controller* Controller() const { return &controller; }

  uint64_t LogId() const override { return controller.log_id(); }

  void OnRpcDone() override {
    if (controller.Failed()) {
      DINGO_LOG(WARNING) << "Fail send rpc: " << Method() << ", log_id:" << controller.log_id()
                         << " endpoint:" << endpoint2str(controller.remote_side()).c_str()
                         << " error_code:" << controller.ErrorCode() << " error_text:" << controller.ErrorText();

      Status err = Status::NetworkError(controller.ErrorCode(), controller.ErrorText());
      SetStatus(err);
    } else {
      DINGO_LOG(DEBUG) << "Success send rpc: " << Method() << ", log_id:" << controller.log_id()
                       << " endpoint:" << endpoint2str(controller.remote_side()).c_str() << ", request: \n"
                       << request->DebugString() << ", response:\n"
                       << response->DebugString();
    }

    brpc_ctx->cb();
  }

  void Reset() override {
    response->Clear();
    controller.Reset();
    controller.set_log_id(butil::fast_rand());
    controller.set_timeout_ms(FLAGS_rpc_time_out_ms);
    controller.set_max_retry(FLAGS_rpc_max_retry);
    status = Status::OK();
  }

  // virtual void Call(RpcContext* ctx) = 0;
  // void Call(void* channel, RpcCallback cb, void* cq) override {
  void Call(RpcContext* ctx) override {
    brpc_ctx = dynamic_cast<BrpcContext*>(ctx);
    CHECK_NOTNULL(brpc_ctx);
    CHECK_NOTNULL(brpc_ctx->channel);
    StubType stub(brpc_ctx->channel.get());
    Send(stub, brpc::NewCallback(this, &UnaryRpc::OnRpcDone));
  }

  virtual void Send(StubType& stub, google::protobuf::Closure* done) = 0;

 protected:
  RequestType* request;
  ResponseType* response;
  brpc::Controller controller;
  BrpcContext* brpc_ctx;
};

#define DECLARE_UNARY_RPC(NS, SERVICE, METHOD)                                                        \
  class METHOD##Rpc final                                                                             \
      : public UnaryRpc<NS::METHOD##Request, NS::METHOD##Response, NS::SERVICE, NS::SERVICE##_Stub> { \
   public:                                                                                            \
    METHOD##Rpc(const METHOD##Rpc&) = delete;                                                         \
    METHOD##Rpc& operator=(const METHOD##Rpc&) = delete;                                              \
    explicit METHOD##Rpc();                                                                           \
    explicit METHOD##Rpc(const std::string& cmd);                                                     \
    ~METHOD##Rpc() override;                                                                          \
    std::string Method() const override { return ConstMethod(); }                                     \
    void Send(NS::SERVICE##_Stub& stub, google::protobuf::Closure* done) override;                    \
    static std::string ConstMethod();                                                                 \
  };

#define DEFINE_UNAEY_RPC(NS, SERVICE, METHOD)                                         \
  METHOD##Rpc::METHOD##Rpc() : METHOD##Rpc("") {}                                     \
  METHOD##Rpc::METHOD##Rpc(const std::string& cmd) : UnaryRpc(cmd) {}                 \
  METHOD##Rpc::~METHOD##Rpc() = default;                                              \
  void METHOD##Rpc::Send(NS::SERVICE##_Stub& stub, google::protobuf::Closure* done) { \
    stub.METHOD(MutableController(), request, response, done);                        \
  }                                                                                   \
  std::string METHOD##Rpc::ConstMethod() { return fmt::format("{}.{}Rpc", NS::SERVICE::descriptor()->name(), #METHOD); }

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_BRPC_UNARY_RPC_H_