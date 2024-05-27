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

#ifndef DINGODB_SDK_GRPC_UNARY_RPC_H_
#define DINGODB_SDK_GRPC_UNARY_RPC_H_

#include <sys/stat.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "grpcpp/client_context.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/support/async_unary_call.h"
#include "grpcpp/support/status.h"
#include "grpcpp/support/stub_options.h"
#include "sdk/rpc/rpc.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

struct GrpcContext : public RpcContext {
  GrpcContext() = default;
  ~GrpcContext() override = default;

  std::shared_ptr<grpc::Channel> channel;
  grpc::CompletionQueue* cq;
};

template <class RequestType, class ResponseType, class ServiceType, class StubType>
class UnaryRpc : public Rpc {
 public:
  UnaryRpc(const std::string& cmd) : Rpc(cmd) {
    request = new RequestType;
    response = new ResponseType;
    context = std::make_unique<grpc::ClientContext>();
  }

  ~UnaryRpc() override {
    delete request;
    delete response;
    delete grpc_ctx;
  }

  RequestType* MutableRequest() { return request; }

  const RequestType* Request() const { return request; }

  ResponseType* MutableResponse() { return response; }

  const ResponseType* Response() const { return response; }

  google::protobuf::Message* RawMutableRequest() override { return request; }

  const google::protobuf::Message* RawRequest() const override { return request; }

  google::protobuf::Message* RawMutableResponse() override { return response; }

  const google::protobuf::Message* RawResponse() const override { return response; }

  std::string ServiceName() override { return ServiceType::service_full_name(); }

  std::string ServiceFullName() override { return ServiceType::service_full_name(); }

  grpc::ClientContext* MutableContext() { return context.get();}

  const grpc::ClientContext* Context() const { return context.get(); }

  uint64_t LogId() const override { return -1; }

  void OnRpcDone() override {
    if (!grpc_status.ok()) {
      DINGO_LOG(WARNING) << "Fail send rpc: " << Method() << " endpoint(peer):" << context->peer()
                         << " grpc error_code:" << grpc_status.error_code()
                         << " error_text:" << grpc_status.error_message();
      Status err = Status::NetworkError(grpc_status.error_code(), grpc_status.error_message());
      SetStatus(err);
    } else {
      DINGO_LOG(DEBUG) << "Success send rpc: " << Method() << " endpoint(peer):" << context->peer() << ", request: \n"
                       << request->DebugString() << ", response:\n"
                       << response->DebugString();
    }

    grpc_ctx->cb();
  }

  void Reset() override {
    response->Clear();
    grpc_status = grpc::Status();
    status = Status::OK();
    context->TryCancel();
    context = std::make_unique<grpc::ClientContext>();
  }

  virtual std::unique_ptr<grpc::ClientAsyncResponseReader<ResponseType>> Prepare(StubType* stub,
                                                                                 grpc::CompletionQueue* cq) = 0;

  void Call(RpcContext* ctx) override {
    grpc_ctx = dynamic_cast<GrpcContext*>(ctx);
    CHECK_NOTNULL(grpc_ctx);
    CHECK_NOTNULL(grpc_ctx->channel);
    CHECK_NOTNULL(grpc_ctx->cq);
    grpc::StubOptions options;
    // TODO: cache stub
    stub = std::move(ServiceType::NewStub(grpc_ctx->channel));
    auto reader = Prepare(stub.get(), grpc_ctx->cq);
    reader->Finish(response, &grpc_status, (void*)this);
  }

 protected:
  RequestType* request;
  ResponseType* response;
  std::unique_ptr<grpc::ClientContext> context;
  grpc::Status grpc_status;
  std::unique_ptr<StubType> stub;
  GrpcContext* grpc_ctx;
};

#define DECLARE_UNARY_RPC(NS, SERVICE, METHOD)                                                       \
  class METHOD##Rpc final                                                                            \
      : public UnaryRpc<NS::METHOD##Request, NS::METHOD##Response, NS::SERVICE, NS::SERVICE::Stub> { \
   public:                                                                                           \
    METHOD##Rpc(const METHOD##Rpc&) = delete;                                                        \
    METHOD##Rpc& operator=(const METHOD##Rpc&) = delete;                                             \
    explicit METHOD##Rpc();                                                                          \
    explicit METHOD##Rpc(const std::string& cmd);                                                    \
    ~METHOD##Rpc() override;                                                                         \
    std::string Method() const override { return ConstMethod(); }                                    \
    std::unique_ptr<grpc::ClientAsyncResponseReader<NS::METHOD##Response>> Prepare(                  \
        NS::SERVICE::Stub* stub, grpc::CompletionQueue* cq) override;                                \
    static std::string ConstMethod();                                                                \
  };

#define DEFINE_UNAEY_RPC(NS, SERVICE, METHOD)                                                  \
  METHOD##Rpc::METHOD##Rpc() : METHOD##Rpc("") {}                                              \
  METHOD##Rpc::METHOD##Rpc(const std::string& cmd) : UnaryRpc(cmd) {}                          \
  METHOD##Rpc::~METHOD##Rpc() = default;                                                       \
  std::unique_ptr<grpc::ClientAsyncResponseReader<NS::METHOD##Response>> METHOD##Rpc::Prepare( \
      NS::SERVICE::Stub* stub, grpc::CompletionQueue* cq) {                                    \
    return stub->Async##METHOD(MutableContext(), *request, cq);                                \
  }                                                                                            \
  std::string METHOD##Rpc::ConstMethod() { return fmt::format("{}.{}Rpc", NS::SERVICE::service_full_name(), #METHOD); }

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_GRPC_UNARY_RPC_H_