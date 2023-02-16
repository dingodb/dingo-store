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

#ifndef DINGODB_COMMON_CONTEXT_H_
#define DINGODB_COMMON_CONTEXT_H_

#include "brpc/controller.h"

#include "proto/store.pb.h"

class Context {
 public:
  Context(): cntl_(nullptr), done_(nullptr) {}
  Context(brpc::Controller* cntl, google::protobuf::Closure* done): cntl_(cntl), done_(done) {}
  ~Context(){}

  brpc::Controller* get_cntl() { return cntl_; }
  Context& set_cntl(brpc::Controller* cntl) {
    cntl_ = cntl;
    return *this; 
  }

  google::protobuf::Closure* get_done() { return done_; }
  Context& set_done(google::protobuf::Closure* done) {
    done_ = done;
    return *this;
  }

 private:
  brpc::Controller* cntl_;
  google::protobuf::Closure* done_;
};

#endif