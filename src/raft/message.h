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


#ifndef DINGODB_RAFT_MESSAGE_H_
#define DINGODB_RAFT_MESSAGE_H_

#include <memory>

#include "butil/iobuf.h"
#include "proto/store.pb.h"

namespace dingodb {



class Message {
 public:
  // Message type
  enum Type {
    NONE_MESSAGE = 0,

    ADD_REGION_MESSAGE,
    DESTROY_REGION_MESSAGE,

    KV_GET_MESSAGE,
    KV_PUT_MESSAGE,
  };
  Message(Type msg_type, google::protobuf::Message *msg)
    : type_(msg_type),
      msg_(msg) {}
  Message(butil::IOBuf *buf): buf_(buf) {}
  ~Message();

  butil::IOBuf* SerializeToIOBuf();
  void Deserialization();


 private:
  Type type_;
  std::unique_ptr<google::protobuf::Message> msg_;
  std::unique_ptr<butil::IOBuf> buf_;
};



} // namespace dingodb



#endif // DINGODB_RAFT_MESSAGE_H_