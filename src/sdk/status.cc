// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "sdk/status.h"

#include <cstdio>

#include "fmt/core.h"
#include "glog/logging.h"

namespace dingodb {
namespace sdk {

const char* Status::CopyState(const char* state) {
  uint32_t size;
  std::memcpy(&size, state, sizeof(size));
  char* result = new char[size + 5];
  std::memcpy(result, state, size + 5);
  return result;
}

Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  assert(code != kOk);
  const uint32_t len1 = static_cast<uint32_t>(msg.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char* result = new char[size + 5];
  std::memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  std::memcpy(result + 5, msg.data(), len1);
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    std::memcpy(result + 7 + len1, msg2.data(), len2);
  }
  state_ = result;
}

std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (GetCode()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      case kAlreadyPresent:
        type = "Already present";
        break;
      case kRuntimeError:
        type = "Runtime error";
        break;
      case kNetworkError:
        type = "Network error";
        break;
      case kIllegalState:
        type = "Illegal state";
        break;
      case kNotAuthorized:
        type = "Not authorized";
        break;
      case kAborted:
        type = "Aborted";
        break;
      case kRemoteError:
        type = "Remote error";
        break;
      case kServiceUnavailable:
        type = "Service unavailable";
        break;
      case kTimedOut:
        type = "Timed out";
        break;
      case kUninitialized:
        type = "Uninitialized";
        break;
      case kConfigurationError:
        type = "Configuration error";
        break;
      case kIncomplete:
        type = "Incomplete";
        break;
      case kNotLeader:
        type = "NotLeader";
        break;
      default:
        std::string tmp = fmt::format("Unknown code({}):", static_cast<int>(GetCode()));
        CHECK(false) << tmp;
    }

    std::string result(type);
    uint32_t length;
    std::memcpy(&length, state_, sizeof(length));
    if(length) {
      result.append(": ");
    }
    result.append(state_ + 5, length);
    return result;
  }
}

}  // namespace sdk
}  // namespace dingodb
