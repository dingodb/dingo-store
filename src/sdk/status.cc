// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "sdk/status.h"

#include <cstdio>

#include "fmt/core.h"
#include "glog/logging.h"

namespace dingodb {
namespace sdk {
std::unique_ptr<const char[]> Status::CopyState(const char* s) {
  const size_t cch = std::strlen(s) + 1;  // +1 for the null terminator
  char* rv = new char[cch];
  std::strncpy(rv, s, cch);
  return std::unique_ptr<const char[]>(rv);
}

Status::Status(Code code, int32_t p_errno, const Slice& msg, const Slice& msg2) : code_(code), errno_(p_errno) {
  const uint32_t len1 = static_cast<uint32_t>(msg.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);

  char* const result = new char[size + 1];  // +1 for null terminator
  memcpy(result, msg.data(), len1);

  if (len2) {
    result[len1] = ':';
    result[len1 + 1] = ' ';
    memcpy(result + len1 + 2, msg2.data(), len2);
  }
  result[size] = '\0';  // null terminator for C style string
  state_.reset(result);
}

std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code_) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound";
        break;
      case kCorruption:
        type = "Corruption";
        break;
      case kNotSupported:
        type = "Not implemented";
        break;
      case kInvalidArgument:
        type = "Invalid argument";
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
      case kTxnLockConflict:
        type = "TxnLockConflict";
        break;
      case kTxnWriteConflict:
        type = "TxnWriteConflict";
        break;
      case kTxnNotFound:
        type = "TxnNotFound";
        break;
      case kTxnPrimaryMismatch:
        type = "TxnPrimaryMismatch";
        break;
      case kTxnRolledBack:
        type = "TxnRolledBack";
        break;
      default:
        std::string tmp = fmt::format("Unknown code({}):", static_cast<int>(code_));
        CHECK(false) << tmp;
    }

    std::string result(type);
    if (errno_ != kNone) {
      result.append(fmt::format(" (errno:{}) ", errno_));
    }

    if (state_ != nullptr) {
      result.append(": ");
      result.append(state_.get());
    }

    return result;
  }
}

}  // namespace sdk
}  // namespace dingodb
