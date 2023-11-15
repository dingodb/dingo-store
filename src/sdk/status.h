// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#ifndef DINGODB_SDK_STATUS_H_
#define DINGODB_SDK_STATUS_H_

#include <algorithm>
#include <string>

#include "common/slice.h"

namespace dingodb {
namespace sdk {

// TODO:: add sub code
class Status {
 public:
  // Create a success status.
  Status() noexcept : state_(nullptr) {}
  ~Status() { delete[] state_; }

  Status(const Status& rhs);
  Status& operator=(const Status& rhs);

  Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }

  Status& operator=(Status&& rhs) noexcept;

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kNotFound, msg, msg2); }

  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kCorruption, msg, msg2); }

  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kNotSupported, msg, msg2); }

  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2);
  }

  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kIOError, msg, msg2); }

  static Status AlreadyPresent(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kAlreadyPresent, msg, msg2);
  }

  static Status RuntimeError(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kRuntimeError, msg, msg2); }

  static Status NetworkError(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kNetworkError, msg, msg2); }
  static Status IllegalState(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kIllegalState, msg, msg2); }

  static Status NotAuthorized(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotAuthorized, msg, msg2);
  }

  static Status Aborted(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kAborted, msg, msg2); }

  static Status RemoteError(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kRemoteError, msg, msg2); }

  static Status ServiceUnavailable(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kServiceUnavailable, msg, msg2);
  }

  static Status TimedOut(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kTimedOut, msg, msg2); }

  static Status Uninitialized(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kUninitialized, msg, msg2);
  }

  static Status ConfigurationError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kConfigurationError, msg, msg2);
  }

  static Status Incomplete(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kIncomplete, msg, msg2); }

  static Status NotLeader(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kNotLeader, msg, msg2); }

  // Returns true iff the status indicates success.
  bool IsOK() const {
    return (state_ == nullptr);
  }

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const { return GetCode() == kNotFound; }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const { return GetCode() == kCorruption; }

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const { return GetCode() == kIOError; }

  // Returns true iff the status indicates a NotSupportedError.
  bool IsNotSupportedError() const { return GetCode() == kNotSupported; }

  // Returns true iff the status indicates an InvalidArgument.
  bool IsInvalidArgument() const { return GetCode() == kInvalidArgument; }

  bool IsAlreadyPresent() const { return GetCode() == kAlreadyPresent; }

  bool IsRuntimeError() const { return GetCode() == kRuntimeError; }

  bool IsNetworkError() const { return GetCode() == kNetworkError; }

  bool IsIllegalState() const { return GetCode() == kIllegalState; }

  bool IsNotAuthorized() const { return GetCode() == kNotAuthorized; }

  bool IsAborted() const { return GetCode() == kAborted; }

  bool IsRemoteError() const { return GetCode() == kRemoteError; }

  bool IsServiceUnavailable() const { return GetCode() == kServiceUnavailable; }

  bool IsTimedOut() const { return GetCode() == kTimedOut; }

  bool IsUninitialized() const { return GetCode() == kUninitialized; }

  bool IsConfigurationError() const { return GetCode() == kConfigurationError; }

  bool IsIncomplete() const { return GetCode() == kIncomplete; }

  bool IsNotLeader() const { return GetCode() == kNotLeader; }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

 private:
  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kAlreadyPresent = 6,
    kRuntimeError = 7,
    kNetworkError = 8,
    kIllegalState = 9,
    kNotAuthorized = 10,
    kAborted = 11,
    kRemoteError = 12,
    kServiceUnavailable = 13,
    kTimedOut = 14,
    kUninitialized = 15,
    kConfigurationError = 16,
    kIncomplete = 17,
    kNotLeader = 18
  };

  Code GetCode() const {
    return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
  }

  Status(Code code, const Slice& msg, const Slice& msg2);
  static const char* CopyState(const char* s);

  // OK status has a null state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
  const char* state_;
};

inline Status::Status(const Status& rhs) { state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_); }

inline Status& Status::operator=(const Status& rhs) {
  if (this != &rhs) {
    delete[] state_;
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
  }
  return *this;
}

inline Status& Status::operator=(Status&& rhs) noexcept {
  std::swap(state_, rhs.state_);
  return *this;
}

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_STATUS_H_