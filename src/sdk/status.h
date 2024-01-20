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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "common/slice.h"

/// @brief Return the given status if it is not @c OK.
#define DINGO_RETURN_NOT_OK(s)              \
  do {                                      \
    const ::dingodb::sdk::Status& _s = (s); \
    if (!_s.IsOK()) return _s;              \
  } while (0)

#define DECLARE_ERROR_STATUS(NAME, CODE)                                                                        \
  static Status NAME(const Slice& msg, const Slice& msg2 = Slice()) { return Status(CODE, kNone, msg, msg2); }; \
  static Status NAME(int32_t p_errno, const Slice& msg, const Slice& msg2 = Slice()) {                          \
    return Status(CODE, p_errno, msg, msg2);                                                                    \
  }                                                                                                             \
  bool Is##NAME() const { return code_ == (CODE); }

static const int32_t kNone = 0;
namespace dingodb {
namespace sdk {

class Status {
 public:
  // Create a success status.
  Status() noexcept : code_(kOk), errno_(kNone), state_(nullptr) {}
  ~Status() = default;

  Status(const Status& rhs);
  Status& operator=(const Status& rhs);

  Status(Status&& rhs) noexcept;
  Status& operator=(Status&& rhs) noexcept;

  bool ok() const { return code_ == kOk; }  // NOLINT
  static Status OK() { return Status(); }

  DECLARE_ERROR_STATUS(OK, kOk);
  DECLARE_ERROR_STATUS(NotFound, kNotFound);
  DECLARE_ERROR_STATUS(Corruption, kCorruption);
  DECLARE_ERROR_STATUS(NotSupported, kNotSupported);
  DECLARE_ERROR_STATUS(InvalidArgument, kInvalidArgument);
  DECLARE_ERROR_STATUS(IOError, kIOError);
  DECLARE_ERROR_STATUS(AlreadyPresent, kAlreadyPresent);
  DECLARE_ERROR_STATUS(RuntimeError, kRuntimeError);
  DECLARE_ERROR_STATUS(NetworkError, kNetworkError);
  DECLARE_ERROR_STATUS(IllegalState, kIllegalState);
  DECLARE_ERROR_STATUS(NotAuthorized, kNotAuthorized);
  DECLARE_ERROR_STATUS(Aborted, kAborted);
  DECLARE_ERROR_STATUS(RemoteError, kRemoteError);
  DECLARE_ERROR_STATUS(ServiceUnavailable, kServiceUnavailable);
  DECLARE_ERROR_STATUS(TimedOut, kTimedOut);
  DECLARE_ERROR_STATUS(Uninitialized, kUninitialized);
  DECLARE_ERROR_STATUS(ConfigurationError, kConfigurationError);
  DECLARE_ERROR_STATUS(Incomplete, kIncomplete);
  DECLARE_ERROR_STATUS(NotLeader, kNotLeader);
  DECLARE_ERROR_STATUS(TxnLockConflict, kTxnLockConflict);
  DECLARE_ERROR_STATUS(TxnWriteConflict, kTxnWriteConflict);
  DECLARE_ERROR_STATUS(TxnNotFound, kTxnNotFound);
  DECLARE_ERROR_STATUS(TxnPrimaryMismatch, kTxnPrimaryMismatch);
  DECLARE_ERROR_STATUS(TxnRolledBack, kTxnRolledBack);

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  int32_t Errno() const { return errno_; }

 private:
  enum Code : uint8_t {
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
    kNotLeader = 18,
    kTxnLockConflict = 19,
    kTxnWriteConflict = 20,
    kTxnNotFound = 21,
    kTxnPrimaryMismatch = 22,
    kTxnRolledBack = 23,
  };

  Status(Code code, int32_t p_errno, const Slice& msg, const Slice& msg2);

  static std::unique_ptr<const char[]> CopyState(const char* s);

  Code code_;
  int32_t errno_;
  // A nullptr state_ (which is at least the case for OK) means the extra message is empty.
  std::unique_ptr<const char[]> state_;
};

inline Status::Status(const Status& rhs) : code_(rhs.code_), errno_(rhs.errno_) {
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_.get());
}

inline Status& Status::operator=(const Status& rhs) {
  if (this != &rhs) {
    code_ = rhs.code_;
    errno_ = rhs.errno_;
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_.get());
  }
  return *this;
}

inline Status::Status(Status&& rhs) noexcept : Status() { *this = std::move(rhs); }

inline Status& Status::operator=(Status&& rhs) noexcept {
  if (this != &rhs) {
    code_ = rhs.code_;
    errno_ = rhs.errno_;
    state_ = std::move(rhs.state_);
  }
  return *this;
}

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_STATUS_H_