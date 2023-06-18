// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#ifndef DINGODB_COMMON_VERSION_H_
#define DINGODB_COMMON_VERSION_H_

#include "gflags/gflags.h"

namespace dingodb {

#ifndef GIT_VERSION
#define GIT_VERSION "unknown"
#endif

#ifndef GIT_TAG_NAME
#define GIT_TAG_NAME "v0.6.0"
#endif

#ifndef DINGO_BUILD_TYPE
#define DINGO_BUILD_TYPE "unknown"
#endif

#ifndef DINGO_CONTRIB_BUILD_TYPE
#define DINGO_CONTRIB_BUILD_TYPE "unknown"
#endif

DECLARE_bool(show_version);

}  // namespace dingodb

#endif  // DINGODB_COMMON_VERSION_H_
