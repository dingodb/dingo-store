#!/usr/bin/env bash
set -euo pipefail

# Eureka images predate the raft_meta_force_no_sync implementation from
# dingodb/braft#3. Rebuild the real library using Eureka's ABI and dependencies.
readonly braft_revision=0f61451cfaf1f5854356d88f887469addf95e34b
readonly eureka_root="${1:?Usage: bash scripts/ci_build_braft.sh <eureka-install-path>}"
work_dir=$(mktemp -d "$PWD/.ci-braft.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
source_dir="$work_dir/source"
build_dir="$work_dir/build"

git init "$source_dir"
git -C "$source_dir" remote add origin https://github.com/dingodb/braft.git
git -C "$source_dir" fetch --depth 1 origin "$braft_revision"
git -C "$source_dir" checkout --detach "$braft_revision"

cmake -S "$source_dir" -B "$build_dir" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD=17 \
  "-DCMAKE_CXX_FLAGS=-DUSE_BTHREAD_MUTEX -iquote$source_dir/src -iquote$build_dir" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_PREFIX_PATH="$eureka_root" \
  -DCMAKE_LIBRARY_PATH="$eureka_root/lib" \
  -DBRPC_WITH_GLOG=ON \
  -DWITH_DEBUG_SYMBOLS=OFF \
  -DBUILD_UNIT_TESTS=OFF \
  -DBRPC_INCLUDE_PATH="$eureka_root/include" \
  -DBRPC_LIB="$eureka_root/lib/libbrpc.a" \
  -DGFLAGS_INCLUDE_PATH="$eureka_root/include" \
  -DGFLAGS_LIB="$eureka_root/lib/libgflags.a" \
  -DGLOG_INCLUDE_PATH="$eureka_root/include" \
  -DGLOG_LIB="$eureka_root/lib/libglog.a" \
  -DLEVELDB_INCLUDE_PATH="$eureka_root/include" \
  -DLEVELDB_LIB="$eureka_root/lib/libleveldb.a" \
  -DPROTOBUF_INCLUDE_DIR="$eureka_root/include" \
  -DPROTOBUF_LIBRARY="$eureka_root/lib/libprotobuf.a" \
  -DPROTOBUF_PROTOC_EXECUTABLE="$eureka_root/bin/protoc" \
  -DOPENSSL_ROOT_DIR="$eureka_root" \
  -DOPENSSL_INCLUDE_DIR="$eureka_root/include" \
  -DOPENSSL_SSL_LIBRARY="$eureka_root/lib/libssl.a" \
  -DOPENSSL_CRYPTO_LIBRARY="$eureka_root/lib/libcrypto.a"
cmake --build "$build_dir" --target braft-static --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}"

# Match Eureka's static-library installation; dingo-store prefers .a libraries.
install -m 644 "$build_dir/output/lib/libbraft.a" "$eureka_root/lib/libbraft.a"
cp -r "$build_dir/output/include/braft" "$eureka_root/include/"
