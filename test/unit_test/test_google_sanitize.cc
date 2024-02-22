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

#include <gtest/gtest.h>

#include <iostream>

#include "glog/logging.h"

class GoogleSanitizeTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// (heap) use after free
TEST(GoogleSanitizeTest, HeapUseAfterFree) {
#if 0  // NOLINT
  int* array = new int[100];
  delete[] array;
  // cppcheck-suppress deallocuse
  LOG(INFO) << array[1] << std::endl;  // BOOM
#endif
}

// heap buffer overflow
TEST(GoogleSanitizeTest, HeapBufferOverflow) {
#if 0  // NOLINT
  int* array = new int[100];
  int res = array[100];
  delete[] array;
  LOG(INFO) << "res : " << res << std::endl;

#endif
}

// stack buffer overflow
TEST(GoogleSanitizeTest, StackBufferOverflow) {
#if 0  // NOLINT
  int array[100];
  LOG(INFO) << "array[100] : " << array[100] << std::endl;
#endif
}

// global buffer overflow
#if 0  // NOLINT
int global_buffer_overflow_array[100];

TEST(GoogleSanitizeTest, GlobalBufferOverflow) {
  LOG(INFO) << "global_buffer_overflow_array : " << global_buffer_overflow_array[100] << std::endl;
}
#endif

// bad
// use after return
#if 0  // NOLINT

int *ptr = nullptr;

__attribute__((noinline))
void FunctionThatEscapesLocalObject() {
  int local[100] = {1};
  ptr = &local[0];
}

TEST(GoogleSanitizeTest, UseAfterReturn) {
  FunctionThatEscapesLocalObject();
  LOG(INFO) << ptr[1000] << std::endl;
}

#endif

// Use After Scope
#if 0  // NOLINT

TEST(GoogleSanitizeTest, UseAfterScope) {
  volatile int *p = 0;

  auto lamdab_main = [&p]() {
    {
      int x = 0;
      p = &x;
    }
    *p = 5;
    return 0;
  };

  lamdab_main();
}
#endif

// bad
// AddressSanitizerInitializationOrderFiasco
#if 0  // NOLINT

int extern_global = 0;

int __attribute__((noinline)) ReadExternGlobal() { return extern_global; }

int Foo() { return 42; }

TEST(GoogleSanitizeTest, AddressSanitizerInitializationOrderFiasco) {
  int x = ReadExternGlobal() + 1;

  LOG(INFO) << "x : " << x << std::endl;

  int extern_global = Foo();

  LOG(INFO) << "extern_global : " << extern_global << std::endl;
}

#endif

// memory leaks
TEST(GoogleSanitizeTest, MemoryLeaks) {
#if 10  // NOLINT
  int *array = new int[100];
  LOG(INFO) << array[1];  // BOOM
#endif
}