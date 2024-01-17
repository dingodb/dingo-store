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

#ifndef DINGODB_BENCHMARK_COLOR_H_
#define DINGODB_BENCHMARK_COLOR_H_

#define COLOR_RESET "\033[0m"
#define COLOR_BLACK "\033[30m"               // Black
#define COLOR_RED "\033[31m"                 // Red
#define COLOR_GREEN "\033[32m"               // Green
#define COLOR_YELLOW "\033[33m"              // Yellow
#define COLOR_BLUE "\033[34m"                // Blue
#define COLOR_MAGENTA "\033[35m"             // Magenta
#define COLOR_CYAN "\033[36m"                // Cyan
#define COLOR_WHITE "\033[37m"               // White
#define COLOR_BOLDBLACK "\033[1m\033[30m"    // Bold Black
#define COLOR_BOLDRED "\033[1m\033[31m"      // Bold Red
#define COLOR_BOLDGREEN "\033[1m\033[32m"    // Bold Green
#define COLOR_BOLDYELLOW "\033[1m\033[33m"   // Bold Yellow
#define COLOR_BOLDBLUE "\033[1m\033[34m"     // Bold Blue
#define COLOR_BOLDMAGENTA "\033[1m\033[35m"  // Bold Magenta
#define COLOR_BOLDCYAN "\033[1m\033[36m"     // Bold Cyan
#define COLOR_BOLDWHITE "\033[1m\033[37m"    // Bold White

// example
// std::cout << COLOR_RED << "hello world" << COLOR_RESET << std::endl;

#endif  // DINGODB_BENCHMARK_COLOR_H_