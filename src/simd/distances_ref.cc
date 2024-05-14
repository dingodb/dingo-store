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

/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 */

#include "simd/distances_ref.h"

#include <cmath>
namespace dingodb {

float fvec_L2sqr_ref(const float* x, const float* y, size_t d) {
  size_t i;
  float res = 0;
  for (i = 0; i < d; i++) {
    const float tmp = x[i] - y[i];
    res += tmp * tmp;
  }
  return res;
}

float fvec_L1_ref(const float* x, const float* y, size_t d) {
  size_t i;
  float res = 0;
  for (i = 0; i < d; i++) {
    const float tmp = x[i] - y[i];
    res += std::fabs(tmp);
  }
  return res;
}

float fvec_Linf_ref(const float* x, const float* y, size_t d) {
  size_t i;
  float res = 0;
  for (i = 0; i < d; i++) {
    res = std::fmax(res, std::fabs(x[i] - y[i]));
  }
  return res;
}

float fvec_inner_product_ref(const float* x, const float* y, size_t d) {
  size_t i;
  float res = 0;
  for (i = 0; i < d; i++) res += x[i] * y[i];
  return res;
}

float fvec_norm_L2sqr_ref(const float* x, size_t d) {
  size_t i;
  double res = 0;
  for (i = 0; i < d; i++) res += x[i] * x[i];
  return res;
}

void fvec_L2sqr_ny_ref(float* dis, const float* x, const float* y, size_t d, size_t ny) {
  for (size_t i = 0; i < ny; i++) {
    dis[i] = fvec_L2sqr_ref(x, y, d);
    y += d;
  }
}

void fvec_inner_products_ny_ref(float* ip, const float* x, const float* y, size_t d, size_t ny) {
  for (size_t i = 0; i < ny; i++) {
    ip[i] = fvec_inner_product_ref(x, y, d);
    y += d;
  }
}

void fvec_madd_ref(size_t n, const float* a, float bf, const float* b, float* c) {
  for (size_t i = 0; i < n; i++) c[i] = a[i] + bf * b[i];
}

int fvec_madd_and_argmin_ref(size_t n, const float* a, float bf, const float* b, float* c) {
  float vmin = 1e20;
  int imin = -1;

  for (size_t i = 0; i < n; i++) {
    c[i] = a[i] + bf * b[i];
    if (c[i] < vmin) {
      vmin = c[i];
      imin = i;
    }
  }
  return imin;
}

}  // namespace dingodb
