#ifndef DINGODB_EXPR_CALC_CAMPARISON_H_
#define DINGODB_EXPR_CALC_CAMPARISON_H_

template <typename T>
bool CalcEq(T v0, T v1) {
  return v0 == v1;
}

template <typename T>
bool CalcGe(T v0, T v1) {
  return v0 >= v1;
}

template <typename T>
bool CalcGt(T v0, T v1) {
  return v0 > v1;
}

template <typename T>
bool CalcLe(T v0, T v1) {
  return v0 <= v1;
}

template <typename T>
bool CalcLt(T v0, T v1) {
  return v0 < v1;
}

template <typename T>
bool CalcNe(T v0, T v1) {
  return v0 != v1;
}

#endif  // DINGODB_EXPR_CALC_CAMPARISON_H_
