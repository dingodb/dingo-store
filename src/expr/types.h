#ifndef DINGODB_EXPR_TYPES_H_
#define DINGODB_EXPR_TYPES_H_

#include <cstdint>

#define TYPE_INT32   0x01
#define TYPE_INT64   0x02
#define TYPE_BOOL    0x03
#define TYPE_FLOAT   0x04
#define TYPE_DOUBLE  0x05
#define TYPE_DECIMAL 0x06
#define TYPE_STRING  0x07

namespace dingodb::expr {

typedef unsigned char byte;

template <int T>
class CxxTraits {};

template <>
class CxxTraits<TYPE_INT32> {
 public:
  typedef int32_t type;
};

template <>
class CxxTraits<TYPE_INT64> {
 public:
  typedef int64_t type;
};

template <>
class CxxTraits<TYPE_BOOL> {
 public:
  typedef bool type;
};

template <>
class CxxTraits<TYPE_FLOAT> {
 public:
  typedef float type;
};

template <>
class CxxTraits<TYPE_DOUBLE> {
 public:
  typedef double type;
};

template <>
class CxxTraits<TYPE_DECIMAL> {
 public:
  typedef std::string type;
};

template <>
class CxxTraits<TYPE_STRING> {
 public:
  typedef std::string type;
};

// TODO: Decimal & String

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_TYPES_H_
