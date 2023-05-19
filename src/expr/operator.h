#ifndef DINGODB_EXPR_OPERATOR_H_
#define DINGODB_EXPR_OPERATOR_H_

#include <functional>

#include "calc/arithmetic.h"
#include "calc/relational.h"
#include "calc/special.h"
#include "operand_stack.h"

namespace dingodb::expr {

typedef std::function<void(OperandStack &)> Operator;

template <typename T>
class OperatorNull {
 public:
  void operator()(OperandStack &stack) { stack.Push<T>(); }
};

template <typename T>
class OperatorConst {
 public:
  OperatorConst(T value) : m_value(value) {}
  void operator()(OperandStack &stack) { stack.Push(m_value); }

 private:
  T m_value;
};

template <typename T>
class OperatorVarI {
 public:
  OperatorVarI(uint32_t index) : m_index(index) {}
  void operator()(OperandStack &stack) { stack.PushTuple(m_index); }

 private:
  uint32_t m_index;
};

template <typename T, typename R, R (*Calc)(T)>
class UnaryOperator {
 public:
  void operator()(OperandStack &stack) {
    auto v = stack.Get<T>();
    if (v.has_value()) {
      stack.Set<R>(Calc(*v));
    } else {
      stack.Set<R>();
    }
  }
};

template <typename T, typename R, R (*Calc)(const wrap<T> &)>
class UnarySpecialOperator {
 public:
  void operator()(OperandStack &stack) {
    auto v = stack.Get<T>();
    stack.Set<R>(Calc(v));
  }
};

template <typename T, typename R, R (*Calc)(T, T)>
class BinaryOperator {
 public:
  void operator()(OperandStack &stack) {
    auto v1 = stack.Pop<T>();
    auto v0 = stack.Get<T>();
    if (v0.has_value() && v1.has_value()) {
      stack.Set<R>(Calc(*v0, *v1));
    } else {
      stack.Set<R>();
    }
  }
};

template <typename T>
using OperatorPos = UnaryOperator<T, T, CalcPos>;
template <typename T>
using OperatorNeg = UnaryOperator<T, T, CalcNeg>;
template <typename T>
using OperatorAdd = BinaryOperator<T, T, CalcAdd>;
template <typename T>
using OperatorSub = BinaryOperator<T, T, CalcSub>;
template <typename T>
using OperatorMul = BinaryOperator<T, T, CalcMul>;
template <typename T>
using OperatorDiv = BinaryOperator<T, T, CalcDiv>;
template <typename T>
using OperatorMod = BinaryOperator<T, T, CalcMod>;

template <typename T>
using OperatorEq = BinaryOperator<T, bool, CalcEq>;
template <typename T>
using OperatorGe = BinaryOperator<T, bool, CalcGe>;
template <typename T>
using OperatorGt = BinaryOperator<T, bool, CalcGt>;
template <typename T>
using OperatorLe = BinaryOperator<T, bool, CalcLe>;
template <typename T>
using OperatorLt = BinaryOperator<T, bool, CalcLt>;
template <typename T>
using OperatorNe = BinaryOperator<T, bool, CalcNe>;

class OperatorNot {
 public:
  void operator()(OperandStack &stack) {
    auto v = stack.Get<bool>();
    if (v.has_value()) {
      stack.Set<bool>(!*v);
    } else {
      stack.Set<bool>();
    }
  }
};

class OperatorAnd {
 public:
  void operator()(OperandStack &stack) {
    auto v1 = stack.Pop<bool>();
    auto v0 = stack.Get<bool>();
    if (v0.has_value()) {
      if (!*v0) {
        stack.Set<bool>(false);
      } else if (v1.has_value()) {
        stack.Set<bool>(*v1);
      } else {
        stack.Set<bool>();
      }
    } else if (v1.has_value() && !*v1) {
      stack.Set<bool>(false);
    } else {
      stack.Set<bool>();
    }
  }
};

class OperatorOr {
 public:
  void operator()(OperandStack &stack) {
    auto v1 = stack.Pop<bool>();
    auto v0 = stack.Get<bool>();
    if (v0.has_value()) {
      if (*v0) {
        stack.Set<bool>(true);
      } else if (v1.has_value()) {
        stack.Set<bool>(*v1);
      } else {
        stack.Set<bool>();
      }
    } else if (v1.has_value() && *v1) {
      stack.Set<bool>(true);
    } else {
      stack.Set<bool>();
    }
  }
};

template <typename D, typename T>
class OperatorCast {
 public:
  void operator()(OperandStack &stack) {
    auto v = stack.Get<T>();
    if (v.has_value()) {
      stack.Set<D>((D)(*v));
    } else {
      stack.Set<D>();
    }
  }
};

template <typename T>
using OperatorIsNull = UnarySpecialOperator<T, bool, CalcIsNull>;
template <typename T>
using OperatorIsTrue = UnarySpecialOperator<T, bool, CalcIsTrue>;
template <typename T>
using OperatorIsFalse = UnarySpecialOperator<T, bool, CalcIsFalse>;

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_OPERATOR_H_
