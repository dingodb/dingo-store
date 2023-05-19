#ifndef DINGODB_EXPR_OPERANDSTACK_H_
#define DINGODB_EXPR_OPERANDSTACK_H_

#include <stack>

#include "calc/operand.h"

namespace dingodb::expr {

class OperandStack {
 public:
  OperandStack() : m_stack(), m_tuple(nullptr) {}
  virtual ~OperandStack() {}

  Operand PopAny() {
    auto v = m_stack.top();
    m_stack.pop();
    return v;
  }

  template <typename T>
  wrap<T> Pop() {
    return std::any_cast<wrap<T>>(PopAny());
  }

  template <typename T>
  wrap<T> Get() {
    return std::any_cast<wrap<T>>(m_stack.top());
  }

  template <typename T>
  void Push(T v) {
    m_stack.push(Operand(wrap<T>(v)));
  }

  template <typename T>
  void Push() {
    m_stack.push(Operand(wrap<T>()));
  }

  template <typename T>
  void Set(T v) {
    m_stack.top().emplace<wrap<T>>(wrap<T>(v));
  }

  template <typename T>
  void Set() {
    m_stack.top().emplace<wrap<T>>(wrap<T>());
  }

  void BindTuple(const Tuple* tuple) { m_tuple = tuple; }

  void PushTuple(uint32_t index) {
    if (m_tuple != nullptr) {
      m_stack.push((*m_tuple)[index]);
    } else {
      throw std::runtime_error("No tuple provided.");
    }
  }

 private:
  std::stack<Operand> m_stack;
  const Tuple* m_tuple;
};

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_OPERANDSTACK_H_
