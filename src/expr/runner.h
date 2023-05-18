#ifndef DINGODB_EXPR_RUNNER_H_
#define DINGODB_EXPR_RUNNER_H_

#include "operand_stack.h"
#include "operator_vector.h"

namespace dingodb::expr {

class Runner {
 public:
  Runner() : m_operandStack(), m_operatorVector() {}

  virtual ~Runner() {}

  void Decode(const byte *code, size_t len) { m_operatorVector.Decode(code, len); }

  Operand RunAny(const Tuple *tuple = nullptr) {
    RunInternal(tuple);
    return m_operandStack.PopAny();
  }

  template <typename T>
  wrap<T> Run(const Tuple *tuple = nullptr) {
    RunInternal(tuple);
    return m_operandStack.Pop<T>();
  }

 private:
  OperandStack m_operandStack;
  OperatorVector m_operatorVector;

  void RunInternal(const Tuple *tuple) {
    m_operandStack.BindTuple(tuple);
    for (auto op : m_operatorVector) {
      op(m_operandStack);
    }
  }
};

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_RUNNER_H_
