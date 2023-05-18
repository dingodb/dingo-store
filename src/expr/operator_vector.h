#ifndef DINGODB_EXPR_OPERATORVECTOR_H_
#define DINGODB_EXPR_OPERATORVECTOR_H_

#include <string>
#include <vector>

#include "operator.h"
#include "types.h"

namespace dingodb::expr {

class OperatorVector {
 public:
  OperatorVector() : m_vector() {}
  virtual ~OperatorVector() {}

  void Decode(const byte code[], size_t len);

  auto begin() { return m_vector.begin(); }

  auto end() { return m_vector.end(); }

 private:
  std::vector<Operator> m_vector;

  void Add(const Operator &op) { m_vector.push_back(op); }

  template <template <typename> class OP>
  void AddOperatorByType(byte b);

  void AddCastOperator(byte b);
};

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_OPERATORVECTOR_H_
