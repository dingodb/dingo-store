#ifndef DINGODB_EXPR_CALC_OPERAND_H_
#define DINGODB_EXPR_CALC_OPERAND_H_

#include <any>
#include <optional>
#include <vector>

namespace dingodb::expr {

template <typename T>
using wrap = std::optional<T>;

typedef std::any Operand;

typedef std::vector<Operand> Tuple;

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_CALC_OPERAND_H_
