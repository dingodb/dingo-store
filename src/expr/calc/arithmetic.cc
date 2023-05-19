#include "arithmetic.h"

namespace dingodb::expr {

IMPLEMENT_INVALID_UNARY_OP(CalcPos, std::string)

IMPLEMENT_INVALID_UNARY_OP(CalcNeg, std::string)

IMPLEMENT_INVALID_BINARY_OP(CalcAdd, std::string)

IMPLEMENT_INVALID_BINARY_OP(CalcSub, std::string)

IMPLEMENT_INVALID_BINARY_OP(CalcMul, std::string)

IMPLEMENT_INVALID_BINARY_OP(CalcDiv, std::string)

IMPLEMENT_INVALID_BINARY_OP(CalcMod, float)

IMPLEMENT_INVALID_BINARY_OP(CalcMod, double)

IMPLEMENT_INVALID_BINARY_OP(CalcMod, std::string)

}  // namespace dingodb::expr
