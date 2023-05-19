#include <cstdint>

#include "runner.h"
#include "codec.h"

using namespace dingodb::expr;

void
func_expr_test ()
{
  Runner runner;
  std::string input("\x20\x20");
  auto len = input.size () / 2;
  byte buf[len];
  HexToBytes (buf, input.data (), input.size ());
  runner.Decode (buf, len);
  runner.RunAny ();
}
