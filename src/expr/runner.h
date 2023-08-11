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

#ifndef DINGODB_EXPR_RUNNER_H_
#define DINGODB_EXPR_RUNNER_H_

#include "operand_stack.h"
#include "operator_vector.h"

namespace dingodb::expr
{

class Runner
{
public:
    Runner() : m_operandStack(), m_operatorVector()
    {
    }

    virtual ~Runner()
    {
    }

    void Decode(const byte *code, size_t len)
    {
        m_operatorVector.Decode(code, len);
    }

    Operand RunAny(const Tuple *tuple = nullptr)
    {
        RunInternal(tuple);
        return m_operandStack.PopAny();
    }

    template <typename T> wrap<T> Run(const Tuple *tuple = nullptr)
    {
        RunInternal(tuple);
        return m_operandStack.Pop<T>();
    }

private:
    OperandStack m_operandStack;
    OperatorVector m_operatorVector;

    void RunInternal(const Tuple *tuple)
    {
        m_operandStack.BindTuple(tuple);
        for (auto op : m_operatorVector) {
            op(m_operandStack);
        }
    }
};

} // namespace dingodb::expr

#endif // DINGODB_EXPR_RUNNER_H_
