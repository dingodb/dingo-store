set(LIB_NAME dingo_expr)

add_library(${LIB_NAME} STATIC
    calc/arithmetic.cc
    calc/special.cc
    codec.cc
    operator_vector.cc
)
