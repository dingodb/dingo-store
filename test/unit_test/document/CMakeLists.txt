include_directories(${CMAKE_CURRENT_SOURCE_DIR}/)

file(GLOB DOCUMENT_UNIT_TEST_SRCS "./*.cc")

add_library(DOCUMENT_UNIT_TEST_OBJS
            OBJECT
            ${DOCUMENT_UNIT_TEST_SRCS}
)

add_dependencies(DOCUMENT_UNIT_TEST_OBJS ${DEPEND_LIBS})