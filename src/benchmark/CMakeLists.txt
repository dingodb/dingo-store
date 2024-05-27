enable_testing()

SET(BENCHMARK_BIN "dingodb_bench")


file(GLOB BENCHMARK_SRCS "*.cc")

include_directories(${CMAKE_SOURCE_DIR}/test/benchmark)

add_executable(${BENCHMARK_BIN}
                main.cc
                ${BENCHMARK_SRCS}

                ${PROJECT_SOURCE_DIR}/src/common/helper.cc
                ${PROJECT_SOURCE_DIR}/src/common/role.cc
                ${PROJECT_SOURCE_DIR}/src/coordinator/coordinator_interaction.cc
                ${PROJECT_SOURCE_DIR}/src/common/service_access.cc
                ${PROJECT_SOURCE_DIR}/src/common/threadpool.cc
                ${PROJECT_SOURCE_DIR}/src/common/synchronization.cc
                )

add_dependencies(${BENCHMARK_BIN} sdk fmt glog hdf5 hdf5_cpp)

target_link_libraries(${BENCHMARK_BIN}
                      PRIVATE
                      $<TARGET_OBJECTS:PROTO_OBJS>
                      sdk
                      hdf5_cpp
                      hdf5
                      )