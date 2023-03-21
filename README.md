[![CMake_ubuntu](https://github.com/dingodb/dingo-store/actions/workflows/ci_ubuntu.yml/badge.svg)](https://github.com/dingodb/dingo-store/actions/workflows/ci_ubuntu.yml)
[![CMake_rocky8.6](https://github.com/dingodb/dingo-store/actions/workflows/ci_rocky.yml/badge.svg)](https://github.com/dingodb/dingo-store/actions/workflows/ci_rocky.yml)
[![CMake_centos7](https://github.com/dingodb/dingo-store/actions/workflows/ci_centos.yml/badge.svg)](https://github.com/dingodb/dingo-store/actions/workflows/ci_centos.yml)
[![Java_Maven_Build](https://github.com/dingodb/dingo-store/actions/workflows/java_build.yml/badge.svg)](https://github.com/dingodb/dingo-store/actions/workflows/java_build.yml)
[![Maven_Publish_package](https://github.com/dingodb/dingo-store/actions/workflows/java_package.yml/badge.svg)](https://github.com/dingodb/dingo-store/actions/workflows/java_package.yml)

# What's Dingo-Store?

The Dingo-Store project is a distributed KV storage system based on multiple Raft replication groups, which also provides storage layer computation offloading capability. The upper-layer service of this project is DingoDB based on SQL, and it can also provide high-frequency serving storage capability based on KV. The overall architecture of the project is as follows:

# How to build

Dingo-Store is a hybrid project of C++ and Java, where C++ provides distributed storage and computing capabilities, while the Java layer provides basic API interfaces.

## For C++

```shell
git submodule update --init --recursive
mkdir build && cd build && cmake .. && make -j8
```

## For Java


```java
cd java && mvn clean package -DskipTests
```

