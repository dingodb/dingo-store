FROM rockylinux/rockylinux:8.6

ENV TZ=Asia/Shanghai 
SHELL ["/bin/bash", "-c"]

RUN dnf update -y  \
    && dnf install -y  java-1.8.0-openjdk-devel vim unzip net-tools tzdata wget git gcc gcc-c++ make automake maven openssl openssl-devel  \
    && dnf -y install perl-IPC-Cmd \
    && dnf clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY \
    && wget https://cmake.org/files/v3.25/cmake-3.25.2.tar.gz --no-check-certificate && tar -zxvf cmake-3.25.2.tar.gz && rm -f cmake-3.25.2.tar.gz && cd cmake-3.25.2 && ./bootstrap && gmake -j8 && gmake install && cd .. && rm -rf cmake-3.25.2 


RUN  cd /opt && git clone https://github.com/dingodb/dingo-store.git && cd dingo-store && git submodule update --init --recursive


ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
WORKDIR /opt/dingo-store

