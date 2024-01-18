FROM ubuntu:18.04

ENV TZ=Asia/Shanghai \
    DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

RUN apt-get update  \
    && apt-get install -y openjdk-8-jdk vim unzip netcat net-tools tzdata wget git gcc g++ make automake maven openssl libssl-dev libtool \
    && apt-get install -y software-properties-common  \
    && add-apt-repository -y ppa:ubuntu-toolchain-r/test  \
    && apt-get update  \
    && apt-get install -y gcc-11 g++-11  \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100 --slave /usr/bin/g++ g++ /usr/bin/g++-11 \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && wget https://cmake.org/files/v3.25/cmake-3.25.2.tar.gz --no-check-certificate && tar -zxvf cmake-3.25.2.tar.gz && rm -f cmake-3.25.2.tar.gz && cd cmake-3.25.2 && ./bootstrap && make -j$(nproc) && make install && cd .. && rm -rf cmake-3.25.2 

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

WORKDIR /opt
