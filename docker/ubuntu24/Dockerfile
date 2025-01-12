# FROM ubuntu:24.04
FROM dingodatabase/dingo-eureka:ubuntu2404

ENV TZ=Asia/Shanghai \
    DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

RUN apt-get update \
    && apt-get install -y openjdk-8-jdk vim unzip  net-tools tzdata wget git gcc g++ make automake maven openssl libssl-dev cmake libtool gpg netcat-traditional libboost-dev libboost-program-options-dev libaio-dev  \
    && wget -O- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB | gpg --dearmor |  tee /usr/share/keyrings/oneapi-archive-keyring.gpg > /dev/null \
    && echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" | tee /etc/apt/sources.list.d/oneAPI.list \
    && apt-get update && apt-get install -y intel-oneapi-mkl-devel-2024.1  \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && wget https://cmake.org/files/v3.25/cmake-3.25.2.tar.gz --no-check-certificate && tar -zxvf cmake-3.25.2.tar.gz && rm -f cmake-3.25.2.tar.gz && cd cmake-3.25.2 && ./bootstrap && make -j$(nproc) && make install && cd .. && rm -rf cmake-3.25.2

# install rust
# RUN wget https://sh.rustup.rs --content-disposition
# RUN sh rustup-init.sh -y
RUN wget https://static.rust-lang.org/dist/rust-1.78.0-x86_64-unknown-linux-gnu.tar.xz && xz -d rust-1.78.0-x86_64-unknown-linux-gnu.tar.xz && tar -xf rust-1.78.0-x86_64-unknown-linux-gnu.tar && cd rust-1.78.0-x86_64-unknown-linux-gnu && sh install.sh --prefix=/opt/rust/ && cd .. && rm -rf rust-1.78.0-x86_64-unknown-linux-gnu*

# soure env
RUN echo "source /opt/intel/oneapi/setvars.sh" >> /root/.bashrc

ENV PATH=/opt/rust/bin:/root/.cargo/bin:/opt/intel/oneapi:$PATH

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

WORKDIR /opt
