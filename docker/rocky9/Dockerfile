FROM rockylinux/rockylinux:9.3

ENV TZ=Asia/Shanghai
SHELL ["/bin/bash", "-c"]

RUN echo "[oneAPI]" > /tmp/oneAPI.repo
RUN echo "name=Intel® oneAPI repository">>/tmp/oneAPI.repo
RUN echo "baseurl=https://yum.repos.intel.com/oneapi">>/tmp/oneAPI.repo
RUN echo "enabled=1">>/tmp/oneAPI.repo
RUN echo "gpgcheck=1">>/tmp/oneAPI.repo
RUN echo "repo_gpgcheck=1">>/tmp/oneAPI.repo
RUN echo "gpgkey=https://yum.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB">>/tmp/oneAPI.repo

RUN mv /tmp/oneAPI.repo /etc/yum.repos.d

RUN dnf update -y  \
    && dnf install -y  java-1.8.0-openjdk-devel vim unzip net-tools tzdata wget git gcc gcc-c++ make automake maven openssl openssl-devel gcc-toolset-13* libtool perl-IPC-Cmd perl-FindBin intel-oneapi-mkl-devel patch \
    && dnf clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && wget https://github.com/Kitware/CMake/releases/download/v3.27.8/cmake-3.27.8-linux-x86_64.tar.gz \
    && tar -zxvf cmake-3.27.8-linux-x86_64.tar.gz \
    && mv cmake-3.27.8-linux-x86_64 /opt/

# soure env
RUN echo "source /opt/intel/oneapi/setvars.sh" >> /root/.bashrc

ENV PATH="/opt/intel/oneapi:$PATH"
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
ENV PATH=/opt/rh/gcc-toolset-13/root/usr/bin:/opt/cmake-3.27.8-linux-x86_64/bin:$PATH
WORKDIR /opt
