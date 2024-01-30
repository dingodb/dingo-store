FROM centos:centos8

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

RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* && dnf update -y  \
    && dnf install -y  java-1.8.0-openjdk-devel vim unzip net-tools tzdata wget git gcc gcc-c++ make automake maven openssl openssl-devel patch gcc-toolset-11* libtool perl-IPC-Cmd intel-oneapi-mkl-devel \
    && dnf clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.tar.gz \
    && tar -zxvf cmake-3.26.4-linux-x86_64.tar.gz \
    && mv cmake-3.26.4-linux-x86_64 /opt/ \
    && rm -f cmake-3.26.4-linux-x86_64.tar.gz

# soure env
RUN echo "source /opt/intel/oneapi/setvars.sh" >> /root/.bashrc

ENV PATH="/opt/intel/oneapi:$PATH"
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
ENV PATH=/opt/rh/gcc-toolset-11/root/usr/bin:/opt/cmake-3.26.4-linux-x86_64/bin:$PATH
WORKDIR /opt
