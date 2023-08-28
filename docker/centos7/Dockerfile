FROM centos:7

ENV TZ=Asia/Shanghai
SHELL ["/bin/bash", "-c"]

RUN yum makecache  \
    && yum install -y  ntp openjdk-1.8-jdk vim unzip netcat net-tools tzdata wget git gcc gcc-c++ make automake maven centos-release-scl openssl openssl-devel libtool \
    && yum install -y devtoolset-11-gcc* devtoolset-11-binutils \
    && yum install -y  http://opensource.wandisco.com/centos/7/git/x86_64/wandisco-git-release-7-2.noarch.rpm && yum -y install git \
    && yum -y install perl-IPC-Cmd \
    && yum clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY \
    && wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.tar.gz \
    && tar -zxvf cmake-3.26.4-linux-x86_64.tar.gz \
    && mv cmake-3.26.4-linux-x86_64 /opt/ \
    && rm -f cmake-3.26.4-linux-x86_64.tar.gz

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk/
ENV PATH=/opt/rh/devtoolset-11/root/usr/bin:/opt/cmake-3.26.4-linux-x86_64/bin:$PATH
WORKDIR /opt

