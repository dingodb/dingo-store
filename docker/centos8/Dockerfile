FROM centos:centos8

ENV TZ=Asia/Shanghai 
SHELL ["/bin/bash", "-c"]

RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* && dnf update -y  \
    && dnf install -y  java-1.8.0-openjdk-devel vim unzip net-tools tzdata wget git gcc gcc-c++ make automake maven openssl openssl-devel gcc-toolset-11  cmake libtool \
    && dnf -y install perl-IPC-Cmd \
    && dnf clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY 



ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
ENV PATH=/opt/rh/gcc-toolset-11/root/usr/bin:$PATH
WORKDIR /opt

