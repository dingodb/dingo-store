FROM centos:7

ENV TZ=Asia/Shanghai 
SHELL ["/bin/bash", "-c"]

RUN yum makecache  \
    && yum install -y  ntp openjdk-1.8-jdk vim unzip netcat net-tools tzdata wget git gcc gcc-c++ make automake maven centos-release-scl  openssl openssl-devel  \
    &&  yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-binutils \
    && yum -y install perl-IPC-Cmd \
    && yum clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY \
    && wget https://cmake.org/files/v3.25/cmake-3.25.2.tar.gz --no-check-certificate && tar -zxvf cmake-3.25.2.tar.gz && rm -f cmake-3.25.2.tar.gz && cd cmake-3.25.2 && ./bootstrap && gmake -j8 && gmake install && cd .. && rm -rf cmake-3.25.2 



ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk/
ENV PATH=/opt/rh/devtoolset-11/root/usr/bin:$PATH
WORKDIR /opt

