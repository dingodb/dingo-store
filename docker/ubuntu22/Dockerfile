FROM ubuntu:22.04

ENV TZ=Asia/Shanghai \
    DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

RUN apt-get update  \
    && apt-get install -y openjdk-8-jdk vim unzip netcat net-tools tzdata wget git gcc g++ make automake maven openssl libssl-dev cmake \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/* 




ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

WORKDIR /opt

