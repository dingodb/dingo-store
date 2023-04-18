FROM centos:centos8

ENV TZ=Asia/Shanghai 
SHELL ["/bin/bash", "-c"]

RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* && dnf update -y  \
    && dnf install -y  java-1.8.0-openjdk-devel vim unzip net-tools tzdata wget git gcc gcc-c++ make automake maven openssl openssl-devel gcc-toolset-11  cmake\
    && dnf -y install perl-IPC-Cmd \
    && dnf clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY 


COPY ./dingo-store.tar.gz  /opt

RUN cd /opt && mkdir dingo-store && tar -zxvf dingo-store.tar.gz -C dingo-store && chmod +x -R /opt/dingo-store/* && sed -i 's/electionTimeout: [0-9]\+/electionTimeout: 30000/g' /opt/dingo-store/conf/store.template.yaml && sed -i 's/electionTimeout: [0-9]\+/electionTimeout: 30000/g' /opt/dingo-store/conf/coordinator.template.yaml


ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
ENV PATH=/opt/rh/gcc-toolset-11/root/usr/bin:$PATH
WORKDIR /opt/dingo-store

ENTRYPOINT [ "/opt/dingo-store/scripts/docker-dingo-store.sh" ]
CMD ["cleanstart"]
