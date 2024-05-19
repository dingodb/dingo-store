FROM rockylinux/rockylinux:8

ENV TZ=Asia/Shanghai
SHELL ["/bin/bash", "-c"]

RUN dnf update -y  \
    && dnf install -y vim unzip net-tools tzdata wget git gcc gcc-c++ make automake openssl openssl-devel gcc-toolset-13* libtool patch \
    && dnf clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

COPY ./dingo-store.tar.gz  /opt

RUN cd /opt && mkdir dingo-store && tar -zxvf dingo-store.tar.gz -C dingo-store && chmod +x -R /opt/dingo-store/* && sed -i 's/electionTimeout: [0-9]\+/electionTimeout: 30000/g' /opt/dingo-store/conf/store.template.yaml && sed -i 's/electionTimeout: [0-9]\+/electionTimeout: 30000/g' /opt/dingo-store/conf/coordinator.template.yaml

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

ENV PATH=/opt/rh/gcc-toolset-13/root/usr/bin:$PATH
WORKDIR /opt/dingo-store

ENTRYPOINT [ "/opt/dingo-store/scripts/docker-dingo-store.sh" ]
CMD ["cleanstart"]
