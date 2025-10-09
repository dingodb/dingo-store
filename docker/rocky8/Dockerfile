# FROM rockylinux/rockylinux:8.9
FROM dingodatabase/dingo-eureka:rocky8-store

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
    && dnf install -y  java-1.8.0-openjdk-devel vim unzip net-tools tzdata wget git gcc gcc-c++ make automake maven openssl openssl-devel gcc-toolset-13* libtool perl-IPC-Cmd intel-oneapi-mkl-devel-2024.1.x86_64 patch \
    && dnf clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.tar.gz \
    && tar -zxvf cmake-3.26.4-linux-x86_64.tar.gz \
    && mv cmake-3.26.4-linux-x86_64 /opt/ \
    && rm -f cmake-3.26.4-linux-x86_64.tar.gz

# install rust online
# RUN wget https://sh.rustup.rs --content-disposition
# RUN sh rustup-init.sh -y
RUN wget https://static.rust-lang.org/dist/rust-1.78.0-x86_64-unknown-linux-gnu.tar.xz && xz -d rust-1.78.0-x86_64-unknown-linux-gnu.tar.xz && tar -xf rust-1.78.0-x86_64-unknown-linux-gnu.tar && cd rust-1.78.0-x86_64-unknown-linux-gnu && sh install.sh --prefix=/opt/rust/ && cd .. && rm -rf rust-1.78.0-x86_64-unknown-linux-gnu*

# install rust offline
#COPY ./rust-1.78.0-x86_64-unknown-linux-gnu.tar.xz ./
#RUN xz -d rust-1.78.0-x86_64-unknown-linux-gnu.tar.xz && tar -xf rust-1.78.0-x86_64-unknown-linux-gnu.tar && cd rust-1.78.0-x86_64-unknown-linux-gnu && sh install.sh --prefix=/opt/rust/ && cd .. && rm -rf rust-1.78.0-x86_64-unknown-linux-gnu*

# install mkl online
RUN wget https://registrationcenter-download.intel.com/akdlm/IRC_NAS/fdc7a2bc-b7a8-47eb-8876-de6201297144/l_BaseKit_p_2024.1.0.596_offline.sh && bash l_BaseKit_p_2024.1.0.596_offline.sh -a --silent --cli --eula accept && rm -rf l_BaseKit_p_2024.1.0.596_offline.sh

# install mkl offline
#COPY ./l_BaseKit_p_2024.1.0.596_offline.sh ./
#RUN bash l_BaseKit_p_2024.1.0.596_offline.sh -a --silent --cli --eula accept && rm -rf l_BaseKit_p_2024.1.0.596_offline.sh

# config aio (sysctl commands cannot be executed during Docker image build time)
# RUN sysctl -w fs.aio-max-nr=1048576 && sysctl -w fs.aio-nr=524288

# soure env
RUN echo "source /opt/intel/oneapi/setvars.sh" >> /root/.bashrc

ENV PATH="/opt/intel/oneapi:$PATH"
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
ENV PATH=/opt/rust/bin:/opt/rh/gcc-toolset-13/root/usr/bin:/opt/cmake-3.26.4-linux-x86_64/bin:$PATH
WORKDIR /opt
