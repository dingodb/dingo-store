# Multi-stage build + minimal runtime base.
#
# Builder stage uses dingo-base:rocky8 (same image that ci_rocky8.yml's
# build env extends), guaranteeing glibc / libstdc++ ABI compatibility
# with the stripped binary produced by prepare_release_artifacts.
#
# Runtime stage uses rockylinux:8-minimal (~140 MB) and installs only
# the shared libraries that `ldd dingodb_server` actually requires:
#
#   linux-vdso, ld-linux, libdl, librt, libpthread, libm, libc → glibc
#   libstdc++.so.6                                             → libstdc++
#   libgcc_s.so.1                                              → libgcc (auto)
#
# All heavy dependencies (brpc, braft, rocksdb, boost, openssl, MKL,
# ...) are statically linked into the binary upstream, so the runtime
# image stays slim. See docs/debug-symbols.md for the full pipeline.

ARG BUILDER_IMAGE=dingodatabase/dingo-base:rocky8
FROM ${BUILDER_IMAGE} AS builder

COPY ./dingo-store.tar.gz /opt/
RUN cd /opt \
    && mkdir dingo-store \
    && tar -zxvf dingo-store.tar.gz -C dingo-store \
    && chmod +x -R /opt/dingo-store/* \
    && sed -i 's/electionTimeout: [0-9]\+/electionTimeout: 30000/g' /opt/dingo-store/conf/store.template.yaml \
    && sed -i 's/electionTimeout: [0-9]\+/electionTimeout: 30000/g' /opt/dingo-store/conf/coordinator.template.yaml \
    && rm /opt/dingo-store.tar.gz

FROM rockylinux:8-minimal

ENV TZ=Asia/Shanghai
SHELL ["/bin/bash", "-c"]

# Runtime-only packages: only what `ldd dingodb_server` requires plus
# minimal shell tools for docker-dingo-store.sh / deploy_func.sh
# (sed/awk/grep are already in rockylinux:8-minimal base).
RUN microdnf install -y \
        libstdc++ \
        procps-ng \
        findutils \
        tzdata \
        net-tools \
        iproute \
    && microdnf clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone

COPY --from=builder /opt/dingo-store /opt/dingo-store

WORKDIR /opt/dingo-store

ENTRYPOINT ["/opt/dingo-store/scripts/docker-dingo-store.sh"]
CMD ["cleanstart"]
