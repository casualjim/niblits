FROM --platform=$BUILDPLATFORM ubuntu:24.04

ARG TARGETARCH


RUN apt-get update &&\
  apt-get install -y software-properties-common &&\
  apt-get install -y \
  curl \
  lsb-release \
  gnupg \
  ca-certificates \
  build-essential \
  cmake \
  pkg-config \
  libprotobuf-dev \
  protobuf-compiler \
  libssl-dev \
  tesseract-ocr \
  unzip \
  mold \
  sccache &&\
  curl -sSL https://apt.llvm.org/llvm.sh | bash -s 20 &&\
  apt-get install -y \
  libnode-dev \
  npm \
  libc++-dev &&\
  rm -rf /var/lib/apt/lists/*

ENV CXXFLAGS=-stdlib=libc++ \
  CC=clang-20 \
  CXX=clang++-20 CXXSTDLIB=c++ \
  RUSTUP_HOME=/usr/local/rustup \
  CARGO_HOME=/usr/local/cargo \
  PATH=/usr/local/cargo/bin:$PATH \
  RUST_VERSION=1.91.0


RUN set -eux; \
  \
  apt-get update; \
  apt-get install -y --no-install-recommends \
  ca-certificates \
  gcc \
  libc6-dev \
  wget \
  ; \
  \
  arch="$(dpkg --print-architecture)"; \
  case "$arch" in \
  'amd64') \
  rustArch='x86_64-unknown-linux-gnu'; \
  rustupSha256='20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c'; \
  ;; \
  'arm64') \
  rustArch='aarch64-unknown-linux-gnu'; \
  rustupSha256='e3853c5a252fca15252d07cb23a1bdd9377a8c6f3efa01531109281ae47f841c'; \
  ;; \
  *) \
  echo >&2 "unsupported architecture: $arch"; \
  exit 1; \
  ;; \
  esac; \
  \
  url="https://static.rust-lang.org/rustup/archive/1.28.2/${rustArch}/rustup-init"; \
  wget --progress=dot:giga "$url"; \
  echo "${rustupSha256} *rustup-init" | sha256sum -c -; \
  \
  chmod +x rustup-init; \
  ./rustup-init -y --no-modify-path --profile minimal --default-toolchain $RUST_VERSION --default-host ${rustArch}; \
  rm rustup-init; \
  chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
  \
  apt-get remove -y --auto-remove \
  wget \
  ; \
  rm -rf /var/lib/apt/lists/*; \
  \
  rustup --version; \
  cargo --version; \
  rustc --version;

RUN case "$TARGETARCH" in \
  amd64) TARGET_TRIPLE="x86_64-unknown-linux-gnu" ;; \
  arm64) TARGET_TRIPLE="aarch64-unknown-linux-gnu" ;; \
  *) echo "Unsupported architecture: $TARGETARCH"; exit 1 ;; \
  esac && rustup target add $TARGET_TRIPLE

# Enable sccache and mold for all descendant stages
ENV CARGO_HOME=/usr/local/cargo \
  RUSTC_WRAPPER=/usr/bin/sccache \
  SCCACHE_DIR=/root/.cache/sccache \
  SCCACHE_CACHE_SIZE=10G \
  RUSTFLAGS="-C link-arg=-fuse-ld=mold"

RUN curl -fsSL https://bun.sh/install | BUN_INSTALL=/usr/local bash

ADD cargo.toml /.cargo/config.toml
