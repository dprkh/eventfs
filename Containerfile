# syntax=docker/dockerfile:1

FROM lukemathwalker/cargo-chef:latest-rust-1-bookworm

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV PATH="/usr/local/cargo/bin:${PATH}"
ENV CARGO_HOME="/usr/local/cargo"
ENV RUSTUP_HOME="/usr/local/rustup"
ENV RUST_TEST_THREADS=1

RUN rustup component add clippy
RUN rm -rf "${CARGO_HOME}/registry" "${CARGO_HOME}/git"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        fuse-overlayfs \
        fuse3 \
        libclang-dev \
        libfuse3-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work

CMD ["bash", "/work/dev.sh", "__inside-test"]
