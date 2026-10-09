# Builds the registry image: a Cargo release build, then a distroless runtime stage.
# Image: harbor.computational.bio.uni-giessen.de/aruna/aruna-registry
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

FROM rust:1.97.1-trixie@sha256:1bcff4befb740599103a2c7cb51058e14479b2e35e3a34a3f0dc4ede09927488 AS builder
WORKDIR /build
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
# mold and clang back `.cargo/config.toml`, which only applies to the gnu target.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake mold git \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --locked -p aruna-registry

FROM gcr.io/distroless/cc-debian13@sha256:ed7c407fd64eb0af9dddb9456b94cee188a40a7f53cf38c9836e1e9ae14fca02
WORKDIR /run
COPY --from=builder /build/target/release/aruna-registry .
ENV ARUNA_REGISTRY_LISTEN=0.0.0.0:8080 ARUNA_REGISTRY_DATA=/data
EXPOSE 8080
CMD [ "/run/aruna-registry" ]
