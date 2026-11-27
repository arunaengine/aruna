# Builds the registry image: a Cargo release build, then a distroless runtime stage.
# Image: ghcr.io/arunaengine/aruna-registry
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

FROM rust:1.99.1-trixie@sha256:8a4e90c7946b74626488b1446f712c0fe0ed1a596cbd3d871684bc9c5433ae07 AS builder
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
