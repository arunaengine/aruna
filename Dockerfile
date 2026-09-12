# glibc, not musl: musl's `cmsghdr` is 4-byte aligned, so noq-udp's receive
# timestamp decode trips its alignment assertion and aborts the process.
FROM rust:1.97.1-trixie@sha256:1bcff4befb740599103a2c7cb51058e14479b2e35e3a34a3f0dc4ede09927488 AS builder
WORKDIR /build
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
# mold and clang back `.cargo/config.toml`, which only applies to the gnu target.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake mold \
    && rm -rf /var/lib/apt/lists/*
# Stage the website build in .portal-embed, as the image helper does.
# An absent staging directory produces a headless image; PORTAL_EMBED_DIR can override it.
ARG PORTAL_EMBED_DIR=.portal-embed
COPY . .
RUN cargo build --release --locked -p aruna
RUN cargo build --release --locked -p aruna-doctor
RUN cargo install --locked --version 0.101.0 --root target iroh-doctor
# The runtime image has no shell, so copy the staged portal in the builder.
RUN mkdir -p /portal ${PORTAL_EMBED_DIR} && cp -r ${PORTAL_EMBED_DIR}/. /portal/

FROM gcr.io/distroless/cc-debian13@sha256:ed7c407fd64eb0af9dddb9456b94cee188a40a7f53cf38c9836e1e9ae14fca02
WORKDIR /run
COPY --from=builder /build/target/release/aruna .
COPY --from=builder /build/target/release/aruna-doctor .
COPY --from=builder /build/target/bin/iroh-doctor .
COPY --from=builder /portal/ /run/portal/
# Leave PORTAL_MODE unset: process env would override mounted /run/.env and pin the portal off.
# PORTAL_DIR points at the embedded copy.
ENV PORTAL_DIR=/run/portal

CMD [ "/run/aruna" ]
