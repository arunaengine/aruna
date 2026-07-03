# Build Stage
FROM rust:1-alpine3.24 AS builder
WORKDIR /build
RUN apk update
RUN apk upgrade
ENV RUSTFLAGS="-C target-feature=-crt-static"
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev musl-dev git curl
COPY . .
RUN cargo build --release -p aruna
RUN cargo build --release -p aruna-doctor
RUN cargo install --locked --root target iroh-doctor

FROM alpine:3.24
WORKDIR /run
ARG PORTAL_EMBED_DIR=docker/portal
ARG PORTAL_MODE=disabled
RUN apk update
RUN apk upgrade
RUN apk add libgcc gcompat ca-certificates
COPY --from=builder /build/target/release/aruna .
COPY --from=builder /build/target/release/aruna-doctor .
COPY --from=builder /build/target/bin/iroh-doctor .
COPY ${PORTAL_EMBED_DIR}/ /run/portal/
RUN rm -f /run/portal/.gitkeep
ENV PORTAL_MODE=${PORTAL_MODE}
ENV PORTAL_DIR=/run/portal

CMD [ "/run/aruna" ]
