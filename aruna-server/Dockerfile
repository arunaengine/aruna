# Build Stage
FROM rust:1-alpine3.20 AS builder
WORKDIR /build
RUN apk update
RUN apk upgrade
ENV RUSTFLAGS="-C target-feature=-crt-static --cfg tokio_unstable"
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev protoc protobuf-dev protobuf-dev musl-dev git curl openssh
COPY . .
RUN cargo build --release
RUN cargo install --root /build --locked tokio-console

FROM alpine:3.20
WORKDIR /run
RUN apk update
RUN apk upgrade
RUN apk add libgcc gcompat ca-certificates openssl-dev
COPY --from=builder /build/target/release/aruna_server .
COPY --from=builder /build/bin/tokio-console .
CMD [ "/run/aruna_server" ]
