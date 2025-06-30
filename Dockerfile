# Build Stage
FROM rust:1-alpine3.22 AS builder
WORKDIR /build
RUN apk update
RUN apk upgrade
ENV RUSTFLAGS="-C target-feature=-crt-static"
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev musl-dev git curl openssh
COPY . .
RUN cargo build --release

FROM alpine:3.22
WORKDIR /run
RUN apk update
RUN apk upgrade
RUN apk add libgcc gcompat ca-certificates openssl-dev
COPY --from=builder /build/target/release/aruna .
CMD [ "/run/aruna" ]
