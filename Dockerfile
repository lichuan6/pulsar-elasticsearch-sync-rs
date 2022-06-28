# 1: Build the exe
# FROM rust:1.60 as builder
FROM rust as builder
ENV PKG_CONFIG_ALLOW_CROSS=1
WORKDIR /usr/src

# 1a: Prepare for static linking
RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y pkg-config libssl-dev libprotobuf-dev protobuf-compiler
    #  musl-tools && \
    # rustup target add x86_64-unknown-linux-musl

# 1b: Download and compile Rust dependencies (and store as a separate Docker layer)
RUN USER=root cargo new pulsar-elasticsearch-sync-rs
WORKDIR /usr/src/pulsar-elasticsearch-sync-rs
COPY Cargo.toml Cargo.lock ./
# RUN cargo install --target x86_64-unknown-linux-musl --path .
# RUN RUSTFLAGS=-Clinker=musl-gcc cargo install -—release —target=x86_64-unknown-linux-musl --path .
#RUN cargo install --path .

# 1c: Build the exe using the actual source code
COPY src ./src
COPY elasticsearch-rs-7.x ./elasticsearch-rs-7.x
# RUN cargo install --target x86_64-unknown-linux-musl --path .
# RUN RUSTFLAGS=-Clinker=musl-gcc cargo install -—release —target=x86_64-unknown-linux-musl --path .
RUN cargo install --path .

# 2: Copy the exe and extra files ("static") to an empty Docker image
#FROM scratch
# FROM alpine:latest
# FROM gcr.io/distroless/cc-debian10
FROM gcr.io/distroless/cc-debian10:debug
COPY --from=builder /usr/local/cargo/bin/pulsar-elasticsearch-sync-rs /pulsar-elasticsearch-sync-rs
#COPY static .
# USER 1000
ENTRYPOINT ["/pulsar-elasticsearch-sync-rs"]
