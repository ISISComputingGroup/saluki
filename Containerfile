FROM rust:1-trixie AS builder
WORKDIR /usr/src/saluki
RUN apt-get update && apt-get install -y build-essential cmake && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo install --path .

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y libcurl4-openssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/saluki /usr/local/bin/saluki
ENTRYPOINT ["saluki"]
