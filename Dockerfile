FROM rust:alpine AS builder

RUN apk add --no-cache build-base musl-dev pkgconfig openssl-dev openssl-libs-static

WORKDIR /app

COPY  Cargo.toml Cargo.lock ./

RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null ||true

COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM alpine:3.19 AS runtime

RUN apk add --no-cache ca-certificates tzdata

RUN addgroup -g 1001 etl && adduser -D -u 1001 -G etl etl

WORKDIR /app

COPY --from=builder /app/target/release/etl-engine /usr/local/bin/etl-engine

RUN mkdir -p /app/config /app/data/watched /app/data/processed \
    && chown -R etl:etl /app

USER etl

VOLUME ["/app/config", "app/data"]

ENV RUST_LOG=info
ENV CONFIG_PATH=/app/config/pipeline.json
ENV STATE_PATH=/app/data/etl_state.json

ENTRYPOINT ["/usr/local/bin/etl-engine"]
CMD ["/app/config/pipeline.json", "/app/data/etl_state.json"]
