# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system ports-box \
    && useradd --system --gid ports-box --home-dir /var/lib/ports-box ports-box \
    && mkdir -p /etc/ports-box /var/lib/ports-box \
    && chown -R ports-box:ports-box /var/lib/ports-box

COPY --from=builder /app/target/release/ports-box /usr/local/bin/ports-box
COPY config.example.json /etc/ports-box/config.example.json

USER ports-box
WORKDIR /var/lib/ports-box

EXPOSE 7070

ENTRYPOINT ["ports-box"]
CMD ["-c", "/etc/ports-box/config.json", "-d", "/var/lib/ports-box"]
