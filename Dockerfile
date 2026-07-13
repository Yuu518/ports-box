FROM --platform=$BUILDPLATFORM rust:1-bookworm AS builder

ARG TARGETPLATFORM

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN case "$TARGETPLATFORM" in \
        "linux/amd64") target=x86_64-unknown-linux-gnu ;; \
        "linux/arm64") target=aarch64-unknown-linux-gnu ;; \
        *) echo "unsupported platform: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac \
    && rustup target add "$target" \
    && if [ "$target" != "$(rustc -vV | sed -n 's/^host: //p')" ]; then \
        apt-get update \
        && apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu libc6-dev-arm64-cross \
        && rm -rf /var/lib/apt/lists/*; \
    fi \
    && CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
        CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
        cargo build --release --locked --target "$target" \
    && cp "target/$target/release/ports-box" /app/ports-box

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system ports-box \
    && useradd --system --gid ports-box --home-dir /var/lib/ports-box ports-box \
    && mkdir -p /etc/ports-box /var/lib/ports-box \
    && chown -R ports-box:ports-box /var/lib/ports-box

COPY --from=builder /app/ports-box /usr/local/bin/ports-box
COPY config.example.json /etc/ports-box/config.example.json

USER ports-box
WORKDIR /var/lib/ports-box

EXPOSE 7070

ENTRYPOINT ["ports-box"]
CMD ["-c", "/etc/ports-box/config.json", "-d", "/var/lib/ports-box"]
