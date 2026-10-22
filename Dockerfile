# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.99.1
ARG PYTHON_VERSION=3.12.14
ARG UV_VERSION=0.12.23

FROM rust:${RUST_VERSION}-bookworm AS collector-build
WORKDIR /build/collector
COPY collector/Cargo.toml collector/Cargo.lock ./
COPY collector/src ./src
RUN --mount=type=secret,id=proxy_ca \
    if [ -f /run/secrets/proxy_ca ]; then export CARGO_HTTP_CAINFO=/run/secrets/proxy_ca; fi; \
    cargo build --locked --release

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH" \
    DAGSTER_HOME=/var/lib/sm-trends/dagster \
    CACHE_DIR=/var/lib/sm-trends/cache \
    SPOOL_DIR=/var/lib/sm-trends/spool \
    COLLECTOR_BINARY=/usr/local/bin/sm-collector
WORKDIR /app
COPY --from=uv /uv /usr/local/bin/uv
COPY --from=collector-build /build/collector/target/release/sm-collector /usr/local/bin/
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=secret,id=proxy_ca \
    if [ -f /run/secrets/proxy_ca ]; then export SSL_CERT_FILE=/run/secrets/proxy_ca; fi; \
    uv sync --locked --no-dev --no-install-project
COPY src ./src
COPY deploy ./deploy
COPY scripts ./scripts
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=secret,id=proxy_ca \
    if [ -f /run/secrets/proxy_ca ]; then export SSL_CERT_FILE=/run/secrets/proxy_ca; fi; \
    chmod -R a+rX /app/src /app/deploy /app/scripts \
    && uv sync --locked --no-dev --no-editable \
    && groupadd --gid 10001 pipeline \
    && useradd --uid 10001 --gid 10001 --no-create-home pipeline \
    && mkdir -p /var/lib/sm-trends/cache /var/lib/sm-trends/spool /var/lib/sm-trends/dagster \
    && cp deploy/dagster/dagster.yaml /var/lib/sm-trends/dagster/dagster.yaml \
    && chown -R 10001:10001 /var/lib/sm-trends
USER 10001:10001
CMD ["sm-trends", "status", "--json"]
