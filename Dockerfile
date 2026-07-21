# syntax=docker/dockerfile:1@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim@sha256:f4adb3fe11f03f693466c36afd17b43ffd11eb9df3cd4f7a9337cf2e6ec4c8e8

RUN apt-get update -y && apt-get install --yes --quiet g++ git && rm -rf /var/lib/apt/lists/*

ENV UV_NO_DEV=1

WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

# Copy project files
COPY . /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

CMD ["uv", "run", "--no-sync", "python", "-m", "billing_scanner"]
