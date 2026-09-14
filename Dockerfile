# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
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
