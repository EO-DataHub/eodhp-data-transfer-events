# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

RUN apt-get update -y && apt-get install --yes --quiet g++ && rm -rf /var/lib/apt/lists/*

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
