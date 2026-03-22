# Claude Hatchery Sandbox
# Minimal image for running Claude Code in an isolated environment.
# Uncomment the sections below to add tools your project needs.
FROM debian:trixie-slim@sha256:1d3c811171a08a5adaa4a163fbafd96b61b87aa871bbc7aa15431ac275d3d430

# ── System packages ───────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    jq \
    make \
    unzip \
    zip \
    && rm -rf /var/lib/apt/lists/*

# ── Non-root user (required for --dangerously-skip-permissions) ───────────────
RUN useradd -m -s /bin/bash claude
USER claude
ENV HOME=/home/claude
ENV PATH="/home/claude/.npm-global/bin:/home/claude/.local/bin:$PATH"

# ── Claude Code ───────────────────────────────────────────────────────────────
# Pin the bootstrap script by hash; update CLAUDE_INSTALL_SHA256 when upgrading
# Claude Code. The bootstrap script also verifies the binary it downloads.
RUN curl -fsSL https://claude.ai/install.sh | bash

# =============================================================================
# Optional extras — uncomment what you need
# =============================================================================

# ── Python / uv ───────────────────────────────────────────────────────────────
# COPY --from=ghcr.io/astral-sh/uv:0.10.7 /uv /uvx /bin/

# ── Go ────────────────────────────────────────────────────────────────────────
# COPY --from=golang:1.24 /usr/local/go /usr/local/go
# ENV PATH="/usr/local/go/bin:$PATH"

# ── Rust / cargo ──────────────────────────────────────────────────────────────
# COPY --from=rust:1.86-slim /usr/local/cargo /usr/local/cargo
# COPY --from=rust:1.86-slim /usr/local/rustup /usr/local/rustup
# ENV RUSTUP_HOME=/usr/local/rustup
# ENV PATH="/usr/local/cargo/bin:$PATH"

# ── Extra apt packages ────────────────────────────────────────────────────────
# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     jq \
#     postgresql-client \
#     && rm -rf /var/lib/apt/lists/*
# USER claude

# =============================================================================

WORKDIR /repo
