FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends quota e2fsprogs util-linux coreutils ca-certificates \
    && rm -rf /var/lib/apt/lists/*
