# ostro-build: the `ostro build --docker` image (plan 10-15, D-P10-15).
#
# Built locally on first use as `ostro-build:<ostro version>` from this file, which is
# embedded in the ostro binary (`docker build -t ostro-build:<version> -`, Dockerfile on
# stdin, no build context). No toolchain is baked in: `ostro build --in-container` runs
# the hermetic path and downloads the pinned toolchains (docs/toolchains.md) into the
# /cache volume (the host's toolchain cache). The static musl ostro binary is bind-mounted
# read-only at /usr/local/bin/ostro and the project at /src; the container runs as the
# invoking user (--user uid:gid).
#
# Base: debian:12-slim (glibc 2.36 >= 2.28, which the manylinux componentize-py wheel and
# the official Node builds need), pinned by the multi-arch index digest. Digest source:
# `docker buildx imagetools inspect debian:12-slim` against docker.io on 2026-11-22.
#
# Licence note: the image redistributes Debian packages (base system + ca-certificates)
# under their own licences; a NOTICE for a published image is human_needed at publish time
# (the image is only built locally, never pushed, in Phase 10).
FROM debian:12-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV OSTRO_TOOLCHAIN_DIR=/cache
WORKDIR /src
