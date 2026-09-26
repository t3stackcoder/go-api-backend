# syntax=docker/dockerfile:1
#
# Multi-stage build of the cmd/ binaries (spec 12.1). Build context is the
# repository root; the compose file passes `context: ..` and
# `dockerfile: deploy/Dockerfile`.
#
# Targets:
#   chaosnode  cmd/chaosnode built with -tags faultinject, plus cmd/mediatorctl.
#              Used by the compose `chaos` profile (node1..node3).
#   orders     examples/orders built without the fault tag, plus cmd/mediatorctl.
#
# NOTE: cmd/chaosnode, cmd/mediatorctl and examples/orders are being written
# by other milestones; `docker build` succeeds only once those packages exist.
#
#   docker build -f deploy/Dockerfile --target chaosnode -t go-api-backend/chaosnode .
#   docker build -f deploy/Dockerfile --target orders    -t go-api-backend/orders .

ARG GO_IMAGE=golang:1.27

# ---------------------------------------------------------------------------
# Shared builder: module cache and source.
FROM ${GO_IMAGE} AS base
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

# ---------------------------------------------------------------------------
# Chaos node and operations CLI. The node carries the faultinject tag so the
# chaos controller can arm fault points remotely (spec 11.6).
FROM base AS build-chaosnode
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -tags faultinject -ldflags='-s -w' -o /out/chaosnode ./cmd/chaosnode && \
    go build -ldflags='-s -w' -o /out/mediatorctl ./cmd/mediatorctl

FROM gcr.io/distroless/static:nonroot AS chaosnode
COPY --from=build-chaosnode /out/chaosnode /chaosnode
COPY --from=build-chaosnode /out/mediatorctl /mediatorctl
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/chaosnode"]

# ---------------------------------------------------------------------------
# Example service, without fault injection.
FROM base AS build-orders
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags='-s -w' -o /out/orders ./examples/orders && \
    go build -ldflags='-s -w' -o /out/mediatorctl ./cmd/mediatorctl

FROM gcr.io/distroless/static:nonroot AS orders
COPY --from=build-orders /out/orders /orders
COPY --from=build-orders /out/mediatorctl /mediatorctl
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/orders"]
