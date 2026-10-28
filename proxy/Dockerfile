# The lokadb image (the default target) and the release binary (--target binary), for linux/amd64
# and linux/arm64. The build runs on the build machine's own platform and cross-compiles for the
# other one (cgo: pg_query is C), so no stage runs under emulation:
#   docker build -t lokadb proxy
#   docker buildx build --platform linux/amd64,linux/arm64 --target binary --output type=local,dest=out proxy
FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build
ARG TARGETARCH BUILDARCH
RUN if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      case "$TARGETARCH" in \
        arm64) pkgs="gcc-aarch64-linux-gnu libc6-dev-arm64-cross" ;; \
        amd64) pkgs="gcc-x86-64-linux-gnu libc6-dev-amd64-cross" ;; \
        *) echo "unsupported target $TARGETARCH" >&2; exit 1 ;; \
      esac; \
      apt-get update && apt-get install -y --no-install-recommends $pkgs && rm -rf /var/lib/apt/lists/*; \
    fi
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      case "$TARGETARCH" in arm64) export CC=aarch64-linux-gnu-gcc ;; amd64) export CC=x86_64-linux-gnu-gcc ;; esac; \
    fi; \
    CGO_ENABLED=1 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -ldflags "-X main.version=$VERSION" -o /out/lokadb ./cmd/lokadb

# the release binary alone (linked against bookworm's glibc, 2.36)
FROM scratch AS binary
COPY --from=build /out/lokadb /lokadb

# The image runs as root: the stands and the documented setups share certificate and key volumes
# between containers, written by one and read by another. The runner keeps its state in /state
# (a volume the run command gives: -v lokadb-runner:/state).
FROM debian:bookworm-slim
# the CA bundle of the build image: the runner and the agent call HTTPS endpoints (the LokaDB API)
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/lokadb /usr/local/bin/lokadb
ENTRYPOINT ["lokadb"]
