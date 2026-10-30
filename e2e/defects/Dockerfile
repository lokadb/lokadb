# Built from the repository root (see compose.yml): the lokadb binary under test plus the checks.
FROM golang:1.25-bookworm AS build
WORKDIR /src/proxy
COPY proxy/go.mod proxy/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY proxy/ ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -o /out/lokadb ./cmd/lokadb
WORKDIR /src/defects
COPY e2e/defects/go.mod e2e/defects/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY e2e/defects/*.go ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -o /out/defects .

FROM debian:bookworm-slim
COPY --from=build /out/ /usr/local/bin/
# PR1 fetches Cal.com's migrations over HTTPS
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["defects"]
CMD ["/usr/local/bin/lokadb"]
