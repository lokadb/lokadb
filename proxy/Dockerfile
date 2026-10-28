FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -o /out/lokadb ./cmd/lokadb

FROM debian:bookworm-slim
COPY --from=build /out/lokadb /usr/local/bin/lokadb
ENTRYPOINT ["lokadb"]
