FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && CGO_ENABLED=0 go build -o /out/goapp .

FROM debian:bookworm-slim
COPY --from=build /out/goapp /usr/local/bin/goapp
ENTRYPOINT ["goapp"]
