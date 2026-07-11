# syntax=docker/dockerfile:1.7
# Build context: this service's own directory. Contracts arrive as a versioned Go
# module from the public proxy, so nothing outside this repo is needed.

FROM golang:1.26 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-p=1
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download && \
    go vet ./... && \
    go test ./... && \
    go build -trimpath -ldflags="-s -w" -o /out/payment-service ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/payment-service /payment-service
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/payment-service"]
