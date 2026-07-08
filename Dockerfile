# syntax=docker/dockerfile:1.7
# Build context: repo root (dockerfile: services/payment-service/Dockerfile)

FROM golang:1.26 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-p=1
COPY contracts/gen/go ./contracts/gen/go
COPY services/payment-service ./services/payment-service
WORKDIR /src/services/payment-service
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
