FROM golang:1.24-alpine AS build

ARG VERSION=dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X chiperka-worker/cmd.Version=${VERSION}" -o /chiperka-worker .

FROM alpine:3.21

# The worker drives Docker on the host (mounted /var/run/docker.sock).
RUN apk add --no-cache ca-certificates docker-cli curl bash

WORKDIR /app
COPY --from=build /chiperka-worker /usr/local/bin/chiperka-worker

# Configure with --api / --token flags or CHIPERKA_API_URL / CHIPERKA_WORKER_TOKEN.
ENTRYPOINT ["chiperka-worker", "worker"]
