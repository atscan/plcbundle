# syntax=docker/dockerfile:1

FROM golang:1.25.3-alpine AS builder

RUN apk add --no-cache git gcc musl-dev zstd-dev

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build \
    -ldflags="-w -s" \
    -trimpath \
    -o plcbundle \
    ./cmd/plcbundle

FROM alpine:3.19

RUN apk add --no-cache ca-certificates zstd-libs

RUN addgroup -g 1000 plcbundle && \
    adduser -D -u 1000 -G plcbundle plcbundle && \
    mkdir -p /data && \
    chown plcbundle:plcbundle /data

COPY --from=builder /build/plcbundle /usr/local/bin/plcbundle

WORKDIR /data
USER plcbundle

ENTRYPOINT ["plcbundle"]
