# syntax=docker/dockerfile:1

FROM golang:1.25.3-alpine AS builder

RUN apk add --no-cache git gcc musl-dev zstd-dev

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=1 go build \
    -ldflags="-w -s -X 'main.version=${VERSION}' -X 'main.gitCommit=${GIT_COMMIT}' -X 'main.buildDate=${BUILD_DATE}'" \
    -trimpath \
    -o plcbundle \
    ./cmd/plcbundle

FROM alpine:3.19

RUN apk add --no-cache ca-certificates zstd-libs

COPY --from=builder /build/plcbundle /usr/local/bin/plcbundle

WORKDIR /data

ENTRYPOINT ["plcbundle"]
