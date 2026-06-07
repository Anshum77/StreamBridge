# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /streambridge ./cmd/server

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /streambridge /usr/local/bin/streambridge
COPY migrations ./migrations

EXPOSE 8080

ENTRYPOINT ["streambridge"]
