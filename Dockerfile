# syntax=docker/dockerfile:1

ARG GO_VERSION=1.24

FROM  golang:${GO_VERSION}-alpine

WORKDIR /src
COPY . .
RUN go build --ldflags '-extldflags "-static"' -o /usr/bin/docker-log-driver
