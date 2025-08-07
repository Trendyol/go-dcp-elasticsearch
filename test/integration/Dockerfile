FROM golang:1.24-alpine as builder

WORKDIR /project

RUN apk add build-base

COPY .. .

RUN go mod tidy
RUN go test -v ./...