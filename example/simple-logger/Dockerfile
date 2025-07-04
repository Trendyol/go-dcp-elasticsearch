FROM golang:1.20-alpine as builder

WORKDIR /project

COPY . .

WORKDIR /project/example/simple-logger

RUN go mod download
RUN CGO_ENABLED=0 go build -a -o example main.go

FROM alpine:3.17.0

WORKDIR /app

RUN apk --no-cache add ca-certificates

USER nobody
COPY --from=builder --chown=nobody:nobody /project/example/simple-logger/example .
COPY --from=builder --chown=nobody:nobody /project/example/simple-logger/config.yml ./config.yml

ENTRYPOINT ["./example"]