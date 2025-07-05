FROM golang:1.20-alpine as builder

WORKDIR /project

COPY . .

WORKDIR /project/example/struct-config

RUN go mod download
RUN CGO_ENABLED=0 go build -a -o example main.go

FROM alpine:3.17.0@sha256:8914eb54f968791faf6a8638949e480fef81e697984fba772b3976835194c6d4

WORKDIR /app

RUN apk --no-cache add ca-certificates

USER nobody
COPY --from=builder --chown=nobody:nobody /project/example/struct-config/example .
COPY --from=builder --chown=nobody:nobody /project/example/struct-config/config.yml ./config.yml

ENTRYPOINT ["./example"]