FROM docker.io/library/golang:1.18 AS builder

COPY ./ /src/druid-index-gateway

WORKDIR /src/druid-index-gateway

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags '-extldflags "-static"' -o gateway main.go

FROM scratch

COPY --from=builder /src/druid-index-gateway/gateway /gateway

ENTRYPOINT ["/gateway"]
