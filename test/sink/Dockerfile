FROM golang:1.12 as builder
WORKDIR /sink
RUN go get github.com/prometheus/client_golang/prometheus && \
    go get github.com/prometheus/client_golang/prometheus/promauto && \
    go get github.com/prometheus/client_golang/prometheus/promhttp
COPY sink-test.go . 
RUN CGO_ENABLED=0 GOOS=linux go build  -o sink-test . 

FROM alpine
WORKDIR /app
COPY --from=builder /sink/sink-test /app/sink-test
ENTRYPOINT ./sink-test
