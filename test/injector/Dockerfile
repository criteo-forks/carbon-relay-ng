FROM golang:1.12 as builder
WORKDIR /injector
RUN go get github.com/prometheus/client_golang/prometheus && \
    go get github.com/prometheus/client_golang/prometheus/promauto && \
    go get github.com/prometheus/client_golang/prometheus/promhttp
COPY injector-test.go . 
RUN CGO_ENABLED=0 GOOS=linux go build  -o injector-test . 

FROM alpine
WORKDIR /app
ENV SLEEP_TIME=1000
ENV ENDPOINT_HOST="carbon-relay-ng"
ENV ENDPOINT_PORT=2003
COPY --from=builder /injector/injector-test /app/injector-test
ENTRYPOINT ./injector-test $SLEEP_TIME $ENDPOINT_HOST $ENDPOINT_PORT
