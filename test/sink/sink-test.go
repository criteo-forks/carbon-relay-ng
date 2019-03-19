package main

import (
    "bufio"
    "fmt"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "net"
    "net/http"
    "strconv"
    "strings"
    "time"
)

var (
        metricsInjectorReceived = promauto.NewCounter(prometheus.CounterOpts{
                Name: "sink_metrics_injector_received_count",
                Help: "The total number of metrics received from injector",
        })
        metricsCarbonReceived = promauto.NewCounter(prometheus.CounterOpts{
                Name: "sink_metrics_carbon_received_count",
                Help: "The total number of metrics received from carbon relay",
        })
        metricsLatency = promauto.NewHistogram(prometheus.HistogramOpts{
                Name:    "sink_metrics_latency",
                Help:    "Histogram latency metrics", 
		Buckets: prometheus.ExponentialBuckets(10000000,10,3),
        })
)

func getLatency(epochTimeStamp int64) int64 {
  currentEpochTimeInSec := time.Now().UnixNano()
  latency := currentEpochTimeInSec - epochTimeStamp
  return latency
}

func isMetricFromCarbonRelayNg(metricPath string) bool {
   return strings.HasPrefix(metricPath,"service_is_carbon-relay-ng")
}

func handleMetricMessage(message string) {
  metricMessage := strings.Split(message," ")
  if len(metricMessage) == 3 {
    if isMetricFromCarbonRelayNg(metricMessage[0]) {
      metricsCarbonReceived.Inc()  
    } else {
        epochTimeStamp,_ := strconv.ParseInt(metricMessage[1],10,64)
        latency := getLatency(epochTimeStamp)
        metricsLatency.Observe(float64(latency))
      metricsInjectorReceived.Inc()
    } 
  }
}

func loopingCallReceiveMetrics() {
  go func() {
    ln, _ := net.Listen("tcp", ":2003")
    fmt.Println("Waiting connection")
    for {
      conn, _ := ln.Accept()
      //fmt.Println("Connection accepted")
      buffer:= bufio.NewReader(conn)
      for {   
        message,_ := buffer.ReadString('\n')
        message = strings.TrimSuffix(message, "\n")
        fmt.Println("message ",message)
        handleMetricMessage(message)
      }
    }
  }()
}

func initPrometheusHttpEndpoint() {
  http.Handle("/metrics", promhttp.Handler())
  http.ListenAndServe(":2112", nil)
}

func main() {
  loopingCallReceiveMetrics()
  initPrometheusHttpEndpoint()
}
