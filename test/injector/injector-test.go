package main

import (
  "net"
  "fmt"
  "time"
  "os"
  "strconv"
  "net/http"
  "github.com/prometheus/client_golang/prometheus"
  "github.com/prometheus/client_golang/prometheus/promauto"
  "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
        metricsSent = promauto.NewCounter(prometheus.CounterOpts{
                Name: "injector_sent_metrics",
                Help: "The total number of metrics sent",
        })
)

func initTcpConnection(host string, port int) net.Conn {
  hostPort := fmt.Sprintf("%s:%d",host,port)
  fmt.Printf("Trying to connect")
  conn, err := net.Dial("tcp", hostPort)
  for err != nil {
     conn, err = net.Dial("tcp", hostPort)
     fmt.Println("Fail to connect. Sleep 1 sec. ",err)
     time.Sleep(1000 * time.Millisecond)
  }
  fmt.Println("Connected to server.")
  return conn
}


func getMetricMessage() string {
    metricPath := "my.custom.metric"
    nowNanoSec := time.Now().UnixNano()
    metricMessage := fmt.Sprintf("%s %d %d\n",metricPath,nowNanoSec,nowNanoSec/ 1e9)
    return metricMessage  
}
func loopingCallSendMetrics() {
  go func() {
    sleepTimeInt,_ := strconv.Atoi(os.Args[1])
    sleepTime := time.Duration(sleepTimeInt)* time.Millisecond
    host := os.Args[2]
    port,_ := strconv.Atoi(os.Args[3])
    fmt.Println("config: ","sleeptime=",sleepTime," host=",host," port=",port)
  
    conn := initTcpConnection(host,port)

    for { 
      // send to socket
      fmt.Fprintf(conn, getMetricMessage())
      metricsSent.Inc()
      time.Sleep(sleepTime)
    }
  }()
}

func initPrometheusHttpEndpoint() {
  http.Handle("/metrics", promhttp.Handler())
  http.ListenAndServe(":2112", nil)
}
func main() {
  loopingCallSendMetrics()
  initPrometheusHttpEndpoint()
}
