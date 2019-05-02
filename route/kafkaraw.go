package route

import (
	"bytes"
	"github.com/Shopify/sarama"
	dest "github.com/graphite-ng/carbon-relay-ng/destination"
	"github.com/graphite-ng/carbon-relay-ng/matcher"
	instMetrics "github.com/graphite-ng/carbon-relay-ng/metrics"
	log "github.com/sirupsen/logrus"
	"time"
)

type KafkaRaw struct {
	baseRoute
	buf      chan []byte
	dispatch func(chan []byte, []byte)
}

// NewKafkaMdm creates a special route that writes to a grafana.net datastore
// We will automatically run the route and the destination
func NewKafkaRaw(key, prefix, sub, regex, topic, codec string, maxRetries int, requiredAcks string, brokers []string, bufSize, flushMaxNum, flushMaxWait, timeout int, blocking bool) (Route, error) {
	m, err := matcher.New(prefix, sub, regex)

	if err != nil {
		return nil, err
	}
	reader := &KafkaRaw{
		baseRoute: *newBaseRoute(key, "kafkaRaw"),
		buf:       make(chan []byte, bufSize),
	}
	reader.rm.Buffer.Size.Set(float64(bufSize))
	if blocking {
		reader.dispatch = reader.dispatchBlocking
	} else {
		reader.dispatch = reader.dispatchNonBlocking
	}
	kafkaConfig, err := getKafkaConfiguration(requiredAcks, maxRetries, timeout, codec, time.Duration(flushMaxWait)*time.Millisecond, flushMaxNum, bufSize)
	reader.config.Store(baseConfig{*m, make([]*dest.Destination, 0)})
	kafkaProducer, err := getKafkaProducer(brokers, kafkaConfig)
	go reader.run(kafkaProducer, topic)
	go reader.instrumentsKafka(kafkaProducer)
	go displayErrorsProducer(kafkaProducer)

	return reader, nil
}

func closeKafkaProducer(kafkaProducer sarama.AsyncProducer) {
	err := kafkaProducer.Close()
	if err == nil {
		log.Infoln("kafka producer closed successfully")

	} else {
		log.Fatalln("kafka producer closed failed ", err)
	}

}
func getKafkaProducer(brokers []string, kafkaConfig *sarama.Config) (sarama.AsyncProducer, error) {
	producer, err := sarama.NewAsyncProducer(brokers, kafkaConfig)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}
	return producer, err
}

func (r *KafkaRaw) run(producer sarama.AsyncProducer, topic string) {
	defer closeKafkaProducer(producer)
	for metric := range r.buf {
		producer.Input() <- &sarama.ProducerMessage{
			Key:       sarama.ByteEncoder(metric[:bytes.IndexByte(metric, ' ')]),
			Topic:     topic,
			Value:     sarama.ByteEncoder(metric),
			Timestamp: time.Now(),
		}
	}
}
func (r *KafkaRaw) instrumentsKafka(producer sarama.AsyncProducer) {
	for returnMetric := range producer.Successes() {
		r.rm.Buffer.BufferedMetrics.Dec()
		size := returnMetric.Value.Length()
		r.rm.OutMetrics.Add(float64(size))
		r.rm.OutBatches.Inc()
		latency := time.Now().UnixNano() - returnMetric.Timestamp.UnixNano()
		r.rm.Buffer.ObserveFlush(time.Duration(latency), int64(size), instMetrics.FlushTypeTicker)
		log.Traceln("metric latency", latency, " metric length ", size)
	}
}

func displayErrorsProducer(producer sarama.AsyncProducer) {
	for error := range producer.Errors() {
		log.Errorln("kafka producer error ", error.Error())
	}

}

func (r *KafkaRaw) Dispatch(buf []byte) {
	log.Tracef("kafka %q: sending %s", r.key, buf)
	r.dispatch(r.buf, buf)
}

func (r *KafkaRaw) Flush() error {
	//conf := r.config.Load().(Config)
	// no-op. Flush() is currently not called by anything.
	return nil
}

func (r *KafkaRaw) Shutdown() error {
	close(r.buf)
	return nil
}

func (r *KafkaRaw) Snapshot() Snapshot {
	return makeSnapshot(&r.baseRoute)
}
