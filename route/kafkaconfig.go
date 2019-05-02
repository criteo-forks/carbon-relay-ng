package route

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)
func getKafkaConfiguration(requiredAcks string, maxRetries int, timeout int, codec string,flushMaxWait time.Duration,flushMaxNum int,bufSize int) (*sarama.Config, error) {
	var err error
	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0
	config.Producer.Flush.Frequency = flushMaxWait //flush every time
	config.Producer.Flush.Messages = flushMaxNum                                     //flush best effort
	config.Producer.Flush.MaxMessages = bufSize                                 //flush max
	config.Producer.RequiredAcks, err = getRequiredAcksConfig(requiredAcks)
	config.Producer.Retry.Max = maxRetries // Retry up to 10 times to produce the message
	config.Producer.Compression, err = getCompressionRaw(codec)
	if err != nil {
		log.Fatalf("kafkaRaw %s", err)
	}
	config.Producer.Return.Successes = true
	config.Producer.Timeout = time.Duration(timeout) * time.Millisecond
	err = config.Validate()
	if err != nil {
		log.Fatalf("kafkaRaw failed to validate kafka config. %s", err)
	}
	return config, err
}

func getCompressionRaw(codec string) (sarama.CompressionCodec, error) {
	switch codec {
	case "none":
		return sarama.CompressionNone, nil
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	}
	return 0, fmt.Errorf("unknown compression codec %q", codec)
}
func getRequiredAcksConfig(requiredAck string) (sarama.RequiredAcks, error) {
	switch requiredAck {
	case "waitForAll":
		return sarama.WaitForAll, nil
	case "noResponse":
		return sarama.NoResponse, nil
	case "waitForLocal":
		return sarama.WaitForLocal, nil
	}
	return 0, fmt.Errorf("unknown required acks kafka config %q", requiredAck)
}
