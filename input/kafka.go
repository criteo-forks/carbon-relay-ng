package input

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/graphite-ng/carbon-relay-ng/encoding"
	"github.com/graphite-ng/carbon-relay-ng/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

type Kafka struct {
	BaseInput

	topic        string
	groupID      string
	enableTags   bool
	resetOffsets bool

	config *sarama.Config
	client sarama.Client
	cg     sarama.ConsumerGroup

	dispatcher Dispatcher

	contexts map[int32]PartitionContext
	ctx      context.Context
	closed   chan bool
}

func (k *Kafka) Name() string {
	return "kafka"
}

func (k *Kafka) Start(d Dispatcher) error {
	k.Dispatcher = d

	// Check errors
	go func() {
		for err := range k.cg.Errors() {
			k.logger.Error("kafka input error ", zap.Error(err))
		}
	}()

	// Enable sarama logging
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	// Start consumer
	go func(closed chan bool) {
		for {
			select {
			case <-closed:
				return
			default:
			}
			err := k.cg.Consume(context.Background(), strings.Fields(k.topic), k)
			if err != nil {
				k.logger.Error("kafka input error Consume method ", zap.Error(err))
			}
		}
	}(k.closed)
	k.logger.Info("Sarama consumer up and running!...")

	return nil

}

func (k *Kafka) close() {
	err := k.client.Close()
	if err != nil {
		k.logger.Error("kafka input closed with errors.", zap.Error(err))
	} else {
		k.logger.Info("kafka input closed correctly.")
	}
}

func (k *Kafka) Stop() error {
	close(k.closed)
	k.close()
	return nil
}

func NewKafka(brokers []string, topic string, consumerGroup string, kafkaConfig *sarama.Config, h encoding.FormatAdapter, resetOffsets bool, enableTags bool) *Kafka {
	logger := zap.L().With(zap.String("kafka_topic", topic), zap.String("kafka_consumer_group_id", consumerGroup))

	client, err := sarama.NewClient(brokers, kafkaConfig)
	if err != nil {
		logger.Fatal("kafka input init client failed", zap.Error(err))
	}

	cg, err := sarama.NewConsumerGroup(brokers, consumerGroup, kafkaConfig)
	if err != nil {
		logger.Fatal("kafka input init consumer group failed", zap.Error(err))
	} else {
		logger.Info("kafka input init correctly")
	}

	err = metrics.RegisterKafkaConsumerMetrics("sarama", kafkaConfig)
	if err != nil {
		logger.Fatal("Fail to register kafka consumer metrics", zap.Error(err))
	}

	return &Kafka{
		BaseInput:    BaseInput{handler: h, name: fmt.Sprintf("kafka[topic=%s;cg=%s;id=%s]", topic, consumerGroup, kafkaConfig.ClientID), logger: logger},
		config:       kafkaConfig,
		topic:        topic,
		client:       client,
		cg:           cg,
		groupID:      consumerGroup,
		ctx:          context.Background(),
		closed:       make(chan bool),
		enableTags:   enableTags,
		resetOffsets: resetOffsets,
		contexts:     make(map[int32]PartitionContext),
	}
}

// Partition Context is a data structure to hold variables that are tied to a specific partition
type PartitionContext struct {
	messagesConsumedCounter prometheus.Counter
	timeLagGauge            prometheus.Gauge
	offsetLagGauge          prometheus.Gauge
}

func newPartitionContext(partition int32) PartitionContext {
	// promauto automatically register metrics

	messagesConsumedCounter := promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "kafka",
		Subsystem:   "consumer",
		Name:        "messages",
		Help:        "number of message consumed",
		ConstLabels: prometheus.Labels{"partition": fmt.Sprintf("%d", partition)},
	})

	timeLagGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Namespace:   "kafka",
		Subsystem:   "consumer",
		Name:        "time_lag",
		Help:        "consumer time lag in seconds",
		ConstLabels: prometheus.Labels{"partition": fmt.Sprintf("%d", partition)},
	})

	offsetLagGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Namespace:   "kafka",
		Subsystem:   "consumer",
		Name:        "offset_lag",
		Help:        "consumer offset lag",
		ConstLabels: prometheus.Labels{"partition": fmt.Sprintf("%d", partition)},
	})

	return PartitionContext{
		messagesConsumedCounter: messagesConsumedCounter,
		timeLagGauge:            timeLagGauge,
		offsetLagGauge:          offsetLagGauge,
	}
}

func (pc PartitionContext) destroy() {
	prometheus.Unregister(pc.messagesConsumedCounter)
	prometheus.Unregister(pc.timeLagGauge)
	prometheus.Unregister(pc.offsetLagGauge)
}

// Setup is run at the beginning of a new session, before ConsumeClaim
// A new session is setup whenever an error occur in a processing goroutine or when consumers re-balance
func (k *Kafka) Setup(session sarama.ConsumerGroupSession) error {
	// Snapshot assigned partitions
	currentPartitions := make(map[int32]bool)
	for partition := range k.contexts {
		currentPartitions[partition] = true
	}

	// Iterate newly assigned partitions
	for _, partition := range session.Claims()[k.topic] {

		if k.resetOffsets {
			// Reset partition offset to end of the stream
			k.logger.Info("resetting consumer offset", zap.String("partition", fmt.Sprintf("%d", partition)))
			session.ResetOffset(k.topic, partition, sarama.OffsetNewest, "")
		}

		if _, ok := k.contexts[partition]; !ok {
			// New partition : initialize partition scoped context
			k.logger.Debug("initializing context", zap.String("partition", fmt.Sprintf("%d", partition)))
			k.contexts[partition] = newPartitionContext(partition)

		} else {
			// Already existing partition : do nothing
		}

		delete(currentPartitions, partition)
	}

	// Cleanup contexts belonging to partitions that are not assigned to this consumer anymore
	for partition := range currentPartitions {
		k.logger.Debug("cleaning context", zap.String("partition", fmt.Sprintf("%d", partition)))
		k.contexts[partition].destroy()
		delete(k.contexts, partition)
	}

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (k *Kafka) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// Update consumer metrics
func (k *Kafka) monitorConsumerLag(claim sarama.ConsumerGroupClaim, message *sarama.ConsumerMessage) {
	if message.Offset%100 == 0 {
		partitionContext := k.contexts[claim.Partition()]
		partitionContext.messagesConsumedCounter.Inc()
		partitionContext.offsetLagGauge.Set(float64(claim.HighWaterMarkOffset() - message.Offset))
		partitionContext.timeLagGauge.Set(time.Now().Sub(message.Timestamp).Seconds())
	}
}

// Extract graphite tags from kafka headers
var noTags = make(map[string]string)

func (k *Kafka) getKafkaTags(header []*sarama.RecordHeader) map[string]string {
	tags := make(map[string]string)
	for i := 0; i < len(header); i++ {
		key := string(header[i].Key)
		value := string(header[i].Value)
		if ce := k.logger.Check(zap.DebugLevel, "debug kafka tags"); ce != nil {
			ce.Write(zap.String("key", key), zap.String("value", value))
		}
		tags[key] = value
	}
	return tags
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (k *Kafka) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		k.monitorConsumerLag(claim, message)

		if ce := k.logger.Check(zap.DebugLevel, "debug message value"); ce != nil {
			ce.Write(zap.String("partition", fmt.Sprintf("%d", claim.Partition())), zap.ByteString("message", message.Value))
		}

		tags := noTags
		if k.enableTags {
			tags = k.getKafkaTags(message.Headers)
		}

		err := k.handle(message.Value, tags)
		if err != nil {
			k.logger.Error("invalid message from kafka", zap.ByteString("message", message.Value))
		}
	}

	return nil
}
