package gorillaz

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// KafkaHeaders represents a context key.
// Here we can be sure that no two context keys can ever collide (even if the underlying names are the same in different packages)
// which is pretty important when you consider potentially lots of different components could be using the same context for
// different (or similar) things.
const KafkaHeaders = contextKey("headers")

// Span is used to manage OpenTracing context
const Span = "span"

var (
	kafkaSendCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_send_total",
		Help: "The total number of messages sent to Kafka",
	})

	kafkaSendSuccessCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_send_success_total",
		Help: "The total number of messages sent to Kafka with success",
	})

	kafkaSendErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_send_error_total",
		Help: "The total number of error while sending to Kafka ",
	})

	kafkaReceiveCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_receive_total",
		Help: "The total number of messages received from Kafka",
	})
)

// KafkaEnvelope represents Kafka message
type KafkaEnvelope struct {
	Key  []byte
	Data []byte
	Ctx  context.Context
}

type contextKey string

// KafkaConsumerOptionFunc is a function that configures the Kafka connection.
type KafkaConsumerOptionFunc func(*cluster.Config) error

// KafkaProducerOptionFunc is a function that configures the Kafka connection.
type KafkaProducerOptionFunc func(*sarama.Config) error

// String() methods in https://tip.golang.org/src/context/context.go reveals a fair amount of information about the context.
// So adding our own String() method is a nice way to keep track of what’s what, although, it would work without it.
func (c contextKey) String() string {
	return "gorillaz" + string(c)
}

// BootsrapServerConfig returns the Kafka bootstrap server configuration or panic if not found
func BootsrapServerConfig() []string {
	bootstrapServers := viper.GetStringSlice("kafka.bootstrapservers")
	if bootstrapServers == nil || len(bootstrapServers) == 0 {
		panic(fmt.Errorf("Please provide a 'kafka.bootstrapservers' configuration"))
	}
	return bootstrapServers
}

type KafkaProducer struct {
	Sink   chan<- *KafkaEnvelope
	Errors <-chan *sarama.ProducerError
}

// NewKafkaProducer returns a new KafkaProducer forwarding
func NewKafkaProducer(bootstrapServers []string, sink string, options ...KafkaProducerOptionFunc) (*KafkaProducer, error) {
	Log.Info("Creation of a new Kafka producer",
		zap.Strings("server", bootstrapServers),
		zap.String("sink", sink))

	var p sarama.AsyncProducer
	{
		// Setup Sarama Kafka AsyncProducer
		producerConfig := sarama.NewConfig()
		producerConfig.Version = sarama.V2_0_0_0
		for _, option := range options {
			if err := option(producerConfig); err != nil {
				return nil, err
			}
		}
		var err error
		p, err = sarama.NewAsyncProducer(bootstrapServers, producerConfig)
		if err != nil {
			Log.Error("Error while creating Kafka producer",
				zap.Error(err))
			return nil, err
		}
	}

	msgProducer := make(chan *KafkaEnvelope)
	errChan := produce(p, sink, msgProducer)

	return &KafkaProducer{msgProducer, errChan}, nil
}

// NewKafkaConsumer returns:
// - a channel to consume Kafka messages
// - a channel to consume Kafka notifications
// - a channel to consume errors
// - an error if the consumer could not be created
func NewKafkaConsumer(bootstrapServers []string, topic string, groupID string, options ...KafkaConsumerOptionFunc) (<-chan *KafkaEnvelope, error) {
	Log.Info("Creation of a new Kafka consumer",
		zap.Strings("server", bootstrapServers),
		zap.String("topic", topic))

	config := cluster.NewConfig()
	// we're not interested in offset management errors
	config.Consumer.Return.Errors = false
	// we're not interested in consumer group membership notifications
	config.Group.Return.Notifications = false

	config.ChannelBufferSize = 1024
	config.Version = sarama.V2_0_0_0

	for _, option := range options {
		if err := option(config); err != nil {
			return nil, err
		}
	}

	consumer, err := cluster.NewConsumer(bootstrapServers, groupID, []string{topic}, config)
	if err != nil {
		return nil, err
	}

	messages := make(chan *KafkaEnvelope, 3)

	// Messages
	go func() {
		for {
			select {
			case msg := <-consumer.Messages():
				Log.Debug("Message received",
					zap.String("topic", msg.Topic),
					zap.Int32("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
					zap.String("key", string(msg.Key)),
					zap.String("headers", fmt.Sprintf("%+v", msg.Headers)))
				consumer.MarkOffset(msg, "") // mark message as processed

				// should not use basic type string as key in context.WithValue
				ctx := context.WithValue(context.TODO(), KafkaHeaders, msg.Headers)

				messages <- &KafkaEnvelope{
					Key:  msg.Key,
					Data: msg.Value,
					Ctx:  ctx,
				}
				kafkaReceiveCount.Inc()
			}
		}
	}()
	return messages, nil
}

func produce(producer sarama.AsyncProducer, sink string, messages <-chan *KafkaEnvelope) <-chan *sarama.ProducerError {
	errorChan := make(chan *sarama.ProducerError, 3)

	// send loop goroutine
	go func() {
		for {
			env := <-messages
			var headers []sarama.RecordHeader
			if env.Ctx != nil {
				span := env.Ctx.Value(Span).(opentracing.Span)
				headers = inject(span)
			}
			send(producer, sink, headers, env.Key, env.Data)
			kafkaSendCount.Inc()
		}
	}()

	// success & errors handling goroutine
	go func() {
		for {
			select {
			case success := <-producer.Successes():
				kafkaSendSuccessCount.Inc()
				Log.Debug("Message successfully pushed to Kafka topic",
					zap.String("topic", success.Topic),
					zap.Int32("partition", success.Partition),
					zap.Time("timestamp", success.Timestamp),
					zap.Int64("offset", success.Offset))
			case err := <-producer.Errors():
				kafkaSendErrorCount.Inc()
				Log.Error("Message could not be pushed to Kafka",
					zap.String("topic", err.Msg.Topic),
					zap.Int32("partition", err.Msg.Partition),
					zap.Time("timestamp", err.Msg.Timestamp),
					zap.Error(err.Err))
				errorChan <- err
			}
		}
	}()

	return errorChan
}

func send(producer sarama.AsyncProducer, sink string, headers []sarama.RecordHeader, key []byte, value []byte) {
	Log.Debug("Sending message to Kafka",
		zap.ByteString("key", key),
		zap.String("value", string(value)),
		zap.String("topic", sink))

	producer.Input() <- &sarama.ProducerMessage{
		Topic:   sink,
		Key:     sarama.ByteEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: headers,
	}
}
