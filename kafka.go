package gorillaz

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/opentracing/opentracing-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"os"
	"os/signal"
)

// KafkaHeaders represents a context key.
// Here we can be sure that no two context keys can ever collide (even if the underlying names are the same in different packages)
// which is pretty important when you consider potentially lots of different components could be using the same context for
// different (or similar) things.
const KafkaHeaders = contextKey("headers")

// Span is used to manage OpenTracing context
const Span = "span"

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

// Returns the Kafka bootstrap server configuration of panic if not found
func BootsrapServerConfig() []string {
	bootstrapServers := viper.GetStringSlice("kafka.bootstrapservers")
	if bootstrapServers == nil || len(bootstrapServers) == 0 {
		panic(fmt.Errorf("Please provide a 'kafka.bootstrapservers' configuration"))
	}
	return bootstrapServers
}

type KafkaService struct {
	Consumer *KafkaConsumer
	Producer *KafkaProducer
}

type KafkaConsumer struct {
	Messages      <-chan *KafkaEnvelope
	Notifications <-chan *cluster.Notification
	Errors        <-chan error
}

type KafkaProducer struct {
	Sink   chan<- *KafkaEnvelope
	Errors <-chan *sarama.ProducerError
}

// KafkaService returns an observable and an observer of KafkaEnvelope.
func NewKafkaService(bootstrapServers []string, source string, sink string, groupID string) (*KafkaService, error) {
	consumer, err := NewKafkaConsumer(bootstrapServers, source, groupID)
	producer, err := NewKafkaProducer(bootstrapServers, sink)

	if err != nil {
		return nil, err
	}

	return &KafkaService{consumer, producer}, nil
}

// KafkaProducer returns an observer of KafkaEnvelope.
func NewKafkaProducer(bootstrapServers []string, sink string, options ...KafkaProducerOptionFunc) (*KafkaProducer, error) {
	Log.Info("Creation of a new Kafka producer",
		zap.Strings("server", bootstrapServers),
		zap.String("sink", sink))

	producer, err := createKafkaProducer(bootstrapServers, options...)
	if err != nil {
		return nil, err
	}

	msgProducer := make(chan *KafkaEnvelope)
	errChan := produce(producer, sink, msgProducer)

	return &KafkaProducer{msgProducer, errChan}, nil
}

// KafkaConsumer returns:
// - a channel to consume Kafka messages
// - a channel to consume Kafka notifications
// - a channel to consume errors
// - an error if the consumer could not be created
func NewKafkaConsumer(bootstrapServers []string, source string, groupID string, options ...KafkaConsumerOptionFunc) (*KafkaConsumer, error) {
	Log.Info("Creation of a new Kafka consumer",
		zap.Strings("server", bootstrapServers),
		zap.String("source", source))

	return consume(bootstrapServers, source, groupID, options...)
}

func consume(brokerList []string, source string, groupID string, options ...KafkaConsumerOptionFunc) (*KafkaConsumer, error) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.ChannelBufferSize = 1024
	config.Group.Return.Notifications = true
	config.Version = sarama.V2_0_0_0

	for _, option := range options {
		if err := option(config); err != nil {
			return nil, err
		}
	}

	topics := []string{source}

	consumer, err := cluster.NewConsumer(brokerList, groupID, topics, config)
	if err != nil {
		return nil, err
	}

	messages := make(chan *KafkaEnvelope)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Messages
	go func() {
		for {
			select {
			case msg, ok := <-consumer.Messages():
				if ok {
					Sugar.Debugf("Message received: key=%v, offset=%v, partition=%v, headers=%v",
						msg.Key,
						msg.Offset,
						msg.Partition,
						msg.Headers)
					consumer.MarkOffset(msg, "") // mark message as processed

					// should not use basic type string as key in context.WithValue
					ctx := context.WithValue(context.TODO(), KafkaHeaders, msg.Headers)

					messages <- &KafkaEnvelope{
						Key:  msg.Key,
						Data: msg.Value,
						Ctx:  ctx,
					}
				} else {
					errMsg := "kafka consumer message channel is closed"
					Log.Error(errMsg)
					panic(fmt.Errorf(errMsg))
				}
			case <-signals:
				Log.Info("Kafka consumer stopped by Interrupt signal")
				break
			}
		}
	}()
	return &KafkaConsumer{messages, consumer.Notifications(), consumer.Errors()}, nil
}

func produce(producer sarama.AsyncProducer, sink string, messages <-chan *KafkaEnvelope) <-chan *sarama.ProducerError {
	errorChan := make(chan *sarama.ProducerError)
	go func() {
		for {
			select {
			case env := <-messages:
				var headers []sarama.RecordHeader
				if env.Ctx != nil {
					span := env.Ctx.Value(Span).(opentracing.Span)
					headers = inject(span)
				}
				send(producer, sink, headers, env.Key, env.Data)
			case success := <-producer.Successes():
				Sugar.Debugf("Message successfully pushed to Kafka topic: %s Partition: %d Offset: %d", success.Topic, success.Partition, success.Offset)
			case err := <-producer.Errors():
				Sugar.Debugf("Message could not be pushed to Kafka %v", err.Msg, err.Err)
				errorChan <- err
			}
		}

		for env := range messages {
			var headers []sarama.RecordHeader
			if env.Ctx != nil {
				span := env.Ctx.Value(Span).(opentracing.Span)
				headers = inject(span)
			}
			send(producer, sink, headers, env.Key, env.Data)
		}
	}()
	return errorChan
}

func createKafkaProducer(brokerList []string, options ...KafkaProducerOptionFunc) (sarama.AsyncProducer, error) {
	// Producer
	producerConfig := sarama.NewConfig()
	producerConfig.Version = sarama.V2_0_0_0
	for _, option := range options {
		if err := option(producerConfig); err != nil {
			return nil, err
		}
	}

	p, err := sarama.NewAsyncProducer(brokerList, producerConfig)
	if err != nil {
		Log.Error("Error while creating Kafka producer",
			zap.Error(err))
		return nil, err
	}

	go func() {
		for {
			select {
			case err := <-p.Errors():
				Log.Error("Error while producing message", zap.Error(err))
				panic(err)
			case success := <-p.Successes():
				Log.Debug("success producing message", zap.Int64("Offset", success.Offset))
			}
		}
	}()

	return p, nil
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
