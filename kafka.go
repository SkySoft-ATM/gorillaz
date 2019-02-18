package gorillaz

import (
	"context"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/opentracing/opentracing-go"
	"github.com/reactivex/rxgo"
	"github.com/reactivex/rxgo/handlers"
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

// KafkaEnvelope represents Kafka message
type KafkaEnvelope struct {
	Key  []byte
	Data []byte
	Ctx  context.Context
}

type contextKey string

// String() methods in https://tip.golang.org/src/context/context.go reveals a fair amount of information about the context.
// So adding our own String() method is a nice way to keep track of what’s what, although, it would work without it.
func (c contextKey) String() string {
	return "gorillaz" + string(c)
}

// KafkaService returns an observable and an observer of KafkaEnvelope.
func KafkaService(bootstrapServers string, source string, sink string, groupID string) (rxgo.Observable, rxgo.Observer, error) {
	observable := KafkaConsumer(bootstrapServers, source, groupID)
	observer, err := KafkaProducer(bootstrapServers, sink)

	if err != nil {
		return nil, nil, err
	} else {
		return observable, observer, nil
	}
}

// KafkaProducer returns an observer of KafkaEnvelope.
func KafkaProducer(bootstrapServers string, sink string) (rxgo.Observer, error) {
	if bootstrapServers == "" {
		bootstrapServers = viper.GetString("kafka.bootstrapservers")
	}

	Log.Info("Creation of a new Kafka producer",
		zap.String("server", bootstrapServers),
		zap.String("sink", sink))

	brokerList := strings.Split(bootstrapServers, ",")

	producer, err := createKafkaProducer(brokerList)
	if err != nil {
		return nil, err
	}

	observer := rxgo.NewObserver(handlers.NextFunc(func(i interface{}) {
		env := i.(KafkaEnvelope)
		produce(producer, sink, env)
	}))

	return observer, nil
}

// KafkaConsumer returns an observable of KafkaEnvelope.
func KafkaConsumer(bootstrapServers string, source string, groupID string) rxgo.Observable {
	request := make(chan interface{})
	observable := rxgo.FromChannel(request)

	go func() {
		if bootstrapServers == "" {
			bootstrapServers = viper.GetString("kafka.bootstrapservers")
		}

		Log.Info("Creation of a new Kafka consumer",
			zap.String("server", bootstrapServers),
			zap.String("source", source))

		brokerList := strings.Split(bootstrapServers, ",")

		err := consume(brokerList, source, groupID, request)
		if err != nil {
			request <- err
		}
	}()

	return observable
}

func consume(brokerList []string, source string, groupID string, request chan interface{}) error {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.ChannelBufferSize = 1024
	config.Group.Return.Notifications = true
	topics := []string{source}

	consumer, err := cluster.NewConsumer(brokerList, groupID, topics, config)
	if err != nil {
		return err
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Errors
	go func() {
		for err := range consumer.Errors() {
			Log.Error("Error while consuming messages",
				zap.Error(err))
			panic(err)
		}
	}()

	// Notifications
	go func() {
		for notif := range consumer.Notifications() {
			Sugar.Debugf("New notification: %v", notif)
		}
	}()

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

					request <- KafkaEnvelope{
						Key:  msg.Key,
						Data: msg.Value,
						Ctx:  ctx,
					}
				} else {
					Log.Error("Consumer error", zap.Error(err))
				}
			case <-signals:
				continue
			}
		}
	}()
	return nil
}

func produce(producer sarama.AsyncProducer, sink string, env KafkaEnvelope) {
	var headers []sarama.RecordHeader
	if env.Ctx != nil {
		span := env.Ctx.Value(Span).(opentracing.Span)
		headers = inject(span)
	}
	send(producer, sink, headers, env.Key, env.Data)
}

func createKafkaProducer(brokerList []string) (sarama.AsyncProducer, error) {
	// Producer
	producerConfig := sarama.NewConfig()
	producerConfig.Version = sarama.V2_0_0_0
	p, err := sarama.NewAsyncProducer(brokerList, producerConfig)
	if err != nil {
		Log.Error("Error while creating Kafka producer",
			zap.Error(err))
		return nil, err
	}

	go func() {
		for err := range p.Errors() {
			Log.Error("Error while producing message",
				zap.Error(err))
			panic(err)
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
