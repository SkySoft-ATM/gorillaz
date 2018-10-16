package gorillaz

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/opentracing/opentracing-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"strings"
)

const KafkaHeaders = "headers"
const Span = "span"

type KafkaEnvelope struct {
	Key  []byte
	Data []byte
	Ctx  context.Context
}

func KafkaService(bootstrapServers string, source string, sink string, groupId string,
	handler func(in chan KafkaEnvelope, out chan KafkaEnvelope), parallelism int) error {

	if bootstrapServers == "" {
		bootstrapServers = viper.GetString("kafka.bootstrapservers")
	}

	Log.Info("Creation of a new Kafka service",
		zap.String("server", bootstrapServers),
		zap.String("source", source),
		zap.String("sink", sink))

	brokerList := strings.Split(bootstrapServers, ",")

	producer, err := createKafkaProducer(brokerList)
	if err != nil {
		return err
	}

	// Trigger handlers
	request := make(chan KafkaEnvelope)
	reply := make(chan KafkaEnvelope)

	if parallelism == 0 {
		parallelism = Cores()
	}

	for i := 0; i < parallelism; i++ {
		go handler(request, reply)
	}

	consume(brokerList, source, groupId, request)
	produce(producer, sink, reply)

	return nil
}

func KafkaProducer(bootstrapServers string, sink string) (chan KafkaEnvelope, error) {
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

	reply := make(chan KafkaEnvelope)

	produce(producer, sink, reply)

	return reply, nil
}

func KafkaConsumer(bootstrapServers string, source string, groupId string,
	handler func(in chan KafkaEnvelope), parallelism int) error {

	if bootstrapServers == "" {
		bootstrapServers = viper.GetString("kafka.bootstrapservers")
	}

	Log.Info("Creation of a new Kafka consumer",
		zap.String("server", bootstrapServers),
		zap.String("source", source))

	brokerList := strings.Split(bootstrapServers, ",")

	// Trigger handlers
	request := make(chan KafkaEnvelope)
	if parallelism == 0 {
		parallelism = Cores()
	}

	for i := 0; i < parallelism; i++ {
		go handler(request)
	}

	consume(brokerList, source, groupId, request)

	return nil
}

func consume(brokerList []string, source string, groupId string, request chan KafkaEnvelope) error {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.ChannelBufferSize = 1024
	config.Group.Return.Notifications = true
	topics := []string{source}

	consumer, err := cluster.NewConsumer(brokerList, groupId, topics, config)
	if err != nil {
		panic(err)
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

				ctx := context.WithValue(nil, KafkaHeaders, msg.Headers)
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

	return nil
}

func produce(producer sarama.AsyncProducer, sink string, reply chan KafkaEnvelope) {
	go func() {
		for r := range reply {
			var headers []sarama.RecordHeader
			if r.Ctx != nil {
				span := r.Ctx.Value(Span).(opentracing.Span)
				headers = inject(span)
			}
			send(producer, sink, headers, r.Key, r.Data)
		}
	}()
}

func createKafkaProducer(brokerList []string) (sarama.AsyncProducer, error) {
	// Producer
	producerConfig := sarama.NewConfig()
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
