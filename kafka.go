package gorillaz

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
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
	handler func(in chan KafkaEnvelope, out chan KafkaEnvelope)) error {

	if bootstrapServers == "" {
		bootstrapServers = viper.GetString("kafka.bootstrapservers")
	}

	Log.Info("Creation of a new Kafka service",
		zap.String("server", bootstrapServers),
		zap.String("source", source),
		zap.String("sink", sink))

	brokerList := strings.Split(bootstrapServers, ",")

	consumer, err := createKafkaConsumer(brokerList)
	if err != nil {
		return err
	}

	producer, err := createKafkaProducer(brokerList)
	if err != nil {
		return err
	}

	// Trigger handlers
	request := make(chan KafkaEnvelope)
	reply := make(chan KafkaEnvelope)
	for i := 0; i < Cores(); i++ {
		go handler(request, reply)
	}

	consume(consumer, source, request)
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
	handler func(in chan KafkaEnvelope)) error {

	if bootstrapServers == "" {
		bootstrapServers = viper.GetString("kafka.bootstrapservers")
	}

	Log.Info("Creation of a new Kafka consumer",
		zap.String("server", bootstrapServers),
		zap.String("source", source))

	brokerList := strings.Split(bootstrapServers, ",")

	consumer, err := createKafkaConsumer(brokerList)
	if err != nil {
		return err
	}

	// Trigger handlers
	request := make(chan KafkaEnvelope)
	for i := 0; i < Cores(); i++ {
		go handler(request)
	}

	consume(consumer, source, request)

	return nil
}

func consume(consumer sarama.Consumer, source string, request chan KafkaEnvelope) error {
	partitions, err := consumer.Partitions(source)
	if err != nil {
		Log.Error("Error while fetching Kafka partitions",
			zap.String("topic", source),
			zap.Error(err))
		return err
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(source, partition, sarama.OffsetNewest)
		if err != nil {
			Log.Error("Error while fetching consuming partition", zap.Error(err))
		}
		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				ctx := context.WithValue(nil, KafkaHeaders, message.Headers)

				request <- KafkaEnvelope{
					Key:  message.Key,
					Data: message.Value,
					Ctx:  ctx,
				}
			}
		}(pc)

		go func(pc sarama.PartitionConsumer) {
			for error := range pc.Errors() {
				Log.Error("Error while consuming data",
					zap.Error(error))
			}
		}(pc)
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
		}
	}()

	return p, nil
}

func createKafkaConsumer(brokerList []string) (sarama.Consumer, error) {
	// Consumer
	consumerConfig := sarama.NewConfig()
	c, err := sarama.NewConsumer(brokerList, consumerConfig)
	if err != nil {
		Log.Error("Error while creating Kafka consumer",
			zap.Error(err))
		return nil, err
	}
	Log.Info("Kafka configuration set")

	return c, nil
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
