package gorillaz

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/satori/go.uuid"
	"go.uber.org/zap"
	"github.com/spf13/viper"
	"os"
	"os/signal"
)

func KafkaService(source string, sink string, groupId string,
	handler func(request chan KafkaEnvelope, reply chan KafkaEnvelope)) error {

	bootstrapServers := viper.GetString("kafka.bootstrapservers")

	Log.Info("Creation of a new Kafka service",
		zap.String("server", bootstrapServers),
		zap.String("source", source),
		zap.String("sink", sink))

	consumer, err := kafkaConsumer(bootstrapServers, source, groupId)
	if err != nil {
		return err
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(){
		for range c {
			Log.Info("Closing consumer")
			consumer.Close()
			return
		}
	}()

	producer, err := kafkaProducer(bootstrapServers)
	if err != nil {
		return err
	}

	// Trigger goroutines
	request := make(chan KafkaEnvelope)
	reply := make(chan KafkaEnvelope)
	for i := 0; i < Cores(); i++ {
		go handler(request, reply)
	}

	go consume(consumer, source, request)
	go produce(producer, sink, reply)

	return nil
}

func consume(consumer *kafka.Consumer, source string, request chan KafkaEnvelope)  {
	consumer.SubscribeTopics([]string{source}, nil)

	for {
		message, err := consumer.ReadMessage(-1)

		if err != nil {
			Log.Error("Error while reading Kafka message",
				zap.Error(err))
		}

		ctx := context.WithValue(nil, KafkaHeaders, message.Headers)

		request <- KafkaEnvelope{
			Data: message.Value,
			Ctx:  ctx,
		}
	}
}

func produce(producer *kafka.Producer, sink string, reply chan KafkaEnvelope)  {
	for r := range reply {
		data := r.Data
		span := r.Ctx.Value(Span).(opentracing.Span)

		headers := inject(span)
		uuid := uuid.NewV4()
		send(producer, uuid.Bytes(), data, headers, sink)

		Trace(span, log.String("log", "Message sent"))
	}
}

func kafkaConsumer(bootstrapServers string, source string, groupId string) (*kafka.Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupId,
		"auto.offset.reset": "latest",
	})
	if err != nil {
		Log.Error("Error while creating Kafka consumer",
			zap.Error(err))
		return nil, err
	}

	Log.Info("Kafka consumer created", zap.String("source", source))

	return c, nil
}

func kafkaProducer(bootstrapServers string) (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		Log.Error("Error while creating Kafka consumer",
			zap.Error(err))
		return nil, err
	}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					Log.Error("Error while sending Kafka message", zap.Error(ev.TopicPartition.Error))
				} else {
					Log.Debug("Kafka message sent")
				}
			}
		}
	}()

	Log.Info("Kafka producer created")

	return p, nil
}

func send(p *kafka.Producer, key []byte, value []byte, headers []kafka.Header, sink string) {
	Log.Debug("Sending message to Kafka",
		zap.ByteString("key", key),
		zap.ByteString("value", value),
		zap.String("topic", sink))

	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &sink, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Headers:        headers,
	}, nil)

	if err != nil {
		Log.Error("Error while sending Kafka message", zap.Error(err))
	}
}
