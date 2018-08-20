package gorillaz

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/satori/go.uuid"
	"go.uber.org/zap"
)

var sink string
var producer *kafka.Producer
var consumer *kafka.Consumer

func KafkaService(bootstrapServers string, kafkaSource string, kafkaSink string,
	handler func(request chan KafkaEnvelope, reply chan KafkaEnvelope)) error {

	Log.Info("Creation of a new Kafka service",
		zap.String("server", bootstrapServers),
		zap.String("source", kafkaSource),
		zap.String("sink", kafkaSink))

	sink = kafkaSink
	err := kafkaConfiguration(bootstrapServers, kafkaSource)
	if err != nil {
		return err
	}

	// Trigger goroutines
	request := make(chan KafkaEnvelope)
	reply := make(chan KafkaEnvelope)
	for i := 0; i < Cores(); i++ {
		go handler(request, reply)
	}

	go func() {
		consumer.SubscribeTopics([]string{kafkaSource}, nil)

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
	}()

	for r := range reply {
		data := r.Data
		span := r.Ctx.Value(Span).(opentracing.Span)

		if err != nil {
			Log.Error("Error while marshalling message", zap.Error(err))
			continue
		}

		headers := inject(span)
		uuid := uuid.NewV4()
		Send(uuid.Bytes(), data, headers)

		Trace(span, log.String("log", "Message sent"))
	}

	return nil
}

func kafkaConfiguration(bootstrapServers string, kafkaSource string) error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "ms-asterix-armap",
		"auto.offset.reset": "latest",
	})
	if err != nil {
		Log.Error("Error while creating Kafka consumer",
			zap.Error(err))
		return err
	}
	consumer = c

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		Log.Error("Error while creating Kafka consumer",
			zap.Error(err))
		return err
	}
	producer = p

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

	Log.Info("Kafka configuration set")

	return nil
}

func Send(key []byte, value []byte, headers []kafka.Header) {
	Log.Debug("Sending message to Kafka",
		zap.ByteString("key", key),
		zap.ByteString("value", value),
		zap.String("topic", sink))

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &sink, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Headers:        headers,
	}, nil)

	if err != nil {
		Log.Error("Error while sending Kafka message", zap.Error(err))
	}
}
