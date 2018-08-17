package gorillaz

import (
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
	"go.uber.org/zap"
	"strings"
)

var sink string
var producer sarama.AsyncProducer
var consumer sarama.Consumer

func KafkaService(bootstrapServers string, kafkaSource string, kafkaSink string,
	handler func(chan KafkaEnvelope, chan KafkaEnvelope)) error {
	sink = kafkaSink
	brokerList := strings.Split(bootstrapServers, ",")
	kafkaConfiguration(brokerList, kafkaSource)

	// Trigger goroutines
	request := make(chan KafkaEnvelope)
	reply := make(chan KafkaEnvelope)
	for i := 0; i < Cores(); i++ {
		go handler(request, reply)
	}

	//Get number of partitions
	partitions, err := consumer.Partitions(kafkaSource)
	if err != nil {
		Log.Error("Error while fetching Kafka partitions",
			zap.String("topic", kafkaSource),
			zap.Error(err))
		return err
	}

	// Consume messages
	go func() {
		for _, partition := range partitions {
			pc, err := consumer.ConsumePartition(kafkaSource, partition, sarama.OffsetNewest)
			if err != nil {
				Log.Error("Error while fetching consuming partition", zap.Error(err))
			}
			go func(pc sarama.PartitionConsumer) {
				for message := range pc.Messages() {
					request <- KafkaEnvelope{
						Data: message.Value,
					}
				}
			}(pc)
		}
	}()

	for r := range reply {
		if err != nil {
			Log.Error("Error while marshalling message", zap.Error(err))
			continue
		}

		uuid, _ := uuid.NewV4()
		Send(uuid.String(), r.Data)
	}

	return nil
}

func kafkaConfiguration(brokerList []string, kafkaSource string) error {
	// Producer
	producerConfig := sarama.NewConfig()
	p, err := sarama.NewAsyncProducer(brokerList, producerConfig)
	if err != nil {
		Log.Error("Error while creating Kafka producer",
			zap.Error(err))
		return err
	}

	go func() {
		for err := range p.Errors() {
			Log.Error("Error while producing message",
				zap.Error(err))
		}
	}()
	producer = p

	// Consumer
	consumerConfig := sarama.NewConfig()
	c, err := sarama.NewConsumer(brokerList, consumerConfig)
	if err != nil {
		Log.Error("Error while creating Kafka consumer",
			zap.Error(err))
		return err
	}
	consumer = c

	Log.Info("Kafka configuration set")

	return nil
}

func Send(key string, value []byte) {
	Log.Debug("Sending message to Kafka",
		zap.String("key", key),
		zap.String("value", string(value)),
		zap.String("topic", sink))

	producer.Input() <- &sarama.ProducerMessage{
		Topic: sink,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
}
