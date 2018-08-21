package gorillaz

import (
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
	"go.uber.org/zap"
	"strings"
	"github.com/spf13/viper"
)

var sink string
var producer sarama.AsyncProducer
var consumer sarama.Consumer

func KafkaService(source string, sink string, groupId string,
	handler func(request chan KafkaEnvelope, reply chan KafkaEnvelope)) error {

	bootstrapServers := viper.GetString("kafka.bootstrapservers")

	Log.Info("Creation of a new Kafka service",
		zap.String("server", bootstrapServers),
		zap.String("source", source),
		zap.String("sink", sink))

	sink = sink
	brokerList := strings.Split(bootstrapServers, ",")
	kafkaConfiguration(brokerList, source)

	// Trigger goroutines
	request := make(chan KafkaEnvelope)
	reply := make(chan KafkaEnvelope)
	for i := 0; i < Cores(); i++ {
		go handler(request, reply)
	}

	//Get number of partitions
	partitions, err := consumer.Partitions(source)
	if err != nil {
		Log.Error("Error while fetching Kafka partitions",
			zap.String("topic", source),
			zap.Error(err))
		return err
	}

	// Consume messages
	go func() {
		for _, partition := range partitions {
			pc, err := consumer.ConsumePartition(source, partition, sarama.OffsetNewest)
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

			go func(pc sarama.PartitionConsumer) {
				for error := range pc.Errors() {
					Log.Error("Error while consuming data",
						zap.Error(error))
				}
			}(pc)
		}
	}()

	for r := range reply {
		if err != nil {
			Log.Error("Error while marshalling message", zap.Error(err))
			continue
		}

		uuid := uuid.NewV4()
		send(uuid.String(), r.Data)
	}

	return nil
}

func kafkaConfiguration(brokerList []string, source string) error {
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

func send(key string, value []byte) {
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
