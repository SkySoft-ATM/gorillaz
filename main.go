package gorillaz

import "flag"

func Init() {
	flag.String("kafka.bootstrapservers", "", "Kafka bootstrap servers")
	flag.String("kafka.source", "", "Kafka source topic")
	flag.String("kafka.sink", "", "Kafka sink topic")

	parseConfiguration()
	InitLogs()
	InitTracing()
}
