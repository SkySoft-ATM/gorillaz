package gorillaz

import "context"

const KafkaHeaders = "headers"
const Span = "span"

type KafkaEnvelope struct {
	Data []byte
	Ctx  context.Context
}
