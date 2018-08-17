package gorillaz

import "context"

const Headers = "headers"
const Span = "span"

type KafkaEnvelope struct {
	Data []byte
	Ctx  context.Context
}
