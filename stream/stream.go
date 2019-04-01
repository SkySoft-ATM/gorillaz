package stream

import (
	"context"
)

type Event struct {
	Ctx        context.Context
	Key, Value []byte
}

const timestampKey = "stream_timestamp_ns"

// StreamTimestamp returns the time when the event was sent from the producer in Epoch in nanoseconds
func StreamTimestamp(e *Event) int64 {
	if e.Ctx == nil {
		return 0
	}
	ts := e.Ctx.Value(timestampKey)
	if ts != nil {
		if res, ok := ts.(int64); ok {
			return res
		}
	}
	return 0
}
