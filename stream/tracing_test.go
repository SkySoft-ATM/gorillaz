package stream

import (
	"testing"
)

func TestTimestamp(t *testing.T) {

	ts := int64(1234567890)

	metadata := &Metadata{
		StreamTimestamp: ts,
	}
	ctx := metadataToContext(metadata)
	evt := &Event{
		Ctx: ctx,
	}

	if StreamTimestamp(evt) != ts {
		t.Errorf("expected evt timestamp to be %d but is %d", ts, StreamTimestamp(evt))
	}
}
