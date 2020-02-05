package gorillaz

import (
	"github.com/gogo/protobuf/proto"
	"github.com/skysoft-atm/gorillaz/stream"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStreamDiscovery(t *testing.T) {
	g := New(WithServiceName("test"), WithMockedServiceDiscovery())
	defer g.Shutdown()
	<-g.Run()

	Log.Info("plop " + g.Env)
	firstStreamName := "first stream name"
	_, err := g.NewStreamProvider(firstStreamName, "stream data type")
	if err != nil {
		t.Fatal(err)
	}

	secondStreamName := "second stream name"
	_, err = g.NewStreamProvider(secondStreamName, "stream data type")
	if err != nil {
		t.Fatal(err)
	}

	streamConsumer, err := g.DiscoverStreamDefinitions("test")
	if err != nil {
		t.Fatal(err)
	}

	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()

	values := make([][]byte, 0)

loop:
	for {
		select {
		case e := <-streamConsumer.EvtChan():
			values = append(values, e.Value)
			if len(values) == 4 { // expecting to receive the first existing streams
				break loop
			}
		case <-timer.C:
			t.Fatal("Stream definition not received in time")
		}
	}

	defs := toStreamDefinitions(t, values)

	assert.True(t, containsStream(defs, firstStreamName))
	assert.True(t, containsStream(defs, secondStreamName))

	thirdStreamName := "third stream name"
	_, err = g.NewStreamProvider(thirdStreamName, "stream data type")
	if err != nil {
		t.Fatal(err)
	}

	e := <-streamConsumer.EvtChan()
	sd := toStreamDefinition(t, e.Value)
	assert.Equal(t, thirdStreamName, sd.Name)

}

func toStreamDefinitions(t *testing.T, values [][]byte) []*stream.StreamDefinition {
	defs := make([]*stream.StreamDefinition, 0)
	for _, v := range values {
		sd := toStreamDefinition(t, v)
		defs = append(defs, &sd)
	}
	return defs
}

func toStreamDefinition(t *testing.T, v []byte) stream.StreamDefinition {
	sd := stream.StreamDefinition{}
	err := proto.Unmarshal(v, &sd)
	if err != nil {
		t.Fatal(err)
	}
	return sd
}

func containsStream(definitions []*stream.StreamDefinition, name string) bool {
	for _, d := range definitions {
		if d.Name == name {
			return true
		}
	}
	return false
}
