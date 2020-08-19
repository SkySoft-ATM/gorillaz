package gorillaz

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"time"
)

type MsgHandler func(event stream.Event) error

type NatsConsumerOpts struct {
	ManualAck bool
	Pull bool
	Consumer string
}

type NatsConsumerOpt func(n *NatsConsumerOpts)

// WithManuelAck disable automatic message acknowledgement
func WithManualAck() NatsConsumerOpt {
	return func(o *NatsConsumerOpts){
		o.ManualAck = true
	}
}

// WithPull switches to NATS pull consumption instead of push consumption
func WithPull(consumer string) NatsConsumerOpt {
	return func(o *NatsConsumerOpts){
		o.Pull = true
		o.Consumer = consumer
	}
}

func (g *Gaz) ConsumeNatsSubject(subject string, handler MsgHandler, opts ...NatsConsumerOpt) (*NatsSubscription, error) {
	c := &NatsConsumerOpts{
		ManualAck: false,
	}
	for _,o := range opts {
		o(c)
	}
	if g.NatsConn == nil {
		return nil, fmt.Errorf("gorillaz nats connection is nil, cannot consume stream")
	}


	if c.Pull {
		// if pull mode, the mechanism is very different
		subj := "$JS.API.CONSUMER.MSG.NEXT."+subject+"." + c.Consumer
		ack := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			for {
				if ctx.Err() != nil {
					return
				}
				msg, err := g.NatsConn.Request(subj, nil, time.Second)
				if err == nats.ErrTimeout {
					continue
				}
				if err != nil {
					Log.Error("failed to pull next message", zap.String("stream", subject), zap.String("ocnsumer", c.Consumer), zap.Error(err))
					continue
				}
				var evt stream.StreamEvent
				var e stream.Event
				// try to deserialize object
				err = proto.Unmarshal(msg.Data, &evt)
				if err != nil {
					// that may not be a StreamEvent
					// take the raw payload as it comes
					e = stream.Event{Ctx: context.Background(), Value: msg.Data}
				} else {
					e = stream.Event{Ctx: stream.MetadataToContext(evt.Metadata), Key: evt.Key, Value: evt.Value}
				}

				if c.ManualAck {
					e.AckFunc = func() error {
						ack <- struct{}{}
						return msg.Respond(nil)
					}
				}
				err = handler(e)
				if err == nil && !c.ManualAck {
					if err := msg.Respond(nil); err != nil {
						Log.Error("failed to ack msg", zap.Error(err))
					}
				}

				// wait for msg ack
				if c.ManualAck {
					<- ack
				}
			}
		}()
		return &NatsSubscription{pullCancel: cancel, subject: subject}, nil
	}

	sub,err := g.NatsConn.Subscribe(subject, func(m *nats.Msg){
		var evt stream.StreamEvent
		var e stream.Event
		// try to deserialize object
		err := proto.Unmarshal(m.Data, &evt)
		if err != nil {
			// that may not be a StreamEvent
			// take the raw payload as it comes
			e = stream.Event{Ctx: context.Background(), Value: m.Data}
		} else {
			e = stream.Event{Ctx: stream.MetadataToContext(evt.Metadata), Key: evt.Key, Value: evt.Value}
		}

		if c.ManualAck && m.Reply != "" {
			e.AckFunc = func() error{
				return m.Respond(nil)
			}
		}

		err = handler(e)
		if err == nil {
			if m.Reply != "" && !c.ManualAck {
				// TODO: not great for the consumer, he may receive the same event multiple times and really be aware
				if err := m.Respond(nil); err != nil {
					Log.Error("failed to ack event", zap.Error(err))
				}
			}
		}
	})
	if err == nil {
		return &NatsSubscription{n: sub, subject: sub.Subject}, nil
	}
	return nil, err
}

type NatsSubscription struct {
	subject string
	n *nats.Subscription
	pullCancel context.CancelFunc
}

func (n *NatsSubscription) Unsubscribe() error {
	if n.pullCancel != nil {
		n.pullCancel()
		return nil
	}
	return n.n.Unsubscribe()
}

func (n *NatsSubscription) Subject() string {
	return n.subject
}