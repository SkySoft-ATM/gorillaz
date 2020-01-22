package gorillaz

import (
	"context"
	"github.com/gorilla/websocket"
	"github.com/skysoft-atm/gorillaz/mux"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"time"
)

// serves a file as an http response
func ServeFileFunc(file string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		b, err := ioutil.ReadFile(filepath.Clean(file))
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusOK)
			_, err = w.Write(b)
			if err != nil {
				Log.Debug("Could not write response on http connection", zap.Error(err))
			}
		}
	}
}

type WebsocketConfig struct {
	WriteWait  time.Duration // Time allowed to write a message to the peer.
	PongWait   time.Duration // Time allowed to read the next pong message from the peer.
	PingPeriod time.Duration // Send pings to peer with this period. Must be less than pongWait.
}

type WebsocketOption func(*WebsocketConfig)

// Websocket message
type WsMessage struct {
	MessageType int
	Data        []byte
}

// Upgrades the http request to a websocket connection and returns
// - a channel to publish websocket messages
// - a channel signaling that an error occurred and the publication has stopped
// - an error if the upgrade was not successful
func UpgradeToWebsocketWithContext(rw http.ResponseWriter, req *http.Request, opts ...WebsocketOption) (chan<- *WsMessage, <-chan struct{}, error) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	c := WebsocketConfig{
		WriteWait:  10 * time.Second,
		PongWait:   60 * time.Second,
		PingPeriod: (60 * time.Second * 9) / 10, // must be less than PongWait
	}
	for _, o := range opts {
		o(&c)
	}
	messageChan := make(chan *WsMessage, 1000)
	conn, err := upgrader.Upgrade(rw, req, nil)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	err = conn.SetReadDeadline(time.Now().Add(c.PongWait))
	if err != nil {
		Log.Debug("Could not set read deadline", zap.Error(err))
	}
	conn.SetPongHandler(func(string) error { _ = conn.SetReadDeadline(time.Now().Add(c.PongWait)); return nil })
	go readLoop(ctx, cancel, conn, c.PongWait)

	go func() {
		ticker := time.NewTicker(c.PingPeriod)
		defer func() {
			ticker.Stop()
			cancel()
			err := conn.Close()
			if err != nil {
				Log.Debug("Could not close websocket connection", zap.Error(err))
			}
		}()
		for {
			select {
			case msg, ok := <-messageChan:
				if !ok {
					Log.Info("Websocket publication stopped by producer")
					return
				}
				err := conn.SetWriteDeadline(time.Now().Add(c.WriteWait))
				if err != nil {
					Log.Debug("Could not set write deadline on websocket connection", zap.Error(err))
				}
				if err := conn.WriteMessage(msg.MessageType, msg.Data); err != nil {
					Log.Debug("Could not write message over websocket", zap.Error(err))
					return
				}
			case <-ticker.C:
				err := conn.SetWriteDeadline(time.Now().Add(c.WriteWait))
				if err != nil {
					Log.Debug("Could not set write deadline on websocket connection", zap.Error(err))
				}
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					Log.Debug("Could not send ping message over websocket", zap.Error(err))
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return messageChan, ctx.Done(), nil
}

func readLoop(ctx context.Context, cancel context.CancelFunc, c *websocket.Conn, readTimeout time.Duration) {
	for {
		if _, _, err := c.NextReader(); err != nil { // we will get an error if a 'close' control message is received
			cancel()
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := c.SetReadDeadline(time.Now().Add(readTimeout))
		if err != nil {
			Log.Debug("Could not set read deadline", zap.Error(err))
		}
	}
}

// Publishes the given broadcaster on a websocket, the buffer size configures the buffer on the channel reading from the broadcaster
func PublishPeriodicallyOverWebsocket(supplier func() *WsMessage, period time.Duration, opts ...WebsocketOption) func(w http.ResponseWriter, r *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		ws, stopChan, err := UpgradeToWebsocketWithContext(rw, req, opts...)
		if err != nil {
			Log.Error("Error on connectionStateSubscription", zap.Error(err))
			return
		}
		defer close(ws)

		tick := time.NewTicker(period)

		for {
			select {
			case <-tick.C:
				wsm := supplier()
				if wsm != nil {
					ws <- wsm
				}
			case <-stopChan:
				tick.Stop()
				return
			}
		}
	}
}

// Publishes the given broadcaster on a websocket, the buffer size configures the buffer on the channel reading from the broadcaster
func PublishOverWebsocket(sb *mux.Broadcaster, bufferSize int, transform func(interface{}) *WsMessage, opts ...WebsocketOption) func(w http.ResponseWriter, r *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		ws, stopChan, err := UpgradeToWebsocketWithContext(rw, req, opts...)
		if err != nil {
			Log.Error("Error on connectionStateSubscription", zap.Error(err))
			return
		}
		defer close(ws)

		updateChan := make(chan interface{}, bufferSize)
		sb.Register(updateChan)
		defer sb.Unregister(updateChan)
		if err != nil {
			Log.Error("Unable to register state update channel", zap.Error(err))
			return
		}
		for {
			select {
			case u, ok := <-updateChan:
				if !ok {
					return
				}
				wsm := transform(u)
				if wsm != nil {
					ws <- wsm
				}
			case <-stopChan:
				return
			}
		}
	}
}

// Publishes the given state broadcaster on a websocket, the buffer size configures the buffer on the channel reading from the state broadcaster
func PublishStateOverWebsocket(sb *mux.StateBroadcaster, bufferSize int, transform func(*mux.StateUpdate) *WsMessage, opts ...WebsocketOption) func(w http.ResponseWriter, r *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		ws, stopChan, err := UpgradeToWebsocketWithContext(rw, req, opts...)
		if err != nil {
			Log.Error("Error on connectionStateSubscription", zap.Error(err))
			return
		}
		defer close(ws)

		updateChan := make(chan *mux.StateUpdate, bufferSize)
		sb.Register(updateChan)
		defer sb.Unregister(updateChan)
		for {
			select {
			case u, ok := <-updateChan:
				if !ok {
					return
				}
				wsm := transform(u)
				if wsm != nil {
					ws <- wsm
				}
			case <-stopChan:
				return
			}
		}
	}
}
