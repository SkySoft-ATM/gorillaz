package gorillaz

import (
	"net/http"
	"sync/atomic"
)

// use int32 because sync.atomic package doesn't support boolean out of the box
var isReady int32
var isLive int32

// InitHealthcheck registers /live and /ready (GET) for liveness and readiness probes in k8s
func (g *Gaz) InitHealthcheck() {
	g.router.HandleFunc("/ready", ready).Methods("GET")
	g.router.HandleFunc("/live", live).Methods("GET")
}

// SetReady returns the actual internal state to precise if the given microservice is ready
func (*Gaz) SetReady(status bool) {
	var statusInt int32
	if status {
		statusInt = 1
	}
	atomic.StoreInt32(&isReady, statusInt)
}

// SetLive returns the actual internal state to precise if the given microservice is live
func (*Gaz) SetLive(status bool) {
	var statusInt int32
	if status {
		statusInt = 1
	}
	atomic.StoreInt32(&isLive, statusInt)
}

func ready(w http.ResponseWriter, _ *http.Request) {
	if atomic.LoadInt32(&isReady) == 1 {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func live(w http.ResponseWriter, _ *http.Request) {
	if atomic.LoadInt32(&isLive) == 1 {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}
