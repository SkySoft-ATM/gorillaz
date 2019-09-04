package gorillaz

import (
	"net/http"
	"sync/atomic"
)

// InitHealthcheck registers /live and /ready (GET) for liveness and readiness probes in k8s
func (g *Gaz) InitHealthcheck() {
	ready := func(w http.ResponseWriter, _ *http.Request) {
		if atomic.LoadInt32(g.isReady) == 1 {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}

	live := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	g.Router.HandleFunc("/ready", ready).Methods("GET")
	g.Router.HandleFunc("/live", live).Methods("GET")
}

// SetReady returns the actual internal state to precise if the given microservice is ready
func (g *Gaz) SetReady(status bool) {
	var statusInt int32
	if status {
		statusInt = 1
	}
	atomic.StoreInt32(g.isReady, statusInt)
}
