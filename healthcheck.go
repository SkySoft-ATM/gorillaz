package gorillaz

import (
	"fmt"
	"net/http"

	"github.com/apex/log"
	"github.com/gorilla/mux"
)

var isReady = false
var isLive = false

// InitHealthcheck starts two http endpoints (GET) for liveness and readiness probes in k8s
func InitHealthcheck(serverPort int) {
	r := mux.NewRouter()
	r.HandleFunc("/ready", ready).Methods("GET")
	r.HandleFunc("/live", live).Methods("GET")

	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), r)
		if err != nil {
			log.Fatalf("Error while exposing health port: %v", err)
			panic(err)
		}
	}()
}

// SetReady returns the actual internal state to precise if the given microservice is ready
func SetReady(status bool) {
	//TODO: there might be some concurrency issues here, but I'm not sure how bad it can behave
	isReady = status
}

// SetLive returns the actual internal state to precise if the given microservice is live
func SetLive(status bool) {
	//TODO: there might be some concurrency issues here, but I'm not sure how bad it can behave
	isLive = status
}

func ready(w http.ResponseWriter, _ *http.Request) {
	if isReady {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func live(w http.ResponseWriter, _ *http.Request) {
	if isLive {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}
