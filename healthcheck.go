package gorillaz

import (
	"fmt"
	"github.com/apex/log"
	"github.com/gorilla/mux"
	"net/http"
)

var isReady = false
var isLive = false

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

func SetReady(status bool) {
	isReady = status
}

func SetLive(status bool) {
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
