package gorillaz

import (
	"fmt"
	"github.com/apex/log"
	"github.com/gorilla/mux"
	"net/http"
)

var isReady = false
var isLive = false

func InitHealthcheck(serverPort int) (chan bool, chan bool) {
	r := mux.NewRouter()
	r.HandleFunc("/ready", ready).Methods("GET")
	r.HandleFunc("/live", live).Methods("GET")

	chReady := make(chan bool)
	chLive := make(chan bool)

	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), r)
		if err != nil {
			log.Fatalf("Error while exposing health port: %v", err)
			panic(err)
		}
	}()

	go func() {
		for b := range chReady {
			isReady = b
		}
	}()

	go func() {
		for b := range chLive {
			isLive = b
		}
	}()

	return chReady, chLive
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
