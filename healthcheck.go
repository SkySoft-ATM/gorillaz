package gorillaz

import (
	"fmt"
	"github.com/apex/log"
	"github.com/gorilla/mux"
	"net/http"
)

func InitHealthcheck(serverPort int) {
	r := mux.NewRouter()
	r.HandleFunc("/ready", ready).Methods("GET")
	r.HandleFunc("/live", live).Methods("GET")

	err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), r)
	if err != nil {
		log.Fatalf("Error while exposing health port: %v", err)
		panic(err)
	}
}

func ready(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	//w.WriteHeader(http.StatusNotAcceptable)
}

func live(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
