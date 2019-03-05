package gorillaz

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/gorilla/mux"
)

//TestHealth tests the creation of a prometheus endpoint and the given path
func TestHealth(t *testing.T) {
	SetupLogger()
	router := mux.NewRouter()
	InitHealthcheck(router)

	port, shutdown := setupServerHTTP(router)
	defer shutdown()

	baseURL := fmt.Sprintf("http://localhost:%d", port)

	okStatus := http.StatusOK
	koStatus := http.StatusServiceUnavailable

	check(t, "ready and live are unset", baseURL+"/ready", koStatus)
	check(t, "ready and live are unset", baseURL+"/live", koStatus)

	SetReady(true)
	check(t, "ready is set", baseURL+"/ready", okStatus)
	check(t, "ready is set", baseURL+"/live", koStatus)

	SetLive(true)
	check(t, "ready and live are set", baseURL+"/ready", okStatus)
	check(t, "ready and live are set", baseURL+"/live", okStatus)

	SetReady(false)
	check(t, "ready is set to false", baseURL+"/ready", koStatus)
	check(t, "ready is set to false", baseURL+"/live", okStatus)

	SetLive(false)
	check(t, "ready and live are set to false", baseURL+"/ready", koStatus)
	check(t, "ready and live are set to false", baseURL+"/live", koStatus)
}

func check(t *testing.T, scenario, url string, expectedStatus int) {
	resp, err := http.Get(url)
	if err != nil {
		t.Errorf("%s: cannot query %s, %+v", scenario, url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != expectedStatus {
		t.Errorf("%s: expected status %d but got %d", scenario, expectedStatus, resp.StatusCode)
	}
}
