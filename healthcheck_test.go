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
	gaz := &Gaz{Router: mux.NewRouter(), isReady: new(int32)}
	gaz.InitHealthcheck()

	port, shutdown := setupServerHTTP(gaz.Router)
	defer shutdown()

	baseURL := fmt.Sprintf("http://localhost:%d", port)

	okStatus := http.StatusOK
	koStatus := http.StatusServiceUnavailable

	check(t, "ready is unset", baseURL+"/ready", koStatus)
	check(t, "live is set", baseURL+"/live", okStatus)

	gaz.SetReady(true)
	check(t, "ready is set", baseURL+"/ready", okStatus)

	gaz.SetReady(false)
	check(t, "ready is set to false", baseURL+"/ready", koStatus)
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
