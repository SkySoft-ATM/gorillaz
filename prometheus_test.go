package gorillaz

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/gorilla/mux"
)

//TestPrometheusInit tests the creation of a prometheus endpoint and the given path
func TestPrometheusInit(t *testing.T) {
	SetupLogger()
	path := "/somemetric"
	gaz := &Gaz{Router: mux.NewRouter()}
	gaz.InitPrometheus(path)

	port, shutdown := setupServerHTTP(gaz.Router)
	defer shutdown()

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d%s", port, path))
	if err != nil {
		t.Errorf("cannot query metrics endpoint, %+v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected http status %d but got %d", http.StatusOK, resp.StatusCode)
	}
}
