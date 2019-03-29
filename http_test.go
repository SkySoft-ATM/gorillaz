package gorillaz

import (
	"net"
	"net/http/httptest"

	"github.com/gorilla/mux"
)

// mandatory
func SetupLogger() {
	gaz := &Gaz{Router: mux.NewRouter()}
	gaz.InitLogs("info")
}

// setupServerHttp creates a HTTP server on a random free TCP port
// it returns the TCP port and a function that shutdowns the server
func setupServerHTTP(r *mux.Router) (port int, shutdown func()) {
	srv := httptest.NewServer(r)
	port = srv.Listener.Addr().(*net.TCPAddr).Port
	shutdown = func() {
		srv.Close()
	}
	return
}

//func checkResponse(t *testing.T, resp *http.Response, checks func(test))
