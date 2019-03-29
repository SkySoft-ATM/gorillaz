package gorillaz

import (
	"fmt"
	"net/http"
)

// InitPprof starts an HTTP server on serverPort
// the application must import the package http/pprof to enable pprof.
// it's required to start a new HTTP server because http/pprof init() calls http.HandleFunc, so we cannot attach it to an existing router
// see https://golang.org/src/net/http/pprof/pprof.go?s=6833:6871#L211
func (g *Gaz) InitPprof(serverPort int) {
	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), nil)
		if err != nil {
			Sugar.Errorf("error trying to setup HTTP endpoint on port %d: %v", serverPort, err)
		}
	}()
}
