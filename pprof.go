package gorillaz

import (
	"fmt"
	"net/http"
)

// InitPprof starts an HTTP endpoint on serverPort
func InitPprof(serverPort int) {
	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), nil)
		if err != nil {
			Sugar.Errorf("error trying to setup HTTP endpoint on port %d: %v", serverPort, err)
		}
	}()
}
