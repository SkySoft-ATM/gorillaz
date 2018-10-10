package gorillaz

import (
	"fmt"
	"net/http"
	_ "net/http"
	_ "net/http/pprof"
)

func InitPprof(serverPort int) {
	go func() {
		http.ListenAndServe(fmt.Sprintf(":%d", serverPort), nil)
	}()
}
