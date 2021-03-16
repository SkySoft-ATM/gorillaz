package gorillaz

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func GracefulStop(g *Gaz) {
	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	signal.Notify(gracefulStop, syscall.SIGINT)
	go func() {
		sig := <-gracefulStop
		Sugar.Infof("Caught OS signal: %v", sig)
		buf := make([]byte, 1<<20)
		log.Printf("=== received SIGQUIT ===\n")
		for _, callback := range g.cleanupCallbacks {
			callback()
		}
		stacklen := runtime.Stack(buf, true)
		log.Printf("*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
		os.Exit(1)
	}()
}
