package gorillaz

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func GracefulStop() {
	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	signal.Notify(gracefulStop, syscall.SIGINT)
	go func() {
		sig := <-gracefulStop
		Sugar.Info("Caught OS signal: %v", sig)
		buf := make([]byte, 1<<20)
		stacklen := runtime.Stack(buf, true)
		log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
		os.Exit(0)
	}()
}
