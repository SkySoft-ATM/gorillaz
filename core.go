package gorillaz

import "runtime"

var cores = runtime.NumCPU()

func init() {
	runtime.GOMAXPROCS(cores)
}

// Cores returns the num of CPU
func Cores() int {
	return cores
}
