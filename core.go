package gorillaz

import "runtime"

var cores = runtime.NumCPU()

func init() {
	runtime.GOMAXPROCS(cores)
}

func Cores() int {
	return cores
}
