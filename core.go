package gorillaz

import "runtime"

//TODO: do we need this? Can't we directly use  runtime.NumCPU()?

var cores = runtime.NumCPU()

// Cores returns the num of CPU
func Cores() int {
	return cores
}
