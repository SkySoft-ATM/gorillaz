package gorillaz

import "runtime"

var cores = runtime.NumCPU()

// Cores returns the num of CPU
func Cores() int {
	return cores
}
