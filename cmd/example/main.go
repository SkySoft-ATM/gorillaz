package main

import (
	"github.com/skysoft-atm/gorillaz"
)

func main() {
	gaz := gorillaz.New(nil)

	// write your application code here

	gaz.SetReady(true)
	gaz.SetLive(true)
	select {}
}
