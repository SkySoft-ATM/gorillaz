package main

import (
	gaz "github.com/skysoft-atm/gorillaz"
)

func main() {
	gaz.Init(nil)

	// write your application code here

	gaz.SetReady(true)
	gaz.SetLive(true)
	select {}
}
