package main

import (
	gaz "github.com/skysoft-atm/gorillaz"
)

func main() {
	gaz.New(nil)

	// write your application code here

	gaz.SetReady(true)
	gaz.SetLive(true)
	select {}
}
