package main

import (
	gaz "github.com/skysoft-atm/gorillaz"
)

func main() {
	g := gaz.New(nil)
	g.Run()

	// write your application code here

	g.SetReady(true)
	g.SetLive(true)
	select {}
}
