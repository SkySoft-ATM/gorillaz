package main

import (
	"github.com/skysoft-atm/gorillaz"
)

func main() {
	gaz := gorillaz.New()
	gaz.Run()

	// write your application code here

	gaz.SetReady(true)
	select {}
}
