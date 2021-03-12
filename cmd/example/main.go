package main

import (
	"github.com/skysoft-atm/gorillaz"
)

func main() {
	g := gorillaz.New()
	<-g.Run()

	g.SetReady(true)
	// write your application code here

	select {}
}
