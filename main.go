package main

import (
	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	//fetchLastLSNs()
	//select {}
	doServerStuff()
}

func doServerStuff() {
	server := NewServer()
	server.Start()
}
