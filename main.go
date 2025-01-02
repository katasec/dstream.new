package main

import (
	"database/sql"
	"encoding/hex"
	"log"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	//fetchLastLSNs()
	//select {}
	doServerStuff()

}

func doServerStuff() {
	// Initialize the server
	server := NewServer()
	server.Start()

	// Demonstrate CDC Fetcher querying LastLSN from CheckpointWorker
	tableName := "Cars"
	log.Printf("[Main] Demonstrating CDC Fetcher querying LastLSN for table '%s'...", tableName)
	lastLSN := server.cdcFetcher.FetchLastLSN(tableName)

	// Format the LSN for proper display
	formattedLSN := hex.EncodeToString(lastLSN)
	log.Printf("[Main] Fetched LastLSN for table '%s': %s", tableName, formattedLSN)

	select {}
}
func monitorTable() {
	dbConn, _ := sql.Open("sqlserver", os.Getenv("DSTREAM_DB_CONNECTION_STRING"))
	tableName := "Cars"
	poll_interval, _ := time.ParseDuration("5s")
	max_poll_interval, _ := time.ParseDuration("2m")

	monitor := NewSQLServerMonitor(dbConn, tableName, poll_interval, max_poll_interval)
	monitor.StartMonitor()

}
func doStuff() {
	// Initialize the server
	server := NewServer()
	defer server.Shutdown()

	// Create a Fetcher and a Publisher with the same nats connection
	cdcFetcher := NewChangeDataFetcher("CDCFetcher", server.natsConn, server.dbConn)
	publisher := NewPublisherWorker("Publisher", server.natsConn)

	// PublisherWorker subscribes to the topic
	topic := "cdc.events"
	publisher.Subscribe(topic)

	// FetchCDCWorker publishes data to the topic
	cdcFetcher.Publish(topic)
}
