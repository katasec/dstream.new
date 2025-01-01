package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	fetchLastLSNs()
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
	cdcFetcher := NewCDCFetcher("CDCFetcher", server.NATSConn)
	publisher := NewPublisherWorker("Publisher", server.NATSConn)

	// PublisherWorker subscribes to the topic
	topic := "cdc.events"
	publisher.Subscribe(topic)

	// FetchCDCWorker publishes data to the topic
	cdcFetcher.Publish(topic)

	select {}
}

func fetchLastLSNs() {
	// Initialize the server
	server := NewServer()
	defer server.Shutdown()

	// Initialize database connection
	dbConn, err := sql.Open("sqlserver", os.Getenv("DSTREAM_DB_CONNECTION_STRING"))
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer dbConn.Close()

	// Create and start the CheckpointWorker
	cw := NewCheckpointWorker(dbConn, server.NATSConn)
	cw.Start()

	// Give the worker some time to start up (optional in case of race conditions)
	time.Sleep(1 * time.Second)

	// Define the tables we want to fetch the last LSNs for
	tables := []string{"Cars", "Persons"}

	for _, tableName := range tables {
		// Create a LoadLastLSNRequest
		req := LoadLastLSNRequest{TableName: tableName}
		reqData, _ := json.Marshal(req)

		// Publish the request and wait for a response
		msg, err := server.NATSConn.Request("checkpoint.load", reqData, 2*time.Second)
		if err != nil {
			log.Printf("Failed to fetch last LSN for table %s: %v", tableName, err)
			continue
		}

		// Parse the response
		var resp LoadLastLSNResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			log.Printf("Failed to parse response for table %s: %v", tableName, err)
			continue
		}

		// Handle the response
		if resp.Error != "" {
			log.Printf("Error fetching last LSN for table %s: %s", tableName, resp.Error)
		} else {
			// Convert LastLSN (byte slice) to a human-readable hexadecimal string
			hexLSN := fmt.Sprintf("0x%s", hex.EncodeToString(resp.LastLSN))
			log.Printf("Last LSN for table %s: %s\n", tableName, hexLSN)
		}
	}
}
