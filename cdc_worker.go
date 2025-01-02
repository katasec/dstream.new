package main

import (
	"encoding/hex"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// ChangeDataFetcher struct represents a worker that fetches CDC data and publishes it
type ChangeDataFetcher struct {
	name string
	conn *nats.Conn
}

// NewCDCFetcher creates a new worker that fetches CDC data
func NewCDCFetcher(name string, conn *nats.Conn) *ChangeDataFetcher {
	return &ChangeDataFetcher{
		name: name,
		conn: conn,
	}
}

// FetchLastLSN fetches the last LSN for a given table from the checkpoint worker
func (w *ChangeDataFetcher) FetchLastLSN(tableName string) []byte {
	topic := "checkpoint.load"
	req := LoadLastLSNRequest{TableName: tableName}
	reqData, _ := json.Marshal(req)

	// Use NATS Request-Reply to get the last LSN
	msg, err := w.conn.Request(topic, reqData, 2*time.Second)
	if err != nil {
		log.Fatalf("[%s] Failed to fetch last LSN for table '%s': %v", w.name, tableName, err)
	}

	var resp LoadLastLSNResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		log.Fatalf("[%s] Failed to parse last LSN response: %v", w.name, err)
	}

	if resp.Error != "" {
		log.Fatalf("[%s] Error in last LSN response: %s", w.name, resp.Error)
	}

	log.Printf("[%s] Fetched last LSN for table '%s': %s", w.name, tableName, hex.EncodeToString(resp.LastLSN))
	return resp.LastLSN
}

// ProcessCDCChanges processes CDC changes for a given table and publishes them
func (w *ChangeDataFetcher) ProcessCDCChanges(tableName string) {
	// Fetch the last LSN
	lastLSN := w.FetchLastLSN(tableName)

	// Simulate fetching CDC changes using the last LSN
	log.Printf("[%s] Processing CDC changes for table '%s' starting from LSN: %s", w.name, tableName, lastLSN)

	// Hardcoded new LSN and data (simulate processing changes)
	newLSN := []byte("0x00000000000000000001")
	cdcData := `{
		"operation": "INSERT",
		"table": "users",
		"data": { "id": 1, "name": "Alice" }
	}`

	// Publish the CDC data to a topic
	topic := "cdc.events"
	if err := w.conn.Publish(topic, []byte(cdcData)); err != nil {
		log.Fatalf("[%s] Failed to publish CDC data: %v", w.name, err)
	}
	log.Printf("[%s] Published CDC data to topic '%s': %s", w.name, topic, cdcData)

	// Save the new LSN
	w.SaveLastLSN(tableName, newLSN)
}

// SaveLastLSN saves the last LSN for a given table via the checkpoint worker
func (w *ChangeDataFetcher) SaveLastLSN(tableName string, lastLSN []byte) {
	topic := "checkpoint.save"
	req := SaveLastLSNRequest{
		TableName: tableName,
		LastLSN:   lastLSN,
	}
	reqData, _ := json.Marshal(req)

	if err := w.conn.Publish(topic, reqData); err != nil {
		log.Fatalf("[%s] Failed to save last LSN for table '%s': %v", w.name, tableName, err)
	}
	log.Printf("[%s] Saved last LSN for table '%s': %s", w.name, tableName, lastLSN)
}

// Publish publishes hardcoded CDC data to a topic
func (w *ChangeDataFetcher) Publish(topic string) {
	data := `{
		"operation": "INSERT",
		"table": "users",
		"data": { "id": 1, "name": "Alice" }
	}`
	if err := w.conn.Publish(topic, []byte(data)); err != nil {
		log.Fatalf("[%s] Error publishing CDC data to topic '%s': %v", w.name, topic, err)
	}
	log.Printf("[%s] Published CDC data to topic '%s': %s", w.name, topic, data)
}
