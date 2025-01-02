package main

import (
	"database/sql"
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
	db   *sql.DB
}

// NewChangeDataFetcher creates a new worker that fetches CDC data
func NewChangeDataFetcher(name string, conn *nats.Conn, db *sql.DB) *ChangeDataFetcher {
	cdcFetcher := &ChangeDataFetcher{
		name: name,
		conn: conn,
		db:   db,
	}

	return cdcFetcher
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
func (w *ChangeDataFetcher) ProcessCDCChanges(tableName string, lastLSN []byte) {

	monitor := NewSQLServerTableMonitor2(w.db, tableName, w.conn, 5*time.Second, 30*time.Second)
	err := monitor.StartMonitor(lastLSN)
	if err != nil {
		log.Fatalf("[%s] Error monitoring table '%s': %v", w.name, tableName, err)
	}
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
