package main

import (
	"log"

	"github.com/nats-io/nats.go"
)

// CDCFetcher struct represents a worker that fetches CDC data and publishes it
type CDCFetcher struct {
	name string
	conn *nats.Conn
}

// NewCDCFetcher creates a new worker that fetches CDC data
func NewCDCFetcher(name string, conn *nats.Conn) *CDCFetcher {
	return &CDCFetcher{
		name: name,
		conn: conn,
	}
}

// Publish publishes hardcoded CDC data to a topic
func (w *CDCFetcher) Publish(topic string) {
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
