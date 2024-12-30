package main

import (
	"log"

	"github.com/nats-io/nats.go"
)

// FetchCDCWorker struct represents a worker that fetches CDC data and publishes it
type FetchCDCWorker struct {
	Name     string
	NATSConn *nats.Conn
}

// NewFetchCDCWorker creates a new worker that fetches CDC data
func NewFetchCDCWorker(name string, conn *nats.Conn) *FetchCDCWorker {
	return &FetchCDCWorker{
		Name:     name,
		NATSConn: conn,
	}
}

// PublishCDCData publishes hardcoded CDC data to a topic
func (w *FetchCDCWorker) PublishCDCData(topic string) {
	data := `{
		"operation": "INSERT",
		"table": "users",
		"data": { "id": 1, "name": "Alice" }
	}`
	if err := w.NATSConn.Publish(topic, []byte(data)); err != nil {
		log.Fatalf("[%s] Error publishing CDC data to topic '%s': %v", w.Name, topic, err)
	}
	log.Printf("[%s] Published CDC data to topic '%s': %s", w.Name, topic, data)
}
