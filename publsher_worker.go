package main

import (
	"log"

	"github.com/nats-io/nats.go"
)

// PublisherWorker struct represents a worker that subscribes to a topic and prints data
type PublisherWorker struct {
	Name     string
	NATSConn *nats.Conn
}

// NewPublisherWorker creates a new worker that subscribes to a topic and prints data
func NewPublisherWorker(name string, conn *nats.Conn) *PublisherWorker {
	return &PublisherWorker{
		Name:     name,
		NATSConn: conn,
	}
}

// SubscribeAndPrint subscribes to a topic and prints received messages
func (w *PublisherWorker) SubscribeAndPrint(topic string) {
	_, err := w.NATSConn.Subscribe(topic, func(msg *nats.Msg) {
		log.Printf("[%s] Received message on topic '%s': %s", w.Name, topic, msg.Data)
	})
	if err != nil {
		log.Fatalf("[%s] Error subscribing to topic '%s': %v", w.Name, topic, err)
	}
	log.Printf("[%s] Subscribed to topic '%s", w.Name, topic)
}
