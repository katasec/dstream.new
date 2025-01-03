package natshelper

import (
	"log"

	"github.com/nats-io/nats.go"
)

// Subscribe subscribes to a topic and prints received messages
func Subscribe(module string, conn *nats.Conn, topic string, handler nats.MsgHandler) {
	_, err := conn.Subscribe(topic, handler)
	if err != nil {
		log.Fatalf("[%s] Error subscribing to topic '%s': %v", module, topic, err)
	}
	log.Printf("[%s] Subscribed to topic '%s'", module, topic)
}
