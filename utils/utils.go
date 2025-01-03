package utils

import (
	"encoding/json"
	"fmt"
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

// UnmarshalJSON unmarshals JSON data into a generic type T.
func UnmarshalJSON[T any](data []byte) (T, error) {
	var result T
	err := json.Unmarshal(data, &result)
	if err != nil {
		return result, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
	return result, nil
}

// MarshalJSON marshals a generic type T into JSON data.
func MarshalJSON[T any](value T) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return data, nil
}
