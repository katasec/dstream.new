package main

import (
	"log"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// Server struct encapsulates the messaging server and its resources
type Server struct {
	NATSServer *server.Server
	NATSConn   *nats.Conn
}

// NewServer creates and initializes a new messaging server
func NewServer() *Server {
	// Start an embedded NATS server
	natsServer := test.RunDefaultServer()

	// Connect to the NATS server
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("Error connecting to NATS server: %v", err)
	}

	return &Server{
		NATSServer: natsServer,
		NATSConn:   nc,
	}
}

func (s *Server) Shutdown() {
	s.NATSConn.Close()
	s.NATSServer.Shutdown()
}
