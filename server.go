package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/katasec/dstream/config"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// Server struct encapsulates the messaging server and its resources
type Server struct {
	natsServer *server.Server
	natsConn   *nats.Conn
	config     *config.Config
	dbConn     *sql.DB

	sqlMonitor       SQLServerTableMonitor
	checkpointWorker *CheckpointWorker
	cdcFetcher       *ChangeDataFetcher
	publisher        *PublisherWorker
}

// NewServer creates and initializes a new messaging server
func NewServer() *Server {
	// Start an embedded NATS server
	natsServer := test.RunDefaultServer()

	// Connect to the NATS server
	natsConn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("Error connecting to NATS server: %v", err)
	}

	// Initialize database connection
	dbConn, err := sql.Open("sqlserver", os.Getenv("DSTREAM_DB_CONNECTION_STRING"))
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}

	server := &Server{
		natsServer: natsServer,
		natsConn:   natsConn,
		config:     config.NewConfig(),
		dbConn:     dbConn,

		checkpointWorker: NewCheckpointWorker(dbConn, natsConn),
		cdcFetcher:       NewCDCFetcher("CDCFetcher", natsConn),
		publisher:        NewPublisherWorker("Publisher", natsConn),
	}

	return server
}

func (s *Server) Start() {
	log.Println("Starting server...")

	// Start Checkpoint Worker
	log.Println("Starting Checkpoint Worker...")
	s.checkpointWorker.Start()

	// Subscribe Publisher to CDC Events
	log.Println("Starting Publisher Worker...")
	s.publisher.Subscribe("cdc.events")

	// Start CDC Fetcher to process CDC changes
	log.Println("Starting CDC Fetcher...")
	//s.cdcFetcher.ProcessCDCChanges("users")

	log.Println("Server started successfully.")
}

// Shutdown gracefully shuts down all server components
func (s *Server) Shutdown() {
	log.Println("Shutting down server...")

	s.natsServer.Shutdown()

	if err := s.dbConn.Close(); err != nil {
		log.Printf("Error closing database connection: %v", err)
	}

	log.Println("Server shutdown complete.")
}
