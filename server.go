package main

import (
	"database/sql"
	"log"
	"os"
	"sync"

	"github.com/katasec/dstream/config"
	"github.com/katasec/dstream/topics"
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
		cdcFetcher:       NewChangeDataFetcher("CDCFetcher", natsConn, dbConn),
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
	s.publisher.Subscribe(topics.CDC.Event)

	// WaitGroup to manage goroutines
	var wg sync.WaitGroup

	// Loop through tables in the config
	for _, table := range s.config.Tables {
		tableName := table.Name
		log.Printf("[Server] Preparing to process CDC changes for table '%s'...", tableName)

		// Fetch the last LSN for the table
		lastLSN := s.cdcFetcher.FetchLastLSN(tableName)

		// Increment the WaitGroup counter
		wg.Add(1)

		// Launch a goroutine per table to process CDC changes
		go s.launchProcessCDCChange(tableName, lastLSN, &wg)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	log.Println("All CDC changes processed. Server started successfully.")
}

func (s *Server) launchProcessCDCChange(tableName string, lastLSN []byte, wg *sync.WaitGroup) {
	defer wg.Done() // Decrement the counter when the goroutine completes
	log.Printf("[Server] Processing CDC changes for table '%s'...", tableName)
	s.cdcFetcher.ProcessCDCChanges(tableName, lastLSN)
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
