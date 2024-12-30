package main

// // Server struct encapsulates the messaging server and its resources
// type Server struct {
// 	NATSServer *server.Server
// 	NATSConn   *nats.Conn
// }

// // NewServer creates and initializes a new messaging server
// func NewServer() *Server {
// 	// Start an embedded NATS server
// 	natsServer := test.RunDefaultServer()

// 	// Connect to the NATS server
// 	nc, err := nats.Connect(nats.DefaultURL)
// 	if err != nil {
// 		log.Fatalf("Error connecting to NATS server: %v", err)
// 	}

// 	return &Server{
// 		NATSServer: natsServer,
// 		NATSConn:   nc,
// 	}
// }

func main() {
	// Initialize the server
	server := NewServer()
	defer server.NATSConn.Close()
	defer server.NATSServer.Shutdown()

	// Create FetchCDCWorker
	fetchCDCWorker := NewFetchCDCWorker("FetchCDCWorker", server.NATSConn)

	// Create PublisherWorker
	publisherWorker := NewPublisherWorker("PublisherWorker", server.NATSConn)

	// PublisherWorker subscribes to the topic
	topic := "cdc.events"
	publisherWorker.SubscribeAndPrint(topic)

	// FetchCDCWorker publishes data to the topic
	fetchCDCWorker.PublishCDCData(topic)
}
