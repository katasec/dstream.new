package main

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
