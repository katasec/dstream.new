package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type CheckpointWorker struct {
	dbConn *sql.DB
	nc     *nats.Conn
}

// NewCheckpointWorker initializes a new CheckpointWorker with a database and NATS connection.
func NewCheckpointWorker(dbConn *sql.DB, nc *nats.Conn) *CheckpointWorker {
	return &CheckpointWorker{
		dbConn: dbConn,
		nc:     nc,
	}
}

func (cw *CheckpointWorker) Start() {
	go func() {
		// Listener for LoadLastLSN requests
		subject := "checkpoint.load"
		_, err := cw.nc.Subscribe(subject, cw.handleLoadLastLSN)
		if err != nil {
			log.Fatalf("Failed to subscribe to checkpoint.load: %v", err)
		} else {
			log.Printf("[CheckpointWorker] subscribed to %s\n", subject)
		}

		// Listener for SaveLastLSN requests
		_, err = cw.nc.Subscribe("checkpoint.save", cw.handleSaveLastLSN)
		if err != nil {
			log.Fatalf("Failed to subscribe to checkpoint.save: %v", err)
		}

		log.Println("CheckpointWorker is now listening for requests...")
		select {} // Keep the worker running
	}()

	time.Sleep(time.Second)
}

// handleLoadLastLSN processes a NATS message for loading the last LSN for a table.
func (cw *CheckpointWorker) handleLoadLastLSN(msg *nats.Msg) {
	log.Printf("[CheckpointWorker] Received LoadLastLSN request: %s", string(msg.Data))

	var req LoadLastLSNRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		log.Printf("[CheckpointWorker] Failed to parse LoadLastLSN request: %v", err)
		return
	}

	// Process the load request
	resp := cw.getLastLSNFromDb(req)
	respData, _ := json.Marshal(resp)

	// Respond back to the requester
	if err := msg.Respond(respData); err != nil {
		log.Printf("[CheckpointWorker] Failed to send LoadLastLSN response: %v", err)
		return
	}

	// Log the request and response
	log.Printf("[CheckpointWorker] Processed LoadLastLSN request for table '%s'. Response: %s", req.TableName, string(respData))
}

// getLastLSNFromDb retrieves the last LSN for a given table from the cdc_offsets table.
func (cw *CheckpointWorker) getLastLSNFromDb(req LoadLastLSNRequest) LoadLastLSNResponse {
	query := `SELECT last_lsn FROM cdc_offsets WHERE table_name = @tableName`
	var lastLSN []byte
	err := cw.dbConn.QueryRow(query, sql.Named("tableName", req.TableName)).Scan(&lastLSN)
	if err == sql.ErrNoRows {
		// Return default LSN if no entry exists
		return LoadLastLSNResponse{
			LastLSN: []byte("0x00000000000000000000"),
		}
	}
	if err != nil {
		return LoadLastLSNResponse{
			Error: fmt.Sprintf("failed to load last LSN for table %s: %v", req.TableName, err),
		}
	}
	return LoadLastLSNResponse{
		LastLSN: lastLSN,
	}
}

// handleSaveLastLSN processes a NATS message for saving the last LSN for a table.
func (cw *CheckpointWorker) handleSaveLastLSN(msg *nats.Msg) {
	log.Printf("[CheckpointWorker] Received SaveLastLSN request: %s", string(msg.Data))

	var req SaveLastLSNRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		log.Printf("[CheckpointWorker] Failed to parse SaveLastLSN request: %v", err)
		return
	}

	// Process the save request
	resp := cw.saveLastLSNToDb(req)
	respData, _ := json.Marshal(resp)

	// Respond back to the requester
	if err := msg.Respond(respData); err != nil {
		log.Printf("[CheckpointWorker] Failed to send SaveLastLSN response: %v", err)
		return
	}

	// Log the request and response
	log.Printf("[CheckpointWorker] Processed SaveLastLSN request for table '%s'. Response: %s", req.TableName, string(respData))
}

// saveLastLSNToDb updates the last LSN and timestamp for a given table in the cdc_offsets table.
func (cw *CheckpointWorker) saveLastLSNToDb(req SaveLastLSNRequest) SaveLastLSNResponse {
	query := `
        MERGE cdc_offsets AS target
        USING (SELECT @tableName AS table_name, @lastLSN AS last_lsn) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN
            UPDATE SET target.last_lsn = source.last_lsn, target.updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (table_name, last_lsn, updated_at)
            VALUES (source.table_name, source.last_lsn, CURRENT_TIMESTAMP);
    `
	_, err := cw.dbConn.Exec(query, sql.Named("tableName", req.TableName), sql.Named("lastLSN", req.LastLSN))
	if err != nil {
		return SaveLastLSNResponse{
			Error: fmt.Sprintf("failed to save last LSN for table %s: %v", req.TableName, err),
		}
	}
	return SaveLastLSNResponse{}
}