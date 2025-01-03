package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/katasec/dstream/topics"
	"github.com/nats-io/nats.go"
)

type SQLServerTableMonitor struct {
	dbConn          *sql.DB
	tableName       string
	pollInterval    time.Duration
	maxPollInterval time.Duration
	natsConn        *nats.Conn
	lastLSNs        map[string][]byte
	lsnMutex        sync.Mutex
	columns         []string // Cached column names
}

// NewSQLServerTableMonitor creates a new SQLServerTableMonitor2
func NewSQLServerTableMonitor(dbConn *sql.DB, tableName string, natsConn *nats.Conn, pollInterval, maxPollInterval time.Duration) *SQLServerTableMonitor {
	// Fetch column names once and store them in the struct
	columns, err := fetchColumnNames(dbConn, tableName)
	if err != nil {
		log.Fatalf("Failed to fetch column names for table %s: %v", tableName, err)
	}

	return &SQLServerTableMonitor{
		dbConn:          dbConn,
		tableName:       tableName,
		natsConn:        natsConn,
		pollInterval:    pollInterval,
		maxPollInterval: maxPollInterval,
		lastLSNs:        make(map[string][]byte),
		columns:         columns,
	}
}

// StartMonitor begins monitoring changes for the table and publishes them to NATS
func (m *SQLServerTableMonitor) StartMonitor(lastLSN []byte) error {
	backoff := NewBackoffManager(m.pollInterval, m.maxPollInterval)
	m.lastLSNs[m.tableName] = lastLSN

	for {
		log.Printf("Polling changes for table %s, since LSN: %s", m.tableName, hex.EncodeToString(lastLSN))
		changes, newLSN, err := m.fetchCDCChanges(m.lastLSNs[m.tableName])
		if err != nil {
			log.Printf("Error fetching changes for %s: %v", m.tableName, err)
			time.Sleep(backoff.GetInterval()) // Wait on error
			continue
		}

		if len(changes) > 0 {
			log.Printf("Changes detected for table %s; publishing...", m.tableName)
			for _, change := range changes {
				if err := m.publishChangeToNATS(change); err != nil {
					log.Printf("Failed to publish change for table %s: %v", m.tableName, err)
				}
			}
			m.lsnMutex.Lock()
			m.lastLSNs[m.tableName] = newLSN
			m.lsnMutex.Unlock()
			backoff.ResetInterval()
		} else {
			backoff.IncreaseInterval()
			log.Printf("No changes found for table %s. Next poll in %s", m.tableName, backoff.GetInterval())
		}

		time.Sleep(backoff.GetInterval())
	}
}

// fetchCDCChanges queries CDC changes and returns relevant events
func (m *SQLServerTableMonitor) fetchCDCChanges(lastLSN []byte) ([]map[string]interface{}, []byte, error) {
	columnList := "ct.__$start_lsn, ct.__$operation, " + strings.Join(m.columns, ", ")
	query := fmt.Sprintf(`
        SELECT %s
        FROM cdc.dbo_%s_CT AS ct
        WHERE ct.__$start_lsn > @lastLSN
        ORDER BY ct.__$start_lsn
    `, columnList, m.tableName)

	rows, err := m.dbConn.Query(query, sql.Named("lastLSN", lastLSN))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query CDC table for %s: %w", m.tableName, err)
	}
	defer rows.Close()

	changes := []map[string]interface{}{}
	var latestLSN []byte

	for rows.Next() {
		var lsn []byte
		var operation int
		columnData := make([]interface{}, len(m.columns)+2)
		columnData[0] = &lsn
		columnData[1] = &operation
		for i := range m.columns {
			columnData[i+2] = new(sql.NullString)
		}

		if err := rows.Scan(columnData...); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}

		change := parseChange(lsn, operation, m.columns, columnData)
		changes = append(changes, change)
		latestLSN = lsn
	}

	return changes, latestLSN, nil
}

// publishChangeToNATS publishes a CDC change to a NATS topic
func (m *SQLServerTableMonitor) publishChangeToNATS(change map[string]interface{}) error {
	data, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("failed to marshal change: %w", err)
	}
	return m.natsConn.Publish(topics.CDC.Event, data)
}

// parseChange processes a row into a structured change
func parseChange(lsn []byte, operation int, columns []string, columnData []interface{}) map[string]interface{} {
	operationType := map[int]string{2: "Insert", 4: "Update", 1: "Delete"}[operation]
	data := map[string]interface{}{}
	for i, col := range columns {
		if val, ok := columnData[i+2].(*sql.NullString); ok && val.Valid {
			data[col] = val.String
		} else {
			data[col] = nil
		}
	}
	return map[string]interface{}{
		"metadata": map[string]interface{}{
			"LSN":           hex.EncodeToString(lsn),
			"OperationType": operationType,
		},
		"data": data,
	}
}

// fetchColumnNames fetches column names for a specified table
func fetchColumnNames(db *sql.DB, tableName string) ([]string, error) {
	query := `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tableName`
	rows, err := db.Query(query, sql.Named("tableName", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columns = append(columns, columnName)
	}
	return columns, rows.Err()
}
