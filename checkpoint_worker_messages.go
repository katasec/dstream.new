package main

// LoadLastLSNRequest defines the request payload for loading the last LSN.
type LoadLastLSNRequest struct {
	TableName string `json:"table_name"`
}

// LoadLastLSNResponse defines the response payload for loading the last LSN.
type LoadLastLSNResponse struct {
	LastLSN []byte `json:"last_lsn"`
	Error   string `json:"error,omitempty"`
}

// SaveLastLSNRequest defines the request payload for saving the last LSN.
type SaveLastLSNRequest struct {
	TableName string `json:"table_name"`
	LastLSN   []byte `json:"last_lsn"`
}

// SaveLastLSNResponse defines the response payload for saving the last LSN.
type SaveLastLSNResponse struct {
	Error string `json:"error,omitempty"`
}
