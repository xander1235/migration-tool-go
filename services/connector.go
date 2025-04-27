package services

import (
	"context"
	"migration-tool-go/dtos"
	"sync"
)

// Connector defines the interface for destination connectors
type Connector interface {
	// Initialize sets up the connector with necessary configuration
	Initialize(ctx context.Context, tableInfo *dtos.TableInfoChan, recordsProcessedTracker *sync.Map)
	
	// ProcessRecords starts processing records from the channel
	ProcessRecords(ctx context.Context, recordsChan <-chan map[string]any)
	
	// Close performs cleanup and waits for all pending operations to complete
	Close() error
	
	// GetProcessedCount returns the total number of records processed by this connector
	GetProcessedCount() uint64
	
	// GetFailedRecords returns the records that failed to be processed
	GetFailedRecords() []map[string]any
	
	// IsProcessingDone checks if the connector has finished processing all records
	IsProcessingDone() bool
}
