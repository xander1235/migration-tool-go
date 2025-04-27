package services

import (
	"context"
	"encoding/json"
	"fmt"
	"migration-tool-go/config"
	"migration-tool-go/dtos"
	"migration-tool-go/dtos/common"
	"migration-tool-go/logger"
	"os"
	"sync"
	"time"
)

var MigrationRunner = &migrationRunner{}

type migrationRunner struct {
	startTime     time.Time
	failedRecords sync.Map // Use sync.Map for thread-safe concurrent access
	workerConfig  *common.WorkerConfiguration
	mapMutex      sync.Mutex
}

// Initialize sets up the migration runner
func NewMigrationRunner(workerConfig common.WorkerConfiguration) {
	// Set default values for worker configuration if not provided
	// WorkerBatchSize: Number of workers to use for processing
	if workerConfig.WorkerBatchSize <= 0 {
		workerConfig.WorkerBatchSize = 10000 // Default worker pool size
	}
	// IdBatchSize: Batch size for fetching primary key IDs from the source
	if workerConfig.IdBatchSize <= 0 {
		workerConfig.IdBatchSize = 10000 // Default ID batch size
	}
	// ConcurrentTables: Number of tables to process concurrently
	if workerConfig.ConcurrentTables <= 0 {
		workerConfig.ConcurrentTables = 10 // Default concurrent tables
	}
	// BatchProcessingTimeoutMs: Timeout in milliseconds for batch processing
	if workerConfig.BatchProcessingTimeoutMs <= 0 {
		workerConfig.BatchProcessingTimeoutMs = 500 // Default 500ms timeout
	}
	// RecordBatchSize: Number of records to process in a single batch
	if workerConfig.RecordBatchSize <= 0 {
		workerConfig.RecordBatchSize = 5000 // Default record batch size
	}

	MigrationRunner = &migrationRunner{
		startTime:    time.Now(),
		workerConfig: &workerConfig,
	}

	logger.Sugar.Infof("Migration runner initialized with workers: %d, worker batch size: %d, id batch size: %d, record batch size: %d, concurrent tables: %d, batch processing timeout: %dms",
		workerConfig.NoOfWorkers,
		workerConfig.WorkerBatchSize,
		workerConfig.IdBatchSize,
		workerConfig.RecordBatchSize,
		workerConfig.ConcurrentTables,
		workerConfig.BatchProcessingTimeoutMs)
}

// Run executes the complete migration process
func (m *migrationRunner) Run(ctx context.Context) error {
	// Use concurrent tables from config to determine buffer size
	tableInfoChan := make(chan *dtos.TableInfoChan, m.workerConfig.ConcurrentTables)
	processedAllTables := false
	// Map to track failed records by table name
	m.failedRecords = sync.Map{}

	// Start the source data extraction in a goroutine
	go func() {
		if err := PostgresMigration.GetRecordsFromSource(ctx, tableInfoChan, &processedAllTables); err != nil {
			logger.Sugar.Errorf("Error getting records from source: %v", err)
		}
	}()

	// Process the data
	exitTableProcessing := false
	for !exitTableProcessing {
		select {
		case infoChan, ok := <-tableInfoChan:
			if !ok {
				exitTableProcessing = true
				break
			}

			// Process the received table information
			records := m.processTableInfo(ctx, infoChan)

			// Check if we should exit the loop
			if len(records) == 0 && processedAllTables {
				exitTableProcessing = true
				break
			}
		case <-ctx.Done():
			// Context cancelled or timed out
			logger.Sugar.Info("Migration stopped due to context cancellation")
			exitTableProcessing = true
			break
		}
	}

	logger.Sugar.Infof("Migration completed. Total time taken: %s", time.Since(m.startTime))

	// Handle any failed records if needed
	failedTablesCount := 0
	m.failedRecords.Range(func(key, value interface{}) bool {
		records := value.([]map[string]any)
		if len(records) > 0 {
			failedTablesCount++
		}
		return true
	})

	if failedTablesCount > 0 {
		logger.Sugar.Infof("There were failed records: %d tables had failures", failedTablesCount)

		// Save failed records to files for later analysis or retry
		m.handleFailedRecords()
	}

	return nil
}

// processTableInfo handles the processing of a single table's data
func (m *migrationRunner) processTableInfo(ctx context.Context, infoChan *dtos.TableInfoChan) []map[string]any {
	checkAllRecordsProcessed := &sync.Map{}

	// Initialize failed records tracking for this table if needed
	_, loaded := m.failedRecords.LoadOrStore(infoChan.TableInfo.TableName, []map[string]any{})
	if !loaded {
		logger.Sugar.Infof("Initialized failed records tracking for table %s", infoChan.TableInfo.TableName)
	}

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Initialize connectors based on destination type
	var activeConnectors []Connector
	
	switch config.DestinationConfig.Type {
	case "doris":
		// Initialize Doris connector
		activeConnectors = append(activeConnectors, DorisConnector)
		DorisConnector.Initialize(ctx, infoChan, checkAllRecordsProcessed)
		
	case "kafka":
		// Initialize Kafka connector
		activeConnectors = append(activeConnectors, KafkaConnector)
		KafkaConnector.Initialize(ctx, infoChan, checkAllRecordsProcessed)
		
	default:
		logger.Sugar.Warnf("Unsupported destination type: %s", config.DestinationConfig.Type)
		return nil
	}
	
	if len(activeConnectors) == 0 {
		logger.Sugar.Warn("No active connectors initialized")
		return nil
	}

	// Start a goroutine to log progress
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				logger.Sugar.Infof("Progress for table %s: %d UUIDs read, %d records read, %d records processed, elapsed time: %s",
					infoChan.TableInfo.TableName,
					infoChan.GetTotalUuidsRead(),
					infoChan.GetTotalRecordsRead(),
					infoChan.GetTotalRecordsProcessed(),
					time.Since(m.startTime).String(),
				)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start processing records for each connector
	for _, connector := range activeConnectors {
		go connector.ProcessRecords(ctx, infoChan.RecordsChan)
	}

	// Wait for all records to be processed or context cancellation
	done := make(chan struct{})
	go func() {
		for {
			// Check if we're done processing all records for this table
			allDone := true
			for _, connector := range activeConnectors {
				if !connector.IsProcessingDone() {
					allDone = false
					break
				}
			}
			
			if allDone {
				close(done)
				return
			}
			
			// Sleep a bit to avoid busy waiting
			time.Sleep(100 * time.Millisecond)
		}
	}()
	
	// Wait for either completion or context cancellation
	select {
	case <-done:
		logger.Sugar.Infof("Finished processing all records for table %s", infoChan.TableInfo.TableName)
	case <-ctx.Done():
		logger.Sugar.Warnf("Processing of table %s was cancelled", infoChan.TableInfo.TableName)
	}

	// Close all connectors
	for _, connector := range activeConnectors {
		if err := connector.Close(); err != nil {
			logger.Sugar.Errorf("Error closing connector: %v", err)
		}
	}

	// Retrieve failed records
	var failedRecords []map[string]any
	// In a real implementation, we would get failed records from each connector
	// For now, we'll just use the existing failed records mechanism
	for _, connector := range activeConnectors {
		failedRecords = append(failedRecords, connector.GetFailedRecords()...)
	}

	return failedRecords
}

// checkTableProcessed checks if all records for a table have been processed
func (m *migrationRunner) checkTableProcessed(infoChan *dtos.TableInfoChan, checkAllRecordsProcessed *sync.Map) bool {
	// Get total failed records
	totalFailedRecords := 0
	failedRecordsInterface, ok := m.failedRecords.Load(infoChan.TableInfo.TableName)
	if ok {
		failedRecords, ok := failedRecordsInterface.([]map[string]any)
		if ok {
			totalFailedRecords = len(failedRecords)
		}
	}

	// If we've read all records and processed all of them (including failures), we're done with this table
	if infoChan.ReadingRecordsDone.Load().(bool) && (infoChan.GetTotalRecordsProcessed()+uint64(totalFailedRecords)) >= infoChan.GetTotalRecordsRead() {
		logger.Sugar.Infof("Migration for table %s completed, workers: %d, batch size: %d, batch timeout: %dms, total uuids read: %d, total records read: %d, total records processed: %d, failed records: %d, time taken: %s",
			infoChan.TableInfo.TableName,
			m.workerConfig.NoOfWorkers,
			m.workerConfig.RecordBatchSize,
			m.workerConfig.BatchProcessingTimeoutMs,
			infoChan.GetTotalUuidsRead(),
			infoChan.GetTotalRecordsRead(),
			infoChan.GetTotalRecordsProcessed(),
			totalFailedRecords,
			time.Since(m.startTime).String(),
		)
		return true
	}
	return false
}

// handleFailedRecords processes any failed records for a table
func (m *migrationRunner) handleFailedRecords() error {
	m.failedRecords.Range(func(key, value interface{}) bool {
		tableName := key.(string)
		records := value.([]map[string]any)
		if len(records) > 0 {
			logger.Sugar.Infof("Table %s had %d failed records", tableName, len(records))

			// Write to file
			failedRecordsFile := fmt.Sprintf("%s_failed_records.json", tableName)
			data, err := json.MarshalIndent(records, "", "  ")
			if err != nil {
				logger.Sugar.Errorf("Failed to marshal failed records for table %s: %v", tableName, err)
				return true
			}
			err = os.WriteFile(failedRecordsFile, data, 0644)
			if err != nil {
				logger.Sugar.Errorf("Failed to save failed records for table %s: %v", tableName, err)
			}
		}
		return true
	})

	return nil
}
