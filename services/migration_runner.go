package services

import (
	"context"
	"fmt"
	"migration-tool-go/dtos"
	"migration-tool-go/dtos/common"
	"migration-tool-go/logger"
	"migration-tool-go/utils"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
)

var MigrationRunner = &migrationRunner{}

type migrationRunner struct {
	startTime     time.Time
	failedRecords map[string][]map[string]any
	workerConfig  *common.WorkerConfiguration
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
	m.failedRecords = make(map[string][]map[string]any)

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
			records := m.processTableInfo(infoChan)

			// Check if we should exit the loop
			if len(records) == 0 && processedAllTables {
				exitTableProcessing = true
			}
		case <-ctx.Done():
			// Context cancelled or timed out
			logger.Sugar.Info("Migration stopped due to context cancellation")
			exitTableProcessing = true
		}
	}

	logger.Sugar.Infof("Migration completed. Total time taken: %s", time.Since(m.startTime))

	// Handle any failed records if needed
	if len(m.failedRecords) > 0 {
		logger.Sugar.Infof("There were failed records: %d tables had failures", len(m.failedRecords))

		// Save failed records to files for later analysis or retry
		for tableName, records := range m.failedRecords {
			if len(records) == 0 {
				continue
			}
			filePath := fmt.Sprintf("%s_failed_records.json", tableName)
			logger.Sugar.Infof("Saving %d failed records for table %s to %s", len(records), tableName, filePath)

			_, err := utils.ConvertRecordsToJSON(records, filePath, true)
			if err != nil {
				logger.Sugar.Errorf("Failed to save failed records for table %s: %v", tableName, err)
			}
		}
	}

	return nil
}

// processTableInfo handles the processing of a single table's data
func (m *migrationRunner) processTableInfo(infoChan *dtos.TableInfoChan) []map[string]any {
	var records []map[string]any
	checkAllRecordsProcessed := make(map[string]uint64)
	processedRecordsChan := false

	// Initialize failed records tracking for this table if needed
	if _, exists := m.failedRecords[infoChan.TableInfo.TableName]; !exists {
		m.failedRecords[infoChan.TableInfo.TableName] = []map[string]any{}
	}

	// Process records from the channel
	for !processedRecordsChan {
		select {
		case record, ok := <-infoChan.RecordsChan:
			if !ok {
				processedRecordsChan = true
				break
			}

			// Append the record to our batch
			records = append(records, record)

			// Generate a unique ID for tracking this record
			uuidStr := uuid.New().String()
			checkAllRecordsProcessed[uuidStr] = 0

			// Process in batches based on configured record batch size
			if len(records) >= m.workerConfig.RecordBatchSize {
				// Process exactly the record batch size number of records
				batchRecords := records[:m.workerConfig.RecordBatchSize]
				m.processBatch(infoChan, batchRecords, checkAllRecordsProcessed)

				// Keep any remaining records for the next batch
				if len(records) > m.workerConfig.RecordBatchSize {
					records = records[m.workerConfig.RecordBatchSize:]
				} else {
					records = nil // Clear the batch if we processed all records
				}
			}

		case <-time.After(time.Duration(m.workerConfig.BatchProcessingTimeoutMs) * time.Millisecond): // Use configured batch processing timeout
			// Process any accumulated records if we have some and haven't received any new ones for the configured timeout
			if len(records) > 0 {
				// If we have records exceeding the record batch size, process them in appropriate batches
				if len(records) >= m.workerConfig.RecordBatchSize {
					// Process exactly the record batch size number of records
					batchRecords := records[:m.workerConfig.RecordBatchSize]
					m.processBatch(infoChan, batchRecords, checkAllRecordsProcessed)

					// Keep any remaining records for the next batch
					records = records[m.workerConfig.RecordBatchSize:]
				} else {
					// Process all remaining records if less than batch size
					m.processBatch(infoChan, records, checkAllRecordsProcessed)
					records = nil
				}
			}

			// Check if we're done processing all records for this table
			if m.checkTableProcessed(infoChan, checkAllRecordsProcessed) {
				processedRecordsChan = true
			}
		}
	}

	return records
}

// processBatch handles processing a batch of records
func (m *migrationRunner) processBatch(infoChan *dtos.TableInfoChan, records []map[string]any, checkAllRecordsProcessed map[string]uint64) {
	logger.Sugar.Infof("Migration for table %s in progress, batch size: %d/%d records, batch timeout: %dms, total uuids read: %d, total records read: %d, total records processed: %d, time taken: %s",
		infoChan.TableInfo.TableName,
		len(records),
		m.workerConfig.RecordBatchSize,
		m.workerConfig.BatchProcessingTimeoutMs,
		infoChan.GetTotalUuidsRead(),
		infoChan.GetTotalRecordsRead(),
		lo.Sum(lo.Values(checkAllRecordsProcessed)),
		time.Since(m.startTime).String(),
	)

	// Generate a unique tracking ID for this batch
	uuidStr := uuid.New().String()

	// Convert records to JSON for Doris
	bytesData, err := utils.ConvertRecordsToJSON(records, fmt.Sprintf("final/%s_debug.json", uuidStr), false)
	if err != nil {
		logger.Sugar.Errorf("Failed to marshal records for table %s: %v", infoChan.TableInfo.TableName, err)
		// Add the records to the failed records collection
		m.failedRecords[infoChan.TableInfo.TableName] = append(m.failedRecords[infoChan.TableInfo.TableName], records...)
		return
	}

	// Send the data to Doris
	err = DorisSyncService.SyncDoris(bytesData, uint64(len(records)), infoChan.TableInfo.TableName, uuidStr, checkAllRecordsProcessed)
	if err != nil {
		logger.Sugar.Errorf("Failed to sync data to Doris for table %s: %v", infoChan.TableInfo.TableName, err)
		// Add the records to the failed records collection
		m.failedRecords[infoChan.TableInfo.TableName] = append(m.failedRecords[infoChan.TableInfo.TableName], records...)
		return
	}
}

// checkTableProcessed determines if processing for a table is complete
func (m *migrationRunner) checkTableProcessed(
	infoChan *dtos.TableInfoChan,
	checkAllRecordsProcessed map[string]uint64,
) bool {
	// Check if we've read all records and processed all records
	totalProcessed := lo.Sum(lo.Values(checkAllRecordsProcessed))

	// Consider failed records when determining if we're done
	totalFailedRecords := len(m.failedRecords[infoChan.TableInfo.TableName])

	// If we've read all records and processed all of them (including failures), we're done with this table
	if infoChan.ReadingRecordsDone.Load().(bool) && (totalProcessed+uint64(totalFailedRecords)) == infoChan.GetTotalRecordsRead() {
		logger.Sugar.Infof("Migration for table %s completed, workers: %d, batch size: %d, batch timeout: %dms, total uuids read: %d, total records read: %d, total records processed: %d, failed records: %d, time taken: %s",
			infoChan.TableInfo.TableName,
			m.workerConfig.NoOfWorkers,
			m.workerConfig.RecordBatchSize,
			m.workerConfig.BatchProcessingTimeoutMs,
			infoChan.GetTotalUuidsRead(),
			infoChan.GetTotalRecordsRead(),
			totalProcessed,
			totalFailedRecords,
			time.Since(m.startTime).String(),
		)
		return true
	}

	return false
}
