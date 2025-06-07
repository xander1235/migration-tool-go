package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"migration-tool-go/dtos"
	"migration-tool-go/dtos/common"
	"migration-tool-go/dtos/destinations/doris"
	"migration-tool-go/logger"
	"migration-tool-go/utils"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var DorisConnector = &dorisConnector{}

type dorisConnector struct {
	connectionDetails doris.ConnectionDetails
	configuration     doris.Configuration
	tableInfo         *dtos.TableInfoChan
	processedRecords  *sync.Map
	recordsProcessed  atomic.Uint64
	batchSize         int
	workerCount       int
	workers           []*dorisWorker
	recordQueue       chan []map[string]any
	wg                sync.WaitGroup
	dorisClientMutex  sync.Mutex
	processingDone    atomic.Bool
}

type dorisWorker struct {
	id              uint32
	connector       *dorisConnector
	processedCount  atomic.Uint64
	failedRecords   []map[string]any
	failedRecordsMu sync.Mutex
}

// Initialize sets up the Doris connector
func (d *dorisConnector) Initialize(ctx context.Context, tableInfo *dtos.TableInfoChan, recordsProcessedTracker *sync.Map) {
	d.tableInfo = tableInfo
	d.processedRecords = recordsProcessedTracker
	d.batchSize = 10000 // Default batch size, can be configured
	d.workerCount = 30  // Default worker count, can be configured
	d.recordQueue = make(chan []map[string]any, 100)

	// Initialize workers
	d.workers = make([]*dorisWorker, d.workerCount)
	for i := 0; i < d.workerCount; i++ {
		d.workers[i] = &dorisWorker{
			id:            uint32(i),
			connector:     d,
			failedRecords: make([]map[string]any, 0),
		}
	}

	// Start workers
	for i := 0; i < d.workerCount; i++ {
		d.wg.Add(1)
		go d.workers[i].processRecords(ctx, &d.wg)
	}

	logger.Sugar.Infof("Doris connector initialized with %d workers and batch size %d", d.workerCount, d.batchSize)
}

// ProcessRecords starts processing records from the channel
func (d *dorisConnector) ProcessRecords(ctx context.Context, recordsChan <-chan map[string]any) {
	var batch []map[string]any
	batchTimer := time.NewTimer(5 * time.Second)
	defer batchTimer.Stop()

	for {
		select {
		case record, ok := <-recordsChan:
			if !ok {
				// Channel closed, send any remaining records
				if len(batch) > 0 {
					d.recordQueue <- batch
				}
				close(d.recordQueue)
				d.processingDone.Store(true)
				return
			}

			batch = append(batch, record)

			// If we've reached the batch size, send the batch
			if len(batch) >= d.batchSize {
				d.recordQueue <- batch
				batch = make([]map[string]any, 0, d.batchSize)
				batchTimer.Reset(5 * time.Second)
			}

		case <-batchTimer.C:
			// Time-based batching - send whatever we have after timeout
			if len(batch) > 0 {
				d.recordQueue <- batch
				batch = make([]map[string]any, 0, d.batchSize)
			}
			batchTimer.Reset(5 * time.Second)

		case <-ctx.Done():
			// Context canceled, send any remaining records
			if len(batch) > 0 {
				d.recordQueue <- batch
			}
			close(d.recordQueue)
			d.processingDone.Store(true)
			return
		}
	}
}

// Close closes the Doris connector and releases resources
func (d *dorisConnector) Close() error {
	logger.Sugar.Info("Closing Doris connector...")

	// Signal all workers to stop by closing the record queue
	// This should already be closed by ProcessRecords when the channel is closed
	// but we'll check if it's still open just to be safe
	select {
	case _, ok := <-d.recordQueue:
		if ok {
			// Queue is still open, close it
			close(d.recordQueue)
		}
	default:
		// Queue might be empty but still open
		select {
		case d.recordQueue <- nil:
			// Queue is still open, close it
			close(d.recordQueue)
		default:
			// Queue is likely closed
		}
	}

	// Wait with timeout for all workers to finish
	waitChan := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(waitChan)
	}()

	// Wait for workers to finish or timeout after 5 seconds
	select {
	case <-waitChan:
		logger.Sugar.Info("All Doris workers finished")
	case <-time.After(5 * time.Second):
		logger.Sugar.Warn("Timed out waiting for Doris workers to finish")
	}

	logger.Sugar.Info("Doris connector closed")
	return nil
}

// GetProcessedCount returns the total number of records processed
func (d *dorisConnector) GetProcessedCount() uint64 {
	return d.recordsProcessed.Load()
}

// GetFailedRecords returns the records that failed to be processed
func (d *dorisConnector) GetFailedRecords() []map[string]any {
	var failedRecords []map[string]any

	// Collect failed records from all workers
	for _, worker := range d.workers {
		worker.failedRecordsMu.Lock()
		failedRecords = append(failedRecords, worker.failedRecords...)
		worker.failedRecordsMu.Unlock()
	}

	return failedRecords
}

// IsProcessingDone checks if the connector has finished processing all records
func (d *dorisConnector) IsProcessingDone() bool {
	// Check if processing is done and all workers have finished
	if !d.processingDone.Load() {
		return false
	}

	// Additional check: ensure all records in the queue have been processed
	// This is a more comprehensive check than just relying on the processingDone flag
	if d.tableInfo.ReadingRecordsDone.Load().(bool) {
		totalProcessed := d.GetProcessedCount()
		totalFailedRecords := uint64(len(d.GetFailedRecords()))

		// If we've processed all records (including failures), we're done
		return (totalProcessed + totalFailedRecords) >= d.tableInfo.GetTotalRecordsRead()
	}

	return false
}

// Worker methods

func (w *dorisWorker) processRecords(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case batch, ok := <-w.connector.recordQueue:
			if !ok {
				// Queue closed, worker can exit
				return
			}

			// Process the batch
			w.processBatch(batch)

		case <-ctx.Done():
			return
		}
	}
}

func (w *dorisWorker) processBatch(records []map[string]any) {
	if len(records) == 0 {
		return
	}

	// Generate a unique label for this batch
	uniqueLabel := uuid.New().String()

	// Convert records to JSON
	jsonData, err := utils.ConvertRecordsToJSON(records, "", false)
	if err != nil {
		logger.Sugar.Errorf("Worker %d failed to convert records to JSON: %v", w.id, err)
		w.addFailedRecords(records)
		return
	}
	
	// Construct the Doris URL
	//beNodes := w.connector.connectionDetails.BeNodes
	//bePort := w.connector.connectionDetails.BePort
	database := w.connector.connectionDetails.Database
	tableName := w.connector.tableInfo.TableInfo.TableName

	dorisURL := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load", w.connector.connectionDetails.FeNodes, w.connector.connectionDetails.FePort, database, tableName)

	// Send the data to Doris with retry
	err = w.streamLoadDoris(dorisURL, w.connector.connectionDetails.Username, w.connector.connectionDetails.Password, jsonData, uniqueLabel, uint64(len(records)))

	if err != nil {
		logger.Sugar.Errorf("Worker %d failed to stream load to Doris: %v", w.id, err)
		w.addFailedRecords(records)
		return
	}

	// Update processed count
	count := uint64(len(records))
	w.processedCount.Add(count)
	w.connector.recordsProcessed.Add(count)
	w.connector.tableInfo.IncrementTotalRecordsProcessed(count)

	// Update the processed records map
	currentCount, _ := w.connector.processedRecords.LoadOrStore(uniqueLabel, uint64(0))
	w.connector.processedRecords.Store(uniqueLabel, currentCount.(uint64)+count)
}

func (w *dorisWorker) addFailedRecords(records []map[string]any) {
	w.failedRecordsMu.Lock()
	defer w.failedRecordsMu.Unlock()
	w.failedRecords = append(w.failedRecords, records...)
}

func (w *dorisWorker) streamLoadDoris(dorisURL, username, password string, jsonData []byte, uniqueLabel string, noOfRecords uint64) error {
	// Initialize retry parameters
	maxRetries := 10
	initialBackoff := 500 * time.Millisecond
	maxBackoff := 30 * time.Second
	backoff := initialBackoff
	attempt := 1

	for {
		st := time.Now()
		success := false
		var responseStatus string
		var responseBody string

		// Acquire the lock before sending to Doris
		w.connector.dorisClientMutex.Lock()

		// Create HTTP request
		req, err := http.NewRequest("PUT", dorisURL, bytes.NewReader(jsonData))
		if err != nil {
			logger.Sugar.Errorf("Worker %d failed to create request (attempt %d): %v", w.id, attempt, err)
		} else {
			// Set Stream Load headers
			req.Header.Set("Expect", "100-continue")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("format", "json")            // Specify JSON format
			req.Header.Set("strip_outer_array", "true") // Required for JSON array input
			req.Header.Set("label", uniqueLabel)        // Unique label
			req.Header.Set("send_batch_parallelism", "10")
			req.SetBasicAuth(username, password)

			// Send request with timeout
			client := &http.Client{
				Timeout: 5 * time.Minute, // Set a reasonable timeout
			}
			resp, err := client.Do(req)
			if err != nil {
				logger.Sugar.Errorf("Worker %d failed to send request (attempt %d): %v", w.id, attempt, err)
			} else {
				// Read response body
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close() // Close body explicitly

				if err != nil {
					logger.Sugar.Errorf("Worker %d failed to read response body (attempt %d): %v", w.id, attempt, err)
				} else {
					responseStatus = resp.Status
					responseBody = string(body)

					// Check response status
					if resp.StatusCode == http.StatusOK {
						// Success!
						logger.Sugar.Infof("Worker %d, ✅ Doris Stream Load Successful for label %s, with %d records, took %f secs (attempt %d)",
							w.id, uniqueLabel, noOfRecords, time.Since(st).Seconds(), attempt)
						success = true
					} else {
						logger.Sugar.Errorf("Worker %d, Doris Stream load failed (attempt %d) for label %s with %d records: status %s, response %s, took %f secs",
							w.id, attempt, uniqueLabel, noOfRecords, responseStatus, responseBody, time.Since(st).Seconds())
					}
				}
			}
		}

		// Release the lock after sending to Doris
		w.connector.dorisClientMutex.Unlock()

		if success {
			return nil
		}

		// Handle retry logic
		// If we've reached max retries, continue retrying but log less frequently
		if attempt >= maxRetries {
			if attempt%5 == 0 {
				logger.Sugar.Warnf("Worker %d still retrying after %d attempts for label %s with %d records. Will continue until successful.",
					w.id, attempt, uniqueLabel, noOfRecords)
			}
		} else {
			logger.Sugar.Warnf("Worker %d retrying (attempt %d/%d) for label %s with %d records after backoff of %v",
				w.id, attempt, maxRetries, uniqueLabel, noOfRecords, backoff)
		}

		// Wait before retrying
		time.Sleep(backoff)

		// Increase backoff for next retry with exponential backoff and jitter
		jitter := time.Duration(float64(backoff) * 0.1 * (0.5 + rand.Float64())) // 10% jitter
		backoff = time.Duration(float64(backoff)*1.5) + jitter
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		attempt++
		// No maximum retry limit - we'll keep trying until successful
	}
}

// NewDorisConnector creates a new Doris connector
func NewDorisConnector(destination common.Destination[any]) {
	DorisConnector = &dorisConnector{
		connectionDetails: destination.Value.(doris.Doris).ConnectionDetails,
		configuration:     destination.Value.(doris.Doris).Configuration,
	}
	// Initialize processingDone to false
	DorisConnector.processingDone.Store(false)
}
