package services

import (
	"context"
	"encoding/json"
	"migration-tool-go/dtos"
	"migration-tool-go/dtos/common"
	"migration-tool-go/dtos/destinations/kafka"
	"migration-tool-go/logger"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

var KafkaConnector = &kafkaConnector{}

type kafkaConnector struct {
	brokers           []string
	connectionDetails kafka.ConnectionDetails
	configuration     kafka.Configuration
	tableInfo         *dtos.TableInfoChan
	processedRecords  *sync.Map
	recordsProcessed  atomic.Uint64
	batchSize         int
	workerCount       int
	workers           []*kafkaWorker
	recordQueue       chan []map[string]any
	wg                sync.WaitGroup
	producer          sarama.AsyncProducer
	producerMutex     *sync.Mutex
	processingDone    atomic.Bool
	successChan       chan *sarama.ProducerMessage
	errorChan         chan *sarama.ProducerError
	monitorCancel     context.CancelFunc
	monitorCtxCancel  context.CancelFunc
}

type kafkaWorker struct {
	id              uint32
	connector       *kafkaConnector
	processedCount  atomic.Uint64
	failedRecords   []map[string]any
	failedRecordsMu sync.Mutex
}

// Initialize sets up the Kafka connector
func (k *kafkaConnector) Initialize(ctx context.Context, tableInfo *dtos.TableInfoChan, recordsProcessedTracker *sync.Map) {
	k.tableInfo = tableInfo
	k.processedRecords = recordsProcessedTracker
	k.recordsProcessed.Store(0)
	k.processingDone.Store(false)

	// Create a buffered channel for batching records
	k.recordQueue = make(chan []map[string]any, 100)

	// Initialize workers
	k.workers = make([]*kafkaWorker, 0, k.workerCount)

	// Start worker goroutines
	k.wg.Add(k.workerCount)
	for i := 0; i < k.workerCount; i++ {
		worker := &kafkaWorker{
			id:        uint32(i),
			connector: k,
		}
		k.workers = append(k.workers, worker)

		// Start the worker goroutine
		go func(w *kafkaWorker) {
			defer k.wg.Done()
			logger.Sugar.Infof("Starting worker %d", w.id)

			for batch := range k.recordQueue {
				if batch == nil {
					logger.Sugar.Infof("Worker %d received nil batch, exiting", w.id)
					return
				}

				logger.Sugar.Infof("Worker %d processing batch of %d records", w.id, len(batch))

				// Convert records to Kafka messages
				messages, err := w.convertToKafkaMessages(batch)
				if err != nil {
					logger.Sugar.Errorf("❌ Worker %d failed to convert records to Kafka messages: %v", w.id, err)
					continue
				}

				// Send messages to Kafka
				if err := w.sendMessagesWithRetry(messages); err != nil {
					logger.Sugar.Errorf("❌ Worker %d failed to send messages: %v", w.id, err)

					// Track failed records
					w.failedRecordsMu.Lock()
					w.failedRecords = append(w.failedRecords, batch...)
					w.failedRecordsMu.Unlock()
				}
			}

			logger.Sugar.Infof("Worker %d finished processing", w.id)
		}(worker)
	}

	logger.Sugar.Infof("Kafka connector initialized with %d workers and batch size %d", k.workerCount, k.batchSize)
}

// ProcessRecords processes records from the channel
func (k *kafkaConnector) ProcessRecords(ctx context.Context, recordsChan <-chan map[string]any) {
	logger.Sugar.Infof("Starting to process records for Kafka connector")

	// Process records from the channel
	var batch []map[string]any
	batchTimer := time.NewTimer(5 * time.Second)
	defer batchTimer.Stop()

	logger.Sugar.Info("Starting to receive records from channel")

	for {
		select {
		case record, ok := <-recordsChan:
			if !ok {
				// Channel closed, send any remaining records
				logger.Sugar.Info("Record channel closed, sending remaining records")
				if len(batch) > 0 {
					logger.Sugar.Infof("Sending final batch of %d records", len(batch))
					k.recordQueue <- batch
				}

				// Close the record queue to signal workers to finish
				close(k.recordQueue)

				// Wait for all workers to finish processing
				logger.Sugar.Info("Waiting for all workers to finish processing...")
				k.wg.Wait()

				// Set processing done flag
				k.processingDone.Store(true)

				// Log completion statistics
				totalProcessed := k.GetProcessedCount()
				totalRecordsRead := k.tableInfo.GetTotalRecordsRead()
				logger.Sugar.Infof("All records have been processed. Processed: %d, Total Read: %d",
					totalProcessed, totalRecordsRead)

				return
			}

			batch = append(batch, record)

			// If we've reached the batch size, send the batch
			if len(batch) >= k.batchSize {
				logger.Sugar.Infof("Batch size reached (%d), sending batch", len(batch))
				k.recordQueue <- batch
				batch = make([]map[string]any, 0, k.batchSize)
				batchTimer.Reset(5 * time.Second)
			}

		case <-batchTimer.C:
			// Time-based batching - send whatever we have after timeout
			if len(batch) > 0 {
				logger.Sugar.Infof("Batch timer expired, sending batch of %d records", len(batch))
				k.recordQueue <- batch
				batch = make([]map[string]any, 0, k.batchSize)
			}
			batchTimer.Reset(5 * time.Second)

		case <-ctx.Done():
			// Context canceled, send any remaining records
			logger.Sugar.Info("Context canceled, sending remaining records")
			if len(batch) > 0 {
				logger.Sugar.Infof("Sending final batch of %d records due to context cancellation", len(batch))
				k.recordQueue <- batch
			}

			// Close the record queue to signal workers to finish
			close(k.recordQueue)

			// Wait for workers to finish with a timeout
			waitCh := make(chan struct{})
			go func() {
				k.wg.Wait()
				close(waitCh)
			}()

			select {
			case <-waitCh:
				logger.Sugar.Info("All workers finished processing after context cancellation")
			case <-time.After(10 * time.Second):
				logger.Sugar.Warn("Timed out waiting for workers to finish after context cancellation")
			}

			// Set processing done flag
			k.processingDone.Store(true)
			logger.Sugar.Info("Processing done due to context cancellation")
			return
		}
	}
}

// Close closes the Kafka connector and releases all resources
func (k *kafkaConnector) Close() error {
	logger.Sugar.Info("Closing Kafka connector...")

	// Cancel the monitoring context to stop all monitoring goroutines
	if k.monitorCtxCancel != nil {
		logger.Sugar.Info("Cancelling monitoring context")
		k.monitorCtxCancel()
	}

	// Close the producer if it exists
	if k.producer != nil {
		logger.Sugar.Info("Closing Kafka producer")
		if err := k.producer.Close(); err != nil {
			logger.Sugar.Errorf("Error closing Kafka producer: %v", err)
			return err
		}
	}

	// Wait for any remaining worker goroutines to finish
	logger.Sugar.Info("Waiting for worker goroutines to finish")
	done := make(chan struct{})
	go func() {
		k.wg.Wait()
		close(done)
	}()

	// Wait for workers to finish with a timeout
	select {
	case <-done:
		logger.Sugar.Info("All worker goroutines finished")
	case <-time.After(5 * time.Second):
		logger.Sugar.Warn("Timed out waiting for worker goroutines to finish")
	}

	// Set processing done flag if not already set
	if !k.processingDone.Load() {
		logger.Sugar.Info("Setting processing done flag")
		k.processingDone.Store(true)
	}

	logger.Sugar.Info("Kafka connector closed successfully")
	return nil
}

// GetProcessedCount returns the total number of records processed
func (k *kafkaConnector) GetProcessedCount() uint64 {
	return k.recordsProcessed.Load()
}

// GetFailedRecords returns the records that failed to be processed
func (k *kafkaConnector) GetFailedRecords() []map[string]any {
	var failedRecords []map[string]any

	// Collect failed records from all workers
	for _, worker := range k.workers {
		worker.failedRecordsMu.Lock()
		failedRecords = append(failedRecords, worker.failedRecords...)
		worker.failedRecordsMu.Unlock()
	}

	return failedRecords
}

// IsProcessingDone checks if the connector has finished processing all records
func (k *kafkaConnector) IsProcessingDone() bool {
	// Check if processing is done and all workers have finished
	if !k.processingDone.Load() {
		// Perform an additional check to see if we should consider processing done
		// This helps when all records have been processed but the processingDone flag hasn't been set yet
		if k.tableInfo != nil && k.tableInfo.ReadingRecordsDone.Load().(bool) {
			totalProcessed := k.GetProcessedCount()
			totalFailedRecords := uint64(len(k.GetFailedRecords()))
			totalRecordsRead := k.tableInfo.GetTotalRecordsRead()

			// If we've processed all records that were read, we can consider processing done
			// This is a safety check in case the channel closing logic didn't work properly
			if totalRecordsRead > 0 && (totalProcessed+totalFailedRecords) >= totalRecordsRead {
				logger.Sugar.Infof("All records processed despite processingDone flag not set - Processed: %d, Failed: %d, Total Read: %d",
					totalProcessed, totalFailedRecords, totalRecordsRead)

				// Set the processing done flag since we've determined all records are processed
				k.processingDone.Store(true)
				return true
			}
		}
		return false
	}

	// Additional check: ensure all records in the queue have been processed
	// This is a more comprehensive check than just relying on the processingDone flag
	if k.tableInfo != nil && k.tableInfo.ReadingRecordsDone.Load().(bool) {
		totalProcessed := k.GetProcessedCount()
		totalFailedRecords := uint64(len(k.GetFailedRecords()))
		totalRecordsRead := k.tableInfo.GetTotalRecordsRead()

		// Log the current state for debugging
		logger.Sugar.Infof("IsProcessingDone check - Processed: %d, Failed: %d, Total Read: %d",
			totalProcessed, totalFailedRecords, totalRecordsRead)

		// Check if we've processed all records (including failures)
		if (totalProcessed + totalFailedRecords) >= totalRecordsRead {
			logger.Sugar.Infof("All records processed - Processed: %d, Failed: %d, Total Read: %d",
				totalProcessed, totalFailedRecords, totalRecordsRead)
			return true
		}
	}

	return false
}

// Worker methods

func (w *kafkaWorker) processRecords(ctx context.Context, wg *sync.WaitGroup) {
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

func (w *kafkaWorker) processBatch(records []map[string]any) {
	if len(records) == 0 {
		return
	}

	// Generate a unique label for this batch
	uniqueLabel := uuid.New().String()

	// Determine the topic to use
	topic := w.connector.configuration.Topic
	if topic == "" {
		topic = w.connector.tableInfo.TableInfo.TableName
	}

	// Create Kafka messages
	var messages []*sarama.ProducerMessage
	for _, record := range records {
		// Convert record to JSON
		jsonData, err := json.Marshal(record)
		if err != nil {
			logger.Sugar.Errorf("Worker %d failed to marshal record to JSON: %v", w.id, err)
			w.addFailedRecord(record)
			continue
		}

		// Create message
		key := sarama.StringEncoder(uniqueLabel)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   key,
			Value: sarama.ByteEncoder(jsonData),
		}
		messages = append(messages, msg)
	}

	// Send messages to Kafka
	err := w.sendMessagesWithRetry(messages)
	if err != nil {
		logger.Sugar.Errorf("Worker %d failed to send messages to Kafka: %v", w.id, err)
		// Add all records to failed records
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

func (w *kafkaWorker) addFailedRecord(record map[string]any) {
	w.failedRecordsMu.Lock()
	defer w.failedRecordsMu.Unlock()
	w.failedRecords = append(w.failedRecords, record)
}

func (w *kafkaWorker) addFailedRecords(records []map[string]any) {
	w.failedRecordsMu.Lock()
	defer w.failedRecordsMu.Unlock()
	w.failedRecords = append(w.failedRecords, records...)
}

func (w *kafkaWorker) sendMessagesWithRetry(messages []*sarama.ProducerMessage) error {
	st := time.Now()

	// No need for mutex with async producer since it's thread-safe
	for _, msg := range messages {
		// Send message to the async producer
		w.connector.producer.Input() <- msg
	}

	// Increment processed count at the connector level - this is critical for IsProcessingDone to work correctly
	count := uint64(len(messages))
	w.connector.recordsProcessed.Add(count)

	// Also update the tableInfo totalRecordsProcessed counter - this is critical for the migration runner to know when all records are processed
	w.connector.tableInfo.IncrementTotalRecordsProcessed(count)

	// Also update the worker's processed count
	w.processedCount.Add(count)

	logger.Sugar.Infof("✅ Worker %d sent %d messages in %v", w.id, len(messages), time.Since(st))
	return nil
}

func (w *kafkaWorker) convertToKafkaMessages(records []map[string]any) ([]*sarama.ProducerMessage, error) {
	var messages []*sarama.ProducerMessage
	for _, record := range records {
		// Convert record to JSON
		jsonData, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}

		// Create message
		key := sarama.StringEncoder(uuid.New().String())
		msg := &sarama.ProducerMessage{
			Topic: w.connector.configuration.Topic,
			Key:   key,
			Value: sarama.ByteEncoder(jsonData),
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

// NewKafkaConnector creates a new Kafka connector
func NewKafkaConnector(destination common.Destination[any]) {
	kafkaConfig := destination.Value.(kafka.Kafka)
	brokers := strings.Split(kafkaConfig.ConnectionDetails.Brokers, ",")

	// Initialize Kafka producer
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.RequiredAcks = sarama.WaitForLocal
	producerConfig.Producer.Retry.Max = 10
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Return.Errors = true

	// Performance optimizations
	producerConfig.Producer.Flush.Frequency = time.Duration(kafkaConfig.Configuration.FlushFrequencyMs) * time.Millisecond
	producerConfig.Producer.Flush.MaxMessages = kafkaConfig.Configuration.FlushMessages
	producerConfig.Producer.Flush.Bytes = kafkaConfig.Configuration.FlushBytes
	producerConfig.Producer.MaxMessageBytes = kafkaConfig.Configuration.MaxMessageBytes

	// Enable compression if configured
	if kafkaConfig.Configuration.CompressionEnabled {
		switch kafkaConfig.Configuration.CompressionType {
		case "gzip":
			producerConfig.Producer.Compression = sarama.CompressionGZIP
		case "snappy":
			producerConfig.Producer.Compression = sarama.CompressionSnappy
		case "lz4":
			producerConfig.Producer.Compression = sarama.CompressionLZ4
		case "zstd":
			producerConfig.Producer.Compression = sarama.CompressionZSTD
		default:
			producerConfig.Producer.Compression = sarama.CompressionNone
		}
	}

	// Set batch size
	producerConfig.Producer.Flush.Messages = kafkaConfig.Configuration.BatchSize

	// Set max open requests
	producerConfig.Net.MaxOpenRequests = kafkaConfig.Configuration.MaxOpenRequests

	// Create success and error channels with sufficient buffer size
	successChan := make(chan *sarama.ProducerMessage, 100000) // Increased buffer size
	errorChan := make(chan *sarama.ProducerError, 10000)

	// Create the connector instance
	KafkaConnector = &kafkaConnector{
		brokers:           brokers,
		connectionDetails: kafkaConfig.ConnectionDetails,
		configuration:     kafkaConfig.Configuration,
		producerMutex:     &sync.Mutex{},
		successChan:       successChan,
		errorChan:         errorChan,
		batchSize:         10000, // Default batch size
		workerCount:       30,    // Default worker count
	}

	// Initialize processingDone to false
	KafkaConnector.processingDone.Store(false)

	// Initialize async producer
	producer, err := sarama.NewAsyncProducer(brokers, producerConfig)
	if err != nil {
		logger.Sugar.Fatalf("Failed to create Kafka producer: %v", err)
	}
	KafkaConnector.producer = producer

	// Shared counters for monitoring
	successCount := uint64(0)
	errorCount := uint64(0)

	// Create a cancellable context for the monitoring goroutines
	monitorCtx, monitorCtxCancel := context.WithCancel(context.Background())
	KafkaConnector.monitorCtxCancel = monitorCtxCancel

	// Start goroutines to handle success and error channels
	go func() {
		for msg := range producer.Successes() {
			// Forward to our success channel without blocking
			select {
			case successChan <- msg:
				// Message will be counted in the monitoring goroutine
			default:
				// Channel might be full, log directly and increment the counter anyway
				atomic.AddUint64(&successCount, 1)
				if atomic.LoadUint64(&successCount)%1000 == 0 {
					logger.Sugar.Infof("Processed %d messages successfully", atomic.LoadUint64(&successCount))
				}
			}
		}
	}()

	go func() {
		for err := range producer.Errors() {
			// Forward to our error channel without blocking
			select {
			case errorChan <- err:
				// Error will be counted in the monitoring goroutine
			default:
				// Channel might be full, log directly
				logger.Sugar.Errorf("Kafka producer error: %v", err.Err)
				atomic.AddUint64(&errorCount, 1)
			}
		}
	}()

	// Start a goroutine to monitor and log success/error rates
	go func() {
		ticker := time.NewTicker(3 * time.Second) // Even more frequent updates
		defer ticker.Stop()

		for {
			select {
			case <-successChan:
				atomic.AddUint64(&successCount, 1)
			case err := <-errorChan:
				atomic.AddUint64(&errorCount, 1)
				logger.Sugar.Errorf("Kafka producer error: %v", err)
			case <-ticker.C:
				currentSuccess := atomic.LoadUint64(&successCount)
				currentErrors := atomic.LoadUint64(&errorCount)
				totalProcessed := KafkaConnector.recordsProcessed.Load()

				if currentSuccess > 0 || currentErrors > 0 {
					// Sanity check: success count should never exceed processed count
					if currentSuccess > totalProcessed {
						logger.Sugar.Warnf("Success count (%d) exceeds total processed count (%d), capping at total processed",
							currentSuccess, totalProcessed)
						// Cap the success count at the total processed count
						currentSuccess = totalProcessed
					}

					logger.Sugar.Infof("Kafka producer stats - Success: %d, Errors: %d, Total processed: %d",
						currentSuccess,
						currentErrors,
						totalProcessed)
				}
			case <-monitorCtx.Done():
				return
			}
		}
	}()

	logger.Sugar.Infof("Kafka connector initialized with brokers: %v, topic: %s", brokers, kafkaConfig.Configuration.Topic)
}
