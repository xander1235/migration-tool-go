package services

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"migration-tool-go/dtos/common"
	"migration-tool-go/dtos/destinations/kafka"
	"migration-tool-go/logger"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
	"golang.org/x/sync/semaphore"
)

// XDGSCRAMClient is a SCRAM client for Sarama
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

type kafkaSyncService struct {
	connectionDetails kafka.ConnectionDetails
	configuration     kafka.Configuration
	producerPool      *KafkaProducerPool
	mu                sync.Mutex
	
	// Rate limiting and concurrency control
	workerSemaphore   *semaphore.Weighted
	batchSizes        map[string]int
	rateLimiters      map[string]*time.Ticker
	
	// Metrics
	messagesSent      int64
	messagesDropped   int64
	batchesSent       int64
	errors            int64
	metricsLock       sync.RWMutex
	lastMetricsLog    time.Time
}

// Global service instance for backward compatibility
var KafkaSyncService *kafkaSyncService

func NewKafkaSync(destination common.Destination[any]) (*kafkaSyncService, error) {
	// Extract Kafka value from destination
	kafkaValue, ok := destination.Value.(kafka.Kafka)
	if !ok {
		return nil, fmt.Errorf("invalid Kafka destination value")
	}

	// Create a new Kafka sync service
	service := &kafkaSyncService{
		connectionDetails: kafkaValue.ConnectionDetails,
		configuration:     kafkaValue.Configuration,
		batchSizes:        make(map[string]int),
		rateLimiters:      make(map[string]*time.Ticker),
		lastMetricsLog:    time.Now(),
	}

	// Set default batch size
	defaultBatchSize := 100
	if service.configuration.BatchSize > 0 {
		defaultBatchSize = service.configuration.BatchSize
	}
	
	// Initialize semaphore for worker concurrency control
	maxWorkers := 10 // Default max concurrent workers
	if service.configuration.PoolSize > 0 {
		maxWorkers = service.configuration.PoolSize
	}
	service.workerSemaphore = semaphore.NewWeighted(int64(maxWorkers))
	
	// Start metrics reporting
	go service.reportMetricsPeriodically()

	// Initialize Kafka producer pool
	err := service.initProducerPool()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Kafka producer pool: %w", err)
	}

	// Set global instance for backward compatibility
	KafkaSyncService = service

	logger.Sugar.Infof("Initialized Kafka sync service with max %d concurrent workers and default batch size %d", 
		maxWorkers, defaultBatchSize)
	return service, nil
}

// reportMetricsPeriodically logs metrics every minute
func (k *kafkaSyncService) reportMetricsPeriodically() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		k.metricsLock.RLock()
		logger.Sugar.Infof("Kafka metrics - Messages sent: %d, Batches: %d, Errors: %d, Dropped: %d", 
			k.messagesSent, k.batchesSent, k.errors, k.messagesDropped)
		k.metricsLock.RUnlock()
	}
}

func (k *kafkaSyncService) initProducerPool() error {
	// Create Sarama config
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	config.Producer.Flush.Frequency = 500 * time.Millisecond

	// Set retry configuration if specified
	if k.configuration.RetryMax > 0 {
		config.Producer.Retry.Max = k.configuration.RetryMax
	}
	
	// Set flush frequency if specified
	if k.configuration.FlushFrequencyMs > 0 {
		config.Producer.Flush.Frequency = time.Duration(k.configuration.FlushFrequencyMs) * time.Millisecond
	}

	// Set compression if specified
	if k.configuration.CompressionEnabled {
		switch strings.ToLower(k.configuration.CompressionType) {
		case "gzip":
			config.Producer.Compression = sarama.CompressionGZIP
		case "snappy":
			config.Producer.Compression = sarama.CompressionSnappy
		case "lz4":
			config.Producer.Compression = sarama.CompressionLZ4
		case "zstd":
			config.Producer.Compression = sarama.CompressionZSTD
		default:
			config.Producer.Compression = sarama.CompressionNone
		}
	}

	// Set SASL authentication if needed
	if k.connectionDetails.UseSASL {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = k.connectionDetails.Username
		config.Net.SASL.Password = k.connectionDetails.Password

		switch strings.ToLower(k.connectionDetails.SASLMechanism) {
		case "plain":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "scram-sha-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: scram.SHA256}
			}
		case "scram-sha-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: scram.SHA512}
			}
		default:
			return fmt.Errorf("unsupported SASL mechanism: %s", k.connectionDetails.SASLMechanism)
		}
	}

	// Configure TLS if needed
	if k.connectionDetails.UseTLS {
		config.Net.TLS.Enable = true

		// If certificates are provided, configure TLS
		if k.connectionDetails.ClientCertFile != "" && k.connectionDetails.ClientKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(k.connectionDetails.ClientCertFile, k.connectionDetails.ClientKeyFile)
			if err != nil {
				return fmt.Errorf("failed to load client certificates: %w", err)
			}
			config.Net.TLS.Config = &tls.Config{
				Certificates: []tls.Certificate{cert},
			}
		}

		// Skip verification if configured
		if k.connectionDetails.TLSSkipVerify {
			if config.Net.TLS.Config == nil {
				config.Net.TLS.Config = &tls.Config{}
			}
			config.Net.TLS.Config.InsecureSkipVerify = true
		}
	}

	// Create producer pool
	poolSize := 5 // Default pool size
	if k.configuration.PoolSize > 0 {
		poolSize = k.configuration.PoolSize
	}

	// Split brokers string into slice
	brokers := strings.Split(k.connectionDetails.Brokers, ",")
	
	pool, err := NewKafkaProducerPool(brokers, config, poolSize)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer pool: %w", err)
	}

	k.producerPool = pool
	return nil
}

// getBatchSizeForLabel gets the batch size for a specific label
func (k *kafkaSyncService) getBatchSizeForLabel(label string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	
	// Check if we have a custom batch size for this label
	if size, ok := k.batchSizes[label]; ok {
		return size
	}
	
	// Use default batch size
	defaultSize := 100
	if k.configuration.BatchSize > 0 {
		defaultSize = k.configuration.BatchSize
	}
	
	// Store for future use
	k.batchSizes[label] = defaultSize
	return defaultSize
}

// getRateLimiterForLabel gets or creates a rate limiter for a specific label
func (k *kafkaSyncService) getRateLimiterForLabel(label string) *time.Ticker {
	k.mu.Lock()
	defer k.mu.Unlock()
	
	// Check if we already have a rate limiter for this label
	if limiter, ok := k.rateLimiters[label]; ok {
		return limiter
	}
	
	// Create a new rate limiter
	// Default to 100ms (10 operations per second)
	interval := 100 * time.Millisecond
	
	limiter := time.NewTicker(interval)
	k.rateLimiters[label] = limiter
	return limiter
}

// SyncRecords synchronizes records to Kafka
func (k *kafkaSyncService) SyncRecords(ctx context.Context, records []map[string]any, topic string, uniqueLabel string) error {
	if len(records) == 0 {
		logger.Sugar.Infof("No records to sync for label %s", uniqueLabel)
		return nil
	}

	st := time.Now()
	noOfRecords := len(records)
	logger.Sugar.Infof("Starting to sync %d records to Kafka topic %s for label %s", noOfRecords, topic, uniqueLabel)

	// Get batch size for this label
	batchSize := k.getBatchSizeForLabel(uniqueLabel)
	
	// Get rate limiter for this label
	rateLimiter := k.getRateLimiterForLabel(uniqueLabel)
	
	// Calculate number of batches
	numBatches := (len(records) + batchSize - 1) / batchSize
	
	// Create a wait group to wait for all batches to complete
	var wg sync.WaitGroup
	wg.Add(numBatches)
	
	// Create a channel for errors
	errorChan := make(chan error, numBatches)
	
	// Process each batch
	for i := 0; i < len(records); i += batchSize {
		// Apply rate limiting
		<-rateLimiter.C
		
		// Acquire semaphore to limit concurrent workers
		if err := k.workerSemaphore.Acquire(ctx, 1); err != nil {
			logger.Sugar.Errorf("Failed to acquire semaphore: %v", err)
			continue
		}
		
		// Calculate batch end
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		
		// Get batch
		batch := records[i:end]
		
		// Process batch in a goroutine
		go func(batchRecords []map[string]any, batchIndex int) {
			defer wg.Done()
			defer k.workerSemaphore.Release(1)
			
			// Process this batch
			err := k.processBatch(ctx, batchRecords, topic, uniqueLabel, batchIndex, numBatches)
			if err != nil {
				errorChan <- err
			}
		}(batch, i/batchSize)
	}
	
	// Wait for all batches to complete
	wg.Wait()
	close(errorChan)
	
	// Check for errors
	var errs []string
	for err := range errorChan {
		errs = append(errs, err.Error())
	}
	
	// If there were errors, return them
	if len(errs) > 0 {
		k.metricsLock.Lock()
		k.errors += int64(len(errs))
		k.metricsLock.Unlock()
		return fmt.Errorf("failed to sync some batches: %s", strings.Join(errs, "; "))
	}
	
	// Log success
	logger.Sugar.Infof("✅ Successfully synced %d records to Kafka for label %s in %v", 
		noOfRecords, uniqueLabel, time.Since(st))
	return nil
}

// processBatch processes a single batch of records
func (k *kafkaSyncService) processBatch(ctx context.Context, records []map[string]any, topic string, uniqueLabel string, batchIndex, totalBatches int) error {
	// Create messages
	messages := make([]*sarama.ProducerMessage, 0, len(records))
	
	for _, record := range records {
		// Generate a unique key for the message
		key := uuid.New().String()
		
		// Marshal record to JSON
		value, err := json.Marshal(record)
		if err != nil {
			logger.Sugar.Errorf("Failed to marshal record to JSON: %v", err)
			
			k.metricsLock.Lock()
			k.messagesDropped++
			k.errors++
			k.metricsLock.Unlock()
			
			continue
		}
		
		// Create message
		message := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(value),
		}
		
		messages = append(messages, message)
	}
	
	// If no messages, return
	if len(messages) == 0 {
		return nil
	}
	
	// Send messages
	err := k.producerPool.SendMessagesWithTimeout(ctx, messages)
	if err != nil {
		logger.Sugar.Errorf("Failed to send batch %d/%d to Kafka: %v", 
			batchIndex+1, totalBatches, err)
		
		k.metricsLock.Lock()
		k.errors++
		k.messagesDropped += int64(len(messages))
		k.metricsLock.Unlock()
		
		return fmt.Errorf("failed to send batch %d/%d: %w", batchIndex+1, totalBatches, err)
	}
	
	// Update metrics
	k.metricsLock.Lock()
	k.messagesSent += int64(len(messages))
	k.batchesSent++
	k.metricsLock.Unlock()
	
	// Log progress every 10 batches
	if (batchIndex+1)%10 == 0 || batchIndex+1 == totalBatches {
		logger.Sugar.Infof("Progress: processed %d/%d batches for label %s", 
			batchIndex+1, totalBatches, uniqueLabel)
	}
	
	return nil
}

// SyncKafka is the legacy method for backward compatibility
func (k *kafkaSyncService) SyncKafka(records []map[string]any, noOfRecords uint64, table string, uniqueLabel string, checkAllRecordsProcessed *sync.Map, workerId uint32) error {
	// Determine the topic to use - either use the configured topic or the table name
	topic := k.configuration.Topic
	if topic == "" {
		topic = table
	}
	
	// Call the new method
	err := k.SyncRecords(context.Background(), records, topic, uniqueLabel)
	
	// Update the processed records counter using sync.Map
	currentCount, _ := checkAllRecordsProcessed.LoadOrStore(uniqueLabel, uint64(0))
	checkAllRecordsProcessed.Store(uniqueLabel, currentCount.(uint64)+noOfRecords)
	
	return err
}

// Close closes the Kafka sync service
func (k *kafkaSyncService) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	
	// Stop all rate limiters
	for _, limiter := range k.rateLimiters {
		limiter.Stop()
	}
	
	// Clear maps
	k.rateLimiters = make(map[string]*time.Ticker)
	k.batchSizes = make(map[string]int)
	
	// Close producer pool
	if k.producerPool != nil {
		k.producerPool.Close()
	}
	
	logger.Sugar.Info("Kafka sync service closed")
	return nil
}
