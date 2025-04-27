package services

import (
	"context"
	"fmt"
	"migration-tool-go/logger"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// KafkaProducerPool manages a pool of Kafka producers with circuit breaking
type KafkaProducerPool struct {
	producers      chan sarama.SyncProducer
	config         *sarama.Config
	brokers        []string
	mu             sync.Mutex
	size           int
	
	// Circuit breaker state
	consecutiveFailures int
	lastFailureTime     time.Time
	circuitOpen         bool
	halfOpenAttempts    int
	
	// Circuit breaker settings
	failureThreshold    int           // Number of consecutive failures before opening circuit
	resetTimeout        time.Duration // How long to wait before trying half-open state
	halfOpenMaxAttempts int           // Max attempts in half-open state before reopening
	
	// Rate limiting
	rateLimiter         *time.Ticker  // Controls the rate of message sending
	maxBatchSize        int           // Maximum batch size per send operation
	
	// Metrics
	totalSent           int64
	totalErrors         int64
	metricsLock         sync.RWMutex
}

// NewKafkaProducerPool creates a new Kafka producer pool with the specified size
func NewKafkaProducerPool(brokers []string, config *sarama.Config, size int) (*KafkaProducerPool, error) {
	if size <= 0 {
		size = 5 // Default pool size
	}
	
	// Create the pool
	pool := &KafkaProducerPool{
		producers:           make(chan sarama.SyncProducer, size),
		config:              config,
		brokers:             brokers,
		size:                size,
		
		// Circuit breaker defaults
		failureThreshold:    3,
		resetTimeout:        30 * time.Second,
		halfOpenMaxAttempts: 2,
		
		// Rate limiting defaults
		rateLimiter:         time.NewTicker(10 * time.Millisecond), // 100 operations per second max
		maxBatchSize:        500,
	}
	
	// Initialize the pool with producers
	for i := 0; i < size; i++ {
		producer, err := sarama.NewSyncProducer(brokers, config)
		if err != nil {
			// Close any producers we've already created
			pool.Close()
			return nil, fmt.Errorf("failed to create Kafka producer %d/%d: %w", i+1, size, err)
		}
		
		// Add to pool
		pool.producers <- producer
	}
	
	// Start a goroutine to log metrics periodically
	go pool.logMetricsPeriodically()
	
	return pool, nil
}

// logMetricsPeriodically logs pool metrics every minute
func (p *KafkaProducerPool) logMetricsPeriodically() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		p.metricsLock.RLock()
		logger.Sugar.Infof("Kafka pool metrics - Messages sent: %d, Errors: %d, Circuit state: %v", 
			p.totalSent, p.totalErrors, p.getCircuitState())
		p.metricsLock.RUnlock()
	}
}

// getCircuitState returns a string representation of the circuit state
func (p *KafkaProducerPool) getCircuitState() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.circuitOpen {
		return "OPEN"
	} else if p.consecutiveFailures > 0 {
		return fmt.Sprintf("DEGRADED (%d/%d failures)", p.consecutiveFailures, p.failureThreshold)
	}
	return "CLOSED"
}

// GetProducer gets a producer from the pool with a timeout
func (p *KafkaProducerPool) GetProducer(ctx context.Context) (sarama.SyncProducer, error) {
	// Check circuit breaker state
	if !p.canProceed() {
		return nil, fmt.Errorf("circuit breaker is open, Kafka operations temporarily disabled")
	}
	
	// Try to get a producer from the pool
	select {
	case producer := <-p.producers:
		return producer, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout getting producer from pool: %w", ctx.Err())
	case <-time.After(5 * time.Second):
		// If we time out waiting for a producer, create a new one
		logger.Sugar.Warnf("Timeout waiting for Kafka producer from pool, creating new producer")
		producer, err := sarama.NewSyncProducer(p.brokers, p.config)
		if err != nil {
			p.recordFailure()
			return nil, fmt.Errorf("failed to create new Kafka producer: %w", err)
		}
		return producer, nil
	}
}

// ReturnProducer returns a producer to the pool
func (p *KafkaProducerPool) ReturnProducer(producer sarama.SyncProducer) {
	if producer == nil {
		return
	}
	
	// Try to return the producer to the pool, but don't block
	select {
	case p.producers <- producer:
		// Successfully returned to pool
	default:
		// Pool is full, close this producer
		if err := producer.Close(); err != nil {
			logger.Sugar.Errorf("Failed to close extra Kafka producer: %v", err)
		}
	}
}

// canProceed checks if the circuit breaker allows operations
func (p *KafkaProducerPool) canProceed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// If circuit is closed, we can proceed
	if !p.circuitOpen {
		return true
	}
	
	// Check if we should try half-open state
	if time.Since(p.lastFailureTime) > p.resetTimeout {
		// Try half-open state
		if p.halfOpenAttempts < p.halfOpenMaxAttempts {
			p.halfOpenAttempts++
			logger.Sugar.Infof("Circuit breaker in half-open state, attempt %d/%d", 
				p.halfOpenAttempts, p.halfOpenMaxAttempts)
			return true
		}
	}
	
	return false
}

// recordSuccess records a successful operation
func (p *KafkaProducerPool) recordSuccess() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Reset failure counters
	p.consecutiveFailures = 0
	p.circuitOpen = false
	p.halfOpenAttempts = 0
	
	// Update metrics
	p.metricsLock.Lock()
	p.totalSent++
	p.metricsLock.Unlock()
}

// recordFailure records a failed operation
func (p *KafkaProducerPool) recordFailure() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Update failure counters
	p.consecutiveFailures++
	p.lastFailureTime = time.Now()
	
	// Update metrics
	p.metricsLock.Lock()
	p.totalErrors++
	p.metricsLock.Unlock()
	
	// Check if we should open the circuit
	if p.consecutiveFailures >= p.failureThreshold {
		if !p.circuitOpen {
			logger.Sugar.Warnf("Opening circuit breaker after %d consecutive Kafka failures", 
				p.consecutiveFailures)
			p.circuitOpen = true
		}
	}
}

// SendMessagesWithTimeout sends messages using a producer from the pool with circuit breaking
func (p *KafkaProducerPool) SendMessagesWithTimeout(ctx context.Context, messages []*sarama.ProducerMessage) error {
	// Check circuit breaker
	if !p.canProceed() {
		return fmt.Errorf("circuit breaker is open, Kafka operations temporarily disabled")
	}
	
	// Apply rate limiting
	select {
	case <-p.rateLimiter.C:
		// We can proceed
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for rate limiter: %w", ctx.Err())
	}
	
	// Split messages into smaller batches if needed
	if len(messages) > p.maxBatchSize {
		logger.Sugar.Infof("Splitting %d messages into batches of %d", len(messages), p.maxBatchSize)
		
		// Process in batches
		for i := 0; i < len(messages); i += p.maxBatchSize {
			end := i + p.maxBatchSize
			if end > len(messages) {
				end = len(messages)
			}
			
			batch := messages[i:end]
			if err := p.sendBatch(ctx, batch); err != nil {
				return fmt.Errorf("error sending batch %d-%d: %w", i, end, err)
			}
		}
		
		return nil
	}
	
	// Send as a single batch
	return p.sendBatch(ctx, messages)
}

// sendBatch sends a single batch of messages with retries
func (p *KafkaProducerPool) sendBatch(ctx context.Context, messages []*sarama.ProducerMessage) error {
	// Maximum retries
	maxRetries := 3
	backoffTime := 100 * time.Millisecond
	
	// Try to send with retries
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Get a producer
		producer, err := p.GetProducer(ctx)
		if err != nil {
			logger.Sugar.Errorf("Failed to get Kafka producer (attempt %d/%d): %v", 
				attempt+1, maxRetries, err)
			
			// Wait before retry
			select {
			case <-time.After(backoffTime):
				// Exponential backoff
				backoffTime *= 2
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
			}
			
			continue
		}
		
		// Create a timeout context for this send operation
		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		
		// Send messages
		err = func() error {
			defer cancel()
			
			// Use a separate goroutine with timeout to prevent blocking indefinitely
			resultChan := make(chan error, 1)
			go func() {
				// Send the batch
				resultChan <- producer.SendMessages(messages)
			}()
			
			// Wait for result or timeout
			select {
			case err := <-resultChan:
				return err
			case <-sendCtx.Done():
				// Don't return the producer to the pool on timeout
				if closeErr := producer.Close(); closeErr != nil {
					logger.Sugar.Errorf("Failed to close timed out producer: %v", closeErr)
				}
				return fmt.Errorf("timeout sending messages: %w", sendCtx.Err())
			}
		}()
		
		// Handle the result
		if err == nil {
			// Success!
			p.ReturnProducer(producer)
			p.recordSuccess()
			return nil
		}
		
		// Error occurred
		logger.Sugar.Errorf("Error sending %d messages to Kafka (attempt %d/%d): %v", 
			len(messages), attempt+1, maxRetries, err)
		
		// Don't return this producer to the pool
		if closeErr := producer.Close(); closeErr != nil {
			logger.Sugar.Errorf("Failed to close failed producer: %v", closeErr)
		}
		
		// Record the failure
		p.recordFailure()
		
		// Wait before retry
		select {
		case <-time.After(backoffTime):
			// Exponential backoff
			backoffTime *= 2
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		}
	}
	
	// All retries failed
	return fmt.Errorf("failed to send messages after %d attempts", maxRetries)
}

// Close closes all producers in the pool
func (p *KafkaProducerPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Stop the rate limiter
	p.rateLimiter.Stop()
	
	// Close all producers in the pool
	close(p.producers)
	for producer := range p.producers {
		if err := producer.Close(); err != nil {
			logger.Sugar.Errorf("Failed to close Kafka producer: %v", err)
		}
	}
	
	logger.Sugar.Info("Kafka producer pool closed")
}
