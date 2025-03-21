package utils

import (
	"encoding/csv"
	"fmt"
	"migration-tool-go/logger"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

// StatsCollector collects and logs system metrics at specified intervals
type StatsCollector struct {
	interval      time.Duration
	ticker        *time.Ticker
	stopChan      chan bool
	isRunning     uint32
	appMetrics    *AppMetrics
	outputFile    string      // File to export metrics to (when not empty)
	outputEnabled bool        // Whether to export metrics to file
	csvWriter     *csv.Writer // CSV writer for metrics export
	startTime     time.Time   // When the collector started
}

// AppMetrics holds application performance metrics
type AppMetrics struct {
	NumGoroutines int
	MemStats      runtime.MemStats
	CPUUsage      float64 // This is approximate since Go doesn't have direct CPU % measurement
}

// NewStatsCollector creates a new stats collector that collects stats at the specified interval
func NewStatsCollector(interval time.Duration) *StatsCollector {
	if interval <= 0 {
		interval = 60 * time.Second // Default to 60 seconds if invalid interval
	}

	return &StatsCollector{
		interval:      interval,
		stopChan:      make(chan bool),
		appMetrics:    &AppMetrics{},
		outputEnabled: false,
		startTime:     time.Now(),
	}
}

// WithFileOutput configures the stats collector to export metrics to a CSV file
func (s *StatsCollector) WithFileOutput(filePath string) *StatsCollector {
	s.outputFile = filePath
	s.outputEnabled = true
	return s
}

// initializeCSVWriter sets up the CSV file for metrics export
func (s *StatsCollector) initializeCSVWriter() error {
	if !s.outputEnabled || s.outputFile == "" {
		return nil
	}

	// Create or open the CSV file
	file, err := os.Create(s.outputFile)
	if err != nil {
		return fmt.Errorf("failed to create metrics file: %w", err)
	}

	// Create the CSV writer
	s.csvWriter = csv.NewWriter(file)

	// Write the header row
	header := []string{
		"Timestamp",
		"UptimeSeconds",
		"Goroutines",
		"HeapAlloc_MB",
		"TotalAlloc_MB",
		"Sys_MB",
		"NumGC",
	}

	return s.csvWriter.Write(header)
}

// Start begins collecting and logging stats at the configured interval
func (s *StatsCollector) Start() {
	if atomic.CompareAndSwapUint32(&s.isRunning, 0, 1) == false {
		logger.Sugar.Info("Stats collector is already running")
		return
	}

	// Initialize the CSV writer if file output is enabled
	if s.outputEnabled {
		if err := s.initializeCSVWriter(); err != nil {
			logger.Sugar.Warnf("Failed to initialize metrics export: %v", err)
			s.outputEnabled = false
		}
	}

	s.ticker = time.NewTicker(s.interval)
	s.startTime = time.Now()

	go func() {
		logger.Sugar.Infof("Stats collector started. Reporting every %v", s.interval)
		// Collect stats once immediately on start
		s.collectAndLogStats()

		for {
			select {
			case <-s.ticker.C:
				s.collectAndLogStats()
			case <-s.stopChan:
				s.ticker.Stop()
				atomic.StoreUint32(&s.isRunning, 0)
				logger.Sugar.Info("Stats collector stopped")
				return
			}
		}
	}()
}

// Stop halts the stats collection
func (s *StatsCollector) Stop() {
	if atomic.CompareAndSwapUint32(&s.isRunning, 1, 0) == false {
		return
	}

	s.ticker.Stop()
	s.stopChan <- true

	// Flush CSV writer if it exists
	if s.csvWriter != nil {
		s.csvWriter.Flush()
	}

	logger.Sugar.Info("Stats collector stopped")

}

// collectAndLogStats gathers current stats and logs them
func (s *StatsCollector) collectAndLogStats() {
	// Get number of goroutines
	s.appMetrics.NumGoroutines = runtime.NumGoroutine()

	// Get memory stats
	runtime.ReadMemStats(&s.appMetrics.MemStats)

	// Log the stats to console
	logger.Sugar.Infof("Stats | Goroutines: %d | Memory: Alloc=%v MiB, Sys=%v MiB, GC=%d",
		s.appMetrics.NumGoroutines,
		s.appMetrics.MemStats.Alloc/1024/1024,
		s.appMetrics.MemStats.Sys/1024/1024,
		s.appMetrics.MemStats.NumGC,
	)

	// Write metrics to CSV file if enabled
	if s.outputEnabled && s.csvWriter != nil {
		now := time.Now()
		uptime := now.Sub(s.startTime).Seconds()

		row := []string{
			now.Format(time.RFC3339),
			fmt.Sprintf("%.2f", uptime),
			fmt.Sprintf("%d", s.appMetrics.NumGoroutines),
			fmt.Sprintf("%.2f", float64(s.appMetrics.MemStats.Alloc)/1024/1024),
			fmt.Sprintf("%.2f", float64(s.appMetrics.MemStats.TotalAlloc)/1024/1024),
			fmt.Sprintf("%.2f", float64(s.appMetrics.MemStats.Sys)/1024/1024),
			fmt.Sprintf("%d", s.appMetrics.MemStats.NumGC),
		}

		err := s.csvWriter.Write(row)
		if err != nil {
			logger.Sugar.Warnf("Failed to write metrics to CSV: %v", err)
		}

		// Flush periodically to ensure data is written
		s.csvWriter.Flush()
	}
}

// GetMetrics returns the current application metrics
func (s *StatsCollector) GetMetrics() *AppMetrics {
	// Update metrics before returning
	s.appMetrics.NumGoroutines = runtime.NumGoroutine()
	runtime.ReadMemStats(&s.appMetrics.MemStats)
	return s.appMetrics
}
