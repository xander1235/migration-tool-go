package services

import (
	"migration-tool-go/dtos/common"
	"migration-tool-go/logger"
	"migration-tool-go/utils"
)

// statsService is a singleton service for collecting system statistics
type statsService struct {
	statsCollector *utils.StatsCollector
	config         *common.StatsConfiguration
	isRunning      bool
}

// StatsService is the global stats service instance
var StatsService *statsService

// NewStatsService initializes the stats service with the given configuration
func NewStatsService(config common.StatsConfiguration) {
	// Set default values if not provided
	if config.IntervalSeconds <= 0 {
		config.IntervalSeconds = 30 // Default interval: 30 seconds
	}

	// Create the service instance
	StatsService = &statsService{
		config: &config,
	}

	logger.Sugar.Infof("Stats service initialized with enabled=%v, interval=%ds, output=%s",
		config.Enabled,
		config.IntervalSeconds,
		getOutputFileDisplay(config.OutputFile))
}

// Start begins collecting statistics if enabled in configuration
func (s *statsService) Start() {
	if s.isRunning {
		logger.Sugar.Info("Stats service is already running")
		return
	}

	if !s.config.Enabled {
		logger.Sugar.Info("Stats collection is disabled, skipping")
		return
	}

	// Initialize the stats collector
	interval := s.config.GetInterval()
	s.statsCollector = utils.NewStatsCollector(interval)

	// Configure output file if specified
	if s.config.OutputFile != "" {
		s.statsCollector.WithFileOutput(s.config.OutputFile)
	}

	// Start collecting stats
	logger.Sugar.Infof("Starting stats collection with interval: %v", interval)
	s.statsCollector.Start()
	s.isRunning = true
}

// Stop halts the stats collection
func (s *statsService) Stop() {
	if !s.isRunning || s.statsCollector == nil {
		return
	}

	logger.Sugar.Info("Stopping stats collection")
	s.statsCollector.Stop()
	s.isRunning = false
}

// helper function to format output file path for display
func getOutputFileDisplay(path string) string {
	if path == "" {
		return "none (console only)"
	}
	return path
}
