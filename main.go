package main

import (
	"context"
	"flag"
	"migration-tool-go/config"
	"migration-tool-go/logger"
	"migration-tool-go/services"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	// Initialize the logger with optional file output
	logger.Initialize(logger.Config{
		LogToFile:   true,
		LogFilePath: "logs/migration.log",
		LogLevel:    "info",
	})
	// Ensure logs are flushed on exit
	defer logger.Sync()
	startTime := time.Now()

	// Define flags
	configPath := flag.String("config_path", "config/config.json", "Path of the config json")

	// Parse the flags
	flag.Parse()

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Sugar.Infof("Received signal %v, initiating graceful shutdown", sig)
		cancel()
		
		// Add a timeout for graceful shutdown
		gracefulTimeout := 10 * time.Second
		logger.Sugar.Infof("Waiting up to %s for graceful shutdown...", gracefulTimeout)
		
		// Create a timeout channel
		timeoutChan := time.After(gracefulTimeout)
		
		// Wait for either the context to be done or the timeout
		select {
		case <-ctx.Done():
			logger.Sugar.Info("Graceful shutdown completed")
		case <-timeoutChan:
			logger.Sugar.Warn("Graceful shutdown timed out, forcing exit")
			os.Exit(1)
		}
	}()

	// Initialize configuration
	logger.Sugar.Infow("Initializing configuration from", "configPath", *configPath)
	config.InitializeConfig(*configPath)

	// Initialize stats service from config
	logger.Sugar.Info("Initializing stats service")
	services.NewStatsService(config.StatsConfig)

	// Start stats service (will only collect if enabled in config)
	services.StatsService.Start()

	// Ensure stats service is stopped when the application exits
	defer services.StatsService.Stop()

	// Initialize services
	logger.Sugar.Info("Initializing PostgreSQL migration service")
	services.NewPostgresMigration(config.SourceConfig, config.WorkerConfig)

	// Initialize the appropriate destination connectors based on the destination type
	switch config.DestinationConfig.Type {
	case "doris":
		logger.Sugar.Info("Initializing Doris connector")
		services.NewDorisConnector(config.DestinationConfig)
	case "kafka":
		logger.Sugar.Info("Initializing Kafka connector")
		services.NewKafkaConnector(config.DestinationConfig)
	default:
		logger.Sugar.Fatalf("Unsupported destination type: %s", config.DestinationConfig.Type)
	}

	logger.Sugar.Info("Initializing migration runner")
	services.NewMigrationRunner(config.WorkerConfig)

	logger.Sugar.Info("Starting migration process")
	if err := services.MigrationRunner.Run(ctx); err != nil {
		logger.Sugar.Errorf("Migration failed: %v", err)
		os.Exit(1)
	}

	// Explicitly close all connectors to ensure proper shutdown
	logger.Sugar.Info("Shutting down services...")
	
	// Close Kafka connector if it exists
	if services.KafkaConnector != nil {
		if err := services.KafkaConnector.Close(); err != nil {
			logger.Sugar.Errorf("Error closing Kafka connector: %v", err)
		}
	}
	
	// Close Doris connector if it exists
	if services.DorisConnector != nil {
		if err := services.DorisConnector.Close(); err != nil {
			logger.Sugar.Errorf("Error closing Doris connector: %v", err)
		}
	}
	
	// Force exit after a short delay if the application doesn't exit naturally
	go func() {
		logger.Sugar.Info("Waiting 3 seconds for graceful shutdown before forcing exit...")
		time.Sleep(3 * time.Second)
		logger.Sugar.Warn("Forcing application exit")
		os.Exit(0)
	}()

	// Report completion
	logger.Sugar.Infof("Migration completed successfully in %s", time.Since(startTime))
}
