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

	logger.Sugar.Info("Initializing Doris sync service")
	services.NewDorisSync(config.DestinationConfig)

	logger.Sugar.Info("Initializing migration runner")
	services.NewMigrationRunner(config.WorkerConfig)

	logger.Sugar.Info("Starting migration process")
	if err := services.MigrationRunner.Run(ctx); err != nil {
		logger.Sugar.Errorf("Migration failed: %v", err)
		os.Exit(1)
	}

	// Report completion
	logger.Sugar.Infof("Migration completed successfully in %s", time.Since(startTime))
}
