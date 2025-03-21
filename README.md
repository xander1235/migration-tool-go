# Migration Tool

A Go-based tool for migrating data between different database systems. Currently supports PostgreSQL to Apache Doris migrations with configurable worker settings, batch processing, and performance monitoring.

## Overview

This tool is designed to efficiently migrate data from a PostgreSQL database to Apache Doris in a scalable manner. It features configurable workers, batch sizes, and performance monitoring to optimize migration performance. The tool uses `pgx` for PostgreSQL connectivity and HTTP Stream Load for Apache Doris integration.

### Architecture

The migration tool follows a modular, service-based architecture with the following key components:

1. **Migration Runner**: Orchestrates the entire migration process, managing workers and coordinating data flow
2. **PostgreSQL Migration Service**: Handles extracting data from PostgreSQL database tables
3. **Doris Sync Service**: Manages sending data to Apache Doris using the Stream Load protocol
4. **Stats Service**: Collects and reports performance metrics during migration
5. **Logger**: Provides structured logging capabilities using Zap logger

### Data Flow

1. The migration begins by identifying tables to migrate based on configuration
2. For each table, the PostgreSQL service extracts primary keys in batches
3. For each batch of primary keys, corresponding records are fetched
4. Records are transformed into JSON format compatible with Doris
5. Data is streamed to Doris using the Stream Load protocol
6. Metrics are collected throughout the process for monitoring performance

## Features

* High-performance data migration between PostgreSQL and Apache Doris
* Multi-worker architecture with configurable parallelism
* Configurable batch processing for optimal throughput
* Schema and table filtering capabilities
* Performance monitoring and statistics collection
* Error handling with retry logic
* Progress tracking and reporting
* Structured logging with Zap logger for improved observability
* Log file output with configurable log levels

## Requirements

* Go 1.17 or later
* PostgreSQL 11 or later
* Apache Doris 2.8 or later
* pgx library
* go.uber.org/zap for logging

## Usage

1. Clone the repository: `git clone git@github.com:pixisai/migration-tool-go.git`
2. Build the tool: `go build`
3. Run the tool: `./migration-tool-go -config_path config/config.json`

## Configuration

The tool uses a JSON configuration file with the following sections:

### Source Configuration (PostgreSQL)

```json
"source": {
  "type": "postgres",
  "value": {
    "connection_details": {
      "host": "your-postgres-host",
      "port": "5432",
      "username": "your-username",
      "password": "your-password",
      "database": "your-database"
    },
    "configuration": {
      "schemas": ["schema1", "schema2"],
      "excluded_schemas": ["public"],
      "exclude_table_regex_list": [
        {
          "schema": "schema1",
          "regex": ["\\bpattern_\\w*"]
        }
      ],
      "exclude_tables_list": [
        {
          "schema": "schema1",
          "tables": ["table1", "table2"]
        }
      ],
      "pool": 20
    }
  }
}
```

### Destination Configuration (Apache Doris)

```json
"destination": {
  "type": "doris",
  "value": {
    "connection_details": {
      "fe_nodes": "your-fe-nodes",
      "fe_port": 8030,
      "be_nodes": "your-be-nodes",
      "be_port": 8040,
      "username": "root",
      "password": "your-password",
      "database": "your-database"
    },
    "configuration": {
      "pool": 20
    }
  }
}
```

### Worker Configuration

```json
"worker_configuration": {
  "no_of_workers": 20,            // Number of worker goroutines
  "worker_batch_size": 10000,      // Size of the worker batch for processing
  "id_batch_size": 100000,         // Number of IDs to fetch in a batch
  "record_batch_size": 5000,       // Number of records to process in a batch
  "batch_processing_timeout_ms": 500, // Timeout for batch processing
  "concurrent_tables": 1           // Number of tables to process concurrently
}
```

### Statistics Collection

```json
"stats_configuration": {
  "enabled": true,                // Enable/disable stats collection
  "interval_seconds": 30,         // Collection interval in seconds
  "output_file": "stats.csv"      // Optional output file (leave empty for console output)
}
```

### Tracking Configuration

```json
"tracking_configuration": {
  "progress_ticker": "30 secs"    // Frequency of progress updates
}
```

## Logging

The tool uses Uber's Zap logger for structured, high-performance logging. The logger is initialized in `main.go` with the following configuration:

```go
logger.Initialize(logger.Config{
    LogToFile:   true,
    LogFilePath: "logs/migration.log",
    LogLevel:    "info",
})
```

You can configure:

* **LogToFile**: Enable/disable logging to a file (default: false, console only)
* **LogFilePath**: Specify the path for the log file (default: "logs/app.log")
* **LogLevel**: Set the log level ("debug", "info", "warn", "error", "fatal")

Log messages are structured with timestamps, log levels, and caller information, making it easier to trace and debug issues.

## Performance Tuning

For optimal performance, consider adjusting the following parameters:

1. **Worker Count**: Increase `no_of_workers` for more parallelism (limited by CPU)
2. **Batch Sizes**: Tune the various batch sizes based on your data characteristics:
   - `id_batch_size`: Controls how many primary key IDs are fetched at once
   - `record_batch_size`: Controls how many records are processed in a batch
   - `worker_batch_size`: Controls the size of the worker pool
3. **Timeout**: Adjust `batch_processing_timeout_ms` to balance between latency and throughput
4. **Concurrent Tables**: Increase `concurrent_tables` to process multiple tables in parallel

## Contributing

Contributions are welcome! Please submit a pull request with your changes.

## License

MIT License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Project Structure

```
├── config/                # Configuration files and loading logic
├── dtos/                  # Data transfer objects for services
├── logger/                # Zap logger implementation
├── services/              # Core service implementations
│   ├── doris_sync.go      # Handles syncing data to Doris
│   ├── migration_runner.go # Orchestrates the migration process
│   ├── postgres_migration.go # Extracts data from PostgreSQL
│   └── stats_service.go   # Collects and reports metrics
├── utils/                 # Utility functions and helpers
│   ├── stats_collector.go # System metrics collection
│   └── utils.go           # Common utility functions
└── main.go                # Application entry point
```

## Error Handling

The migration tool implements robust error handling strategies:

1. **Failed Records Tracking**: Records that fail to migrate are tracked and saved to JSON files for later analysis or retry
2. **Graceful Shutdown**: Handles system signals (SIGINT, SIGTERM) to ensure clean shutdown
3. **Timeout Handling**: Configurable timeouts prevent operations from blocking indefinitely
4. **Structured Error Logging**: All errors are logged with context information for easier troubleshooting

## Statistics and Monitoring

The tool provides comprehensive statistics and monitoring capabilities:

1. **System Metrics**: Tracks goroutine count, memory usage, and GC statistics
2. **Migration Metrics**: Records counts of processed records, processing times, and failure rates
3. **CSV Output**: Can output metrics to CSV files for further analysis
4. **Progress Reporting**: Real-time progress updates during migration

## Future Enhancements

- Support for additional source and destination database systems
- Schema migration capabilities
- Web-based monitoring dashboard
- Incremental migration support
- Data validation and verification tools