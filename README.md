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
      "include_tables_list": [
        {
          "schema": "schema1",
          "tables": ["table1", "table2"],
          "query_strategy": {
            "type": "batch_size",
            "column": "id",
            "batch_size_params": {
              "batch_size": 10000,
              "start_id": 1
            }
          }
        }
      ],
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

#### Source Configuration Fields

**Connection Details:**
- `host`: The hostname or IP address of the PostgreSQL server
- `port`: The port number PostgreSQL is running on (default: 5432)
- `username`: Username for authentication
- `password`: Password for authentication
- `database`: Name of the database to connect to

**Configuration Options:**
- `schemas`: List of schemas to include in the migration (mutually exclusive with `include_tables_list`)
- `excluded_schemas`: List of schemas to exclude from the migration
- `include_tables_list`: Explicitly specify which tables to include in the migration
  - `schema`: The schema containing the tables
  - `tables`: List of table names to include
  - `query_strategy`: Configuration for how to query the data (see Query Strategies section below)
- `exclude_table_regex_list`: Exclude tables matching the specified regex patterns
  - `schema`: The schema containing the tables
  - `regex`: List of regex patterns to match against table names
- `exclude_tables_list`: Explicitly exclude specific tables
  - `schema`: The schema containing the tables
  - `tables`: List of table names to exclude
- `pool`: Maximum number of database connections in the connection pool

#### Query Strategies

The migration tool supports multiple query strategies to efficiently extract data from PostgreSQL tables. Each strategy is designed for different data distribution patterns and query requirements.

##### 1. Batch Size Strategy

**Type:** `batch_size`

This strategy fetches data in fixed-size batches using OFFSET/LIMIT. It's simple but can be inefficient for large tables with many rows.

**Mandatory Fields:**
- `type`: Must be `"batch_size"`
- `column`: The column to order by (typically the primary key)
- `batch_size_params.batch_size`: Number of rows to fetch in each batch

**Optional Fields:**
- `batch_size_params.order_by`: Column to order by (defaults to the primary key)

**Example:**
```json
"query_strategy": {
  "type": "batch_size",
  "column": "id",
  "batch_size_params": {
    "batch_size": 10000,
    "order_by": "created_at"
  }
}
```

##### 2. Range Strategy

**Type:** `range`

This strategy divides the data into ranges based on a numeric or date column. It's efficient for evenly distributed data.

**Mandatory Fields:**
- `type`: Must be `"range"`
- `column`: The column to base the ranges on
- `range_params.frequency`: Step size for the range (e.g., `1000` for IDs or `'1 day'::interval` for timestamps)

**Optional Fields:**
- `range_params.min`: Minimum value to start from (default: MIN(column))
- `range_params.max`: Maximum value to end at (default: MAX(column))

**Example:**
```json
"query_strategy": {
  "type": "range",
  "column": "created_at",
  "range_params": {
    "frequency": "1 day",
    "min": "2023-01-01T00:00:00",
    "max": "2023-12-31T23:59:59"
  }
}
```

##### 3. Fixed Values Strategy

**Type:** `fixed_values`

This strategy fetches data for specific values of a column. Useful for partitioning data by a discrete set of values.

**Mandatory Fields:**
- `type`: Must be `"fixed_values"`
- `column`: The column to filter on
- `fixed_values_params.values`: Array of values to fetch data for
- `fixed_values_params.batch_size`: Number of rows to fetch per batch

**Example:**
```json
"query_strategy": {
  "type": "fixed_values",
  "column": "status",
  "fixed_values_params": {
    "values": ["active", "pending", "completed"],
    "batch_size": 5000
  }
}
```

##### 4. Time Window Strategy

**Type:** `time_window`

This strategy is specifically designed for time-series data, dividing the data into fixed time windows.

**Mandatory Fields:**
- `type`: Must be `"time_window"`
- `column`: The timestamp column to base the windows on
- `time_window_params.window_size`: Size of each time window (e.g., "1h", "1d", "1w", "1m")

**Optional Fields:**
- `time_window_params.start_time`: Start time for the first window (ISO 8601 format)
- `time_window_params.end_time`: End time for the last window (ISO 8601 format)

**Example:**
```json
"query_strategy": {
  "type": "time_window",
  "column": "event_time",
  "time_window_params": {
    "window_size": "1 hour",
    "start_time": "2023-01-01T00:00:00Z",
    "end_time": "2023-01-02T00:00:00Z"
  }
}
```

#### Choosing the Right Strategy

1. **Batch Size**: Simple but can be inefficient for large tables. Best for small to medium tables.
2. **Range**: Good for numeric or date columns with even distribution. Efficient for large tables.
3. **Fixed Values**: Best when you need to process specific values of a column.
4. **Time Window**: Optimized for time-series data with timestamps.

#### Notes

- The `column` field is required for all strategies and should be indexed for best performance.
- For time-based strategies, ensure the column is of a timestamp or date type.
- The migration tool will automatically determine the min/max values if not specified.
- For resuming failed migrations, the tool tracks progress and can continue from where it left off.

### Destination Configuration

#### Apache Doris

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

##### Doris Configuration Fields

**Connection Details:**
- `fe_nodes`: Comma-separated list of Frontend (FE) node addresses
- `fe_port`: Port for the FE HTTP server (default: 8030)
- `be_nodes`: Comma-separated list of Backend (BE) node addresses
- `be_port`: Port for the BE HTTP server (default: 8040)
- `username`: Username for Doris authentication
- `password`: Password for Doris authentication
- `database`: Name of the target database in Doris

**Configuration Options:**
- `pool`: Maximum number of HTTP clients to use for parallel loading

#### Apache Kafka

```json
"destination": {
  "type": "kafka",
  "value": {
    "connection_details": {
      "brokers": "kafka1:9092,kafka2:9092",
      "username": "your-username",
      "password": "your-password",
      "use_sasl": true,
      "use_tls": true,
      "sasl_mechanism": "PLAIN",
      "tls_skip_verify": false,
      "client_cert_file": "/path/to/client.crt",
      "client_key_file": "/path/to/client.key",
      "ca_cert_file": "/path/to/ca.crt",
      "warpstream_enabled": false
    },
    "configuration": {
      "pool": 5,
      "pool_size": 5,
      "topic": "your-topic",
      "batch_size": 1000,
      "max_open_requests": 5,
      "channel_buffer_size": 256,
      "flush_bytes": 0,
      "flush_messages": 0,
      "flush_frequency_ms": 500,
      "max_message_bytes": 1000000,
      "compression_enabled": true,
      "compression_type": "gzip",
      "retry_max": 5,
      "required_acks": "WaitForLocal"
    }
  }
}
```

##### Kafka Configuration Fields

**Connection Details:**
- `brokers`: Comma-separated list of Kafka broker addresses (e.g., "kafka1:9092,kafka2:9092")
- `username`: Username for SASL/PLAIN authentication
- `password`: Password for SASL/PLAIN authentication
- `use_sasl`: Enable SASL authentication (default: false)
- `use_tls`: Enable TLS encryption (default: false)
- `sasl_mechanism`: SASL mechanism to use (e.g., "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
- `tls_skip_verify`: Skip TLS certificate verification (not recommended for production)
- `client_cert_file`: Path to client certificate file for mutual TLS
- `client_key_file`: Path to client private key file for mutual TLS
- `ca_cert_file`: Path to CA certificate file
- `warpstream_enabled`: Enable WarpStream compatibility mode (default: false)

**Configuration Options:**
- `pool`: Number of Kafka producer instances in the pool (legacy, use pool_size)
- `pool_size`: Number of Kafka producer instances in the pool (recommended)
- `topic`: Default topic to produce messages to (can be overridden per sync operation)
- `batch_size`: Number of messages to batch together before sending (default: 1000)
- `max_open_requests`: Maximum number of unacknowledged requests the client will send before blocking (default: 5)
- `channel_buffer_size`: Size of the internal message queue (default: 256)
- `flush_bytes`: Best-effort number of bytes needed to trigger a flush (0 = disabled)
- `flush_messages`: Best-effort number of messages needed to trigger a flush (0 = disabled)
- `flush_frequency_ms`: Best-effort frequency of flushes (default: 500ms)
- `max_message_bytes`: Maximum permitted size of a message (default: 1,000,000 bytes)
- `compression_enabled`: Enable message compression (default: false)
- `compression_type`: Compression codec to use ("none", "gzip", "snappy", "lz4", "zstd")
- `retry_max`: Maximum number of retries for a failing request (default: 5)
- `required_acks`: Number of acknowledgements required ("NoResponse", "WaitForLocal", "WaitForAll")

### Configuration Notes

### Configuration Notes

1. **Table Selection**: You can either specify tables to include using `include_tables_list` or specify schemas to include using `schemas`. These are mutually exclusive - if `include_tables_list` is provided, `schemas` will be ignored.

2. **Batching Strategy**: The `query_strategy` in the source configuration allows you to control how data is read from PostgreSQL. The `batch_size` strategy is recommended for large tables as it processes data in smaller chunks to reduce memory usage.

3. **Connection Pooling**: Both source and destination configurations support connection pooling. Adjust the `pool` values based on your server's capacity and the desired level of parallelism.

4. **Exclusion Rules**: You can exclude tables using either regex patterns (`exclude_table_regex_list`) or explicit table lists (`exclude_tables_list`). These exclusions are applied after the initial table selection based on `schemas` or `include_tables_list`.

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