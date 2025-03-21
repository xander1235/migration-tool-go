package config

import (
	"context"
	"fmt"
	"log"
	"migration-tool-go/dtos/sources/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	Db *pgxpool.Pool
)

// PostgresConnection implements the Connection interface for PostgreSQL
type PostgresConnection struct {
	// Using direct connection details instead of a complex config struct
	connectionDetails postgres.ConnectionDetails
	configuration     postgres.Configuration
	pool              *pgxpool.Pool
	numWorkers        int32
}

// NewPostgresConnection creates a new PostgreSQL connection with the given connection details and configuration
func NewPostgresConnection(connectionDetails postgres.ConnectionDetails, configuration postgres.Configuration, numWorkers int32) (*PostgresConnection, error) {
	// Validate connection details
	if connectionDetails.Host == "" || connectionDetails.Port == "" || connectionDetails.Database == "" {
		return nil, fmt.Errorf("invalid connection details: host, port, and database are required")
	}

	// Validate configuration
	if len(configuration.Schemas) == 0 {
		return nil, fmt.Errorf("invalid configuration: at least one schema must be specified")
	}

	// Validate pool size
	if configuration.Pool <= 0 {
		return nil, fmt.Errorf("invalid configuration: pool size must be greater than 0")
	}

	// Return a new PostgreSQL connection (not yet connected)
	return &PostgresConnection{
		connectionDetails: connectionDetails,
		configuration:     configuration,
		numWorkers:        numWorkers,
	}, nil
}

// Connect establishes the database connection
func (p *PostgresConnection) Connect(ctx context.Context) error {
	// Always use 'prefer' as the default SSL mode since the ConnectionDetails doesn't have an SSLMode field
	const sslMode = "prefer"

	// Build connection string
	postgresDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		p.connectionDetails.Username,
		p.connectionDetails.Password,
		p.connectionDetails.Host,
		p.connectionDetails.Port,
		p.connectionDetails.Database,
		sslMode,
	)

	poolConfig, err := pgxpool.ParseConfig(postgresDSN)
	if err != nil {
		return fmt.Errorf("failed to parse PostgreSQL DSN: %w", err)
	}

	// Set pool configuration
	poolConfig.MaxConns = p.numWorkers
	poolConfig.MinConns = p.numWorkers / 2

	// Application name for connection identification (optional)
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "pg_migration-tool-go"

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	p.pool = pool
	Db = pool // Set global for backward compatibility - remove this once all code uses the new approach

	return nil
}

// Close closes the database connection
func (p *PostgresConnection) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

// IsHealthy checks if the connection is healthy
func (p *PostgresConnection) IsHealthy(ctx context.Context) bool {
	if p.pool == nil {
		return false
	}
	return p.pool.Ping(ctx) == nil
}

// GetPool returns the underlying connection pool
func (p *PostgresConnection) GetPool() *pgxpool.Pool {
	return p.pool
}

// NewConnection creates a new connection from the postgres configuration (legacy method)
func NewConnection(postgres postgres.Postgres, numWorkers int) *pgxpool.Pool {
	// Use connection details directly
	connectionDetails := postgres.ConnectionDetails
	configuration := postgres.Configuration

	// Create connection
	conn, err := NewPostgresConnection(connectionDetails, configuration, int32(numWorkers))
	if err != nil {
		log.Fatalf("Failed to create PostgreSQL connection: %v", err)
	}

	// Connect to database
	if err := conn.Connect(context.Background()); err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	return conn.GetPool()
}
