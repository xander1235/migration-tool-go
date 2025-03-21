package common

import (
	"context"
	"errors"
)

// ConnectionConfig defines the interface that all connection configurations must implement
type ConnectionConfig interface {
	// Validate checks if the configuration is valid
	Validate() error
	
	// SetDefaults sets default values for optional fields
	SetDefaults()
}

// BaseConnectionConfig contains common configuration fields for database connections
type BaseConnectionConfig struct {
	// Required fields - these must be provided in config.json
	Username string `json:"username"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	Database string `json:"database"`
	
	// Optional fields with defaults
	MaxConnections    int    `json:"max_connections"`
	MinConnections    int    `json:"min_connections"`
	MaxConnLifetime   string `json:"max_conn_lifetime"`
	MaxConnIdleTime   string `json:"max_conn_idle_time"`
	HealthCheckPeriod string `json:"health_check_period"`
	ConnectTimeout    string `json:"connect_timeout"`
	StatementTimeout  string `json:"statement_timeout"`
	StatsInterval     string `json:"stats_interval"`
	EnableMetrics     bool   `json:"enable_metrics"`
	EnableHealthChecks bool  `json:"enable_health_checks"`
}

// ValidateBaseConfig validates the base configuration fields that are common to all connections
func ValidateBaseConfig(config *BaseConnectionConfig) error {
	if config.Username == "" {
		return errors.New("username is required")
	}
	if config.Password == "" {
		return errors.New("password is required")
	}
	if config.Host == "" {
		return errors.New("host is required")
	}
	if config.Port == "" {
		return errors.New("port is required")
	}
	if config.Database == "" {
		return errors.New("database is required")
	}
	return nil
}

// SetBaseDefaults sets default values for the optional fields in BaseConnectionConfig
func SetBaseDefaults(config *BaseConnectionConfig) {
	if config.MaxConnections <= 0 {
		config.MaxConnections = 20
	}
	if config.MinConnections <= 0 {
		config.MinConnections = config.MaxConnections / 2
	}
	if config.MaxConnLifetime == "" {
		config.MaxConnLifetime = "1h"
	}
	if config.MaxConnIdleTime == "" {
		config.MaxConnIdleTime = "30m"
	}
	if config.HealthCheckPeriod == "" {
		config.HealthCheckPeriod = "1m"
	}
	if config.ConnectTimeout == "" {
		config.ConnectTimeout = "10s"
	}
	if config.StatementTimeout == "" {
		config.StatementTimeout = "5m"
	}
	if config.StatsInterval == "" {
		config.StatsInterval = "1m"
	}
}

// Connection defines the interface that all database connections must implement
type Connection interface {
	// Connect establishes the database connection
	Connect(ctx context.Context) error
	
	// Close closes the database connection
	Close()
	
	// IsHealthy checks if the connection is healthy
	IsHealthy(ctx context.Context) bool
}
