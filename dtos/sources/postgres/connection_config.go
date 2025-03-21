package postgres

import (
	"fmt"
	"migration-tool-go/dtos/common"
)

// PostgresConnectionConfig implements the common.ConnectionConfig interface for PostgreSQL
type PostgresConnectionConfig struct {
	common.BaseConnectionConfig

	// PostgreSQL specific configuration fields
	SSLMode         string `json:"ssl_mode"`
	ApplicationName string `json:"application_name"`
}

// Validate checks if the PostgreSQL connection configuration is valid
func (c *PostgresConnectionConfig) Validate() error {
	// First validate base configuration
	if err := common.ValidateBaseConfig(&c.BaseConnectionConfig); err != nil {
		return err
	}

	// Validate PostgreSQL specific fields
	if c.SSLMode == "" {
		return fmt.Errorf("ssl_mode is required")
	}

	return nil
}

// SetDefaults sets default values for optional fields in the PostgreSQL configuration
func (c *PostgresConnectionConfig) SetDefaults() {
	// Set defaults for base configuration
	common.SetBaseDefaults(&c.BaseConnectionConfig)

	// Set PostgreSQL specific defaults
	if c.SSLMode == "" {
		c.SSLMode = "prefer" // Default to prefer SSL
	}

	if c.ApplicationName == "" {
		c.ApplicationName = "pg_migration-tool-go"
	}
}

// CreateConnectionConfigFromDetails creates a PostgresConnectionConfig from the standard ConnectionDetails
func CreateConnectionConfigFromDetails(details ConnectionDetails, poolSize uint) PostgresConnectionConfig {
	return PostgresConnectionConfig{
		BaseConnectionConfig: common.BaseConnectionConfig{
			Username:       details.Username,
			Password:       details.Password,
			Host:           details.Host,
			Port:           details.Port,
			Database:       details.Database,
			MaxConnections: int(poolSize),
		},
		SSLMode: "prefer", // Default value
	}
}

// ValidateConfiguration validates the PostgreSQL configuration
func ValidateConfiguration(config Configuration) error {
	if len(config.Schemas) == 0 {
		return fmt.Errorf("at least one schema must be specified")
	}

	if config.Pool <= 0 {
		return fmt.Errorf("pool size must be greater than 0")
	}

	return nil
}
