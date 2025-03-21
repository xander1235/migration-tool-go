package dtos

import "database/sql"

type ColumnInfo struct {
	Schema            string        `json:"schema"`
	Table             string        `json:"table"`
	Name              string        `json:"name"`
	DataType          string        `json:"data_type"`
	Ordinal           int           `json:"ordinal"`
	Precision         sql.NullInt64 `json:"precision"`
	Scale             sql.NullInt64 `json:"scale"`
	DatetimePrecision sql.NullInt64 `json:"datetime_precision"`
	IsPrimaryKey      bool          `json:"is_primary_key"`
}
