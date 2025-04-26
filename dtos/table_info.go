package dtos

import "migration-tool-go/dtos/sources/postgres"

type TableInfo struct {
	TableSchema   string              `json:"table_schema"`
	TableName     string              `json:"table_name"`
	Columns       []ColumnInfo        `json:"columns"`
	PrimaryKeys   []PrimaryKey        `json:"primary_keys"`
	QueryStrategy *postgres.QueryStrategy `json:"query_strategy,omitempty"`
}

type PrimaryKey struct {
	ColumnName string `json:"column_name"`
	DataType   string `json:"data_type"`
}
