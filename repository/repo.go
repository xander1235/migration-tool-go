package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"migration-tool-go/config"
	"migration-tool-go/dtos"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
)

type Repo struct {
	db *pgxpool.Pool
}

func NewRepo(db *pgxpool.Pool) *Repo {
	return &Repo{
		db: db,
	}
}

func (r Repo) GetTableInfo(ctx context.Context, schemas []any) ([]dtos.TableInfo, error) {
	query := `
		SELECT 
			c.table_schema AS schema, 
			c.table_name AS table, 
			c.column_name AS column, 
			c.udt_name AS data_type, 
			c.ordinal_position, 
			c.numeric_precision AS precision, 
			c.numeric_scale AS scale, 
			c.datetime_precision,
			CASE 
				WHEN kcu.column_name IS NOT NULL THEN TRUE 
				ELSE FALSE 
			END AS is_primary_key
		FROM information_schema.columns c
		LEFT JOIN information_schema.key_column_usage kcu 
			ON c.table_name = kcu.table_name 
			AND c.table_schema = kcu.table_schema 
			AND c.column_name = kcu.column_name
			AND kcu.constraint_name IN (
				SELECT constraint_name 
				FROM information_schema.table_constraints 
				WHERE table_name = c.table_name 
				AND table_schema = c.table_schema 
				AND constraint_type = 'PRIMARY KEY'
			)
		WHERE c.table_schema in (%s) -- ✅ Ensures only the selected schema
		ORDER BY c.table_name, c.ordinal_position;
        `

	var schemaPlaceHolder []string
	for i := range len(schemas) {
		schemaPlaceHolder = append(schemaPlaceHolder, fmt.Sprintf("$%d", i+1))
	}

	query = fmt.Sprintf(query, strings.Join(schemaPlaceHolder, ", "))

	//schemaStr := strings.Join(lo.Map(schemas, func(item string, index int) string { return fmt.Sprintf("'%s'", item) }), ", ")
	rows, err := r.db.Query(ctx, query, schemas...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch column metadata: %v", err)
	}
	defer rows.Close()

	var tableMap = make(map[string]map[string]*dtos.TableInfo)

	for rows.Next() {
		var col dtos.ColumnInfo
		if err := rows.Scan(
			&col.Schema, &col.Table, &col.Name, &col.DataType,
			&col.Ordinal, &col.Precision, &col.Scale, &col.DatetimePrecision, &col.IsPrimaryKey,
		); err != nil {
			return nil, err
		}

		if tableMap[col.Schema] == nil {
			tableMap[col.Schema] = make(map[string]*dtos.TableInfo)
			tableMap[col.Schema][col.Table] = &dtos.TableInfo{
				TableSchema: col.Schema,
				TableName:   col.Table,
			}
		} else if tableMap[col.Schema][col.Table] == nil {
			tableMap[col.Schema][col.Table] = &dtos.TableInfo{
				TableSchema: col.Schema,
				TableName:   col.Table,
			}
		}

		tableMap[col.Schema][col.Table].Columns = append(tableMap[col.Schema][col.Table].Columns, col)
		if col.IsPrimaryKey {
			tableMap[col.Schema][col.Table].PrimaryKeys = append(tableMap[col.Schema][col.Table].PrimaryKeys, dtos.PrimaryKey{
				ColumnName: col.Name,
				DataType:   col.DataType,
			})
		}
	}

	var tableInfoList []dtos.TableInfo

	for _, v := range tableMap {
		for _, v2 := range v {
			if v2.TableName == "platform" {
				tableInfoList = append(tableInfoList, *v2)
			}
		}
	}

	return tableInfoList, nil
}

func (r Repo) FetchBatchMultiPrimaryKeys(ctx context.Context, lastIds map[string]any, includeLastId bool, columnMeta []dtos.ColumnInfo, tableSchema string, tableName string, primaryKeys []dtos.PrimaryKey, idBatchSize int) ([]map[string]any, error) {
	columnMetaMap := lo.SliceToMap(columnMeta, func(item dtos.ColumnInfo) (string, dtos.ColumnInfo) {
		return item.Name, item
	})

	// Build the SELECT clause
	var selectColumns []string
	for _, pk := range primaryKeys {
		selectColumns = append(selectColumns, pk.ColumnName)
	}
	selectClause := strings.Join(selectColumns, ", ")

	// Build the parameterized WHERE clause
	var whereConditions []string
	var params []any // Changed back to []any for db.Query compatibility
	paramCount := 1

	// For the first comparison
	var firstCompare string
	if includeLastId {
		firstCompare = ">="
	} else {
		firstCompare = ">"
	}

	// Build row comparison for all primary keys
	var rowComps []string
	for _, pk := range primaryKeys {
		// Convert values based on data type
		var paramVal any
		switch pk.DataType {
		case "int", "bigint":
			// Keep numeric types as numbers for better query optimization
			paramVal = lastIds[pk.ColumnName]
		case "double precision":
			paramVal = lastIds[pk.ColumnName]
		default: // uuid, varchar, timestamp, date, time, json
			// Convert to string for text-based types
			paramVal = fmt.Sprintf("%v", lastIds[pk.ColumnName])
		}
		params = append(params, paramVal)
		rowComps = append(rowComps, fmt.Sprintf("$%d", paramCount))
		paramCount++
	}

	// Main row comparison
	whereConditions = append(whereConditions,
		fmt.Sprintf("(%s) %s (%s)",
			strings.Join(selectColumns, ", "),
			firstCompare,
			strings.Join(rowComps, ", ")))

	// Add batch size parameter
	params = append(params, idBatchSize) // Keep as int for better query planning

	// Construct the final query with proper ordering
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s WHERE %s ORDER BY %s LIMIT $%d",
		selectClause,
		tableSchema,
		tableName,
		strings.Join(whereConditions, " AND "),
		selectClause,
		paramCount,
	)

	// Execute the query with proper parameter binding
	rows, err := r.db.Query(ctx, query, params...)
	if err != nil {
		log.Printf("Failed to fetch primary key batch: %v", err)
		return nil, fmt.Errorf("failed to fetch primary key batch: %w", err)
	}

	return deserializeRecords(rows, columnMetaMap, selectColumns), nil
}

func (r Repo) FetchBatchPrimaryKeys(ctx context.Context, lastId any, includeLastId bool, tableSchema string, tableName string, primaryKey string, idBatchSize int) ([]any, error) {
	// Build the query with proper parameter binding
	var query string
	if includeLastId {
		query = fmt.Sprintf(
			"SELECT %s FROM %s.%s WHERE %s >= $1 ORDER BY %s LIMIT $2",
			primaryKey, tableSchema, tableName, primaryKey, primaryKey,
		)
	} else {
		query = fmt.Sprintf(
			"SELECT %s FROM %s.%s WHERE %s > $1 ORDER BY %s LIMIT $2",
			primaryKey, tableSchema, tableName, primaryKey, primaryKey,
		)
	}

	// Add query hints for better performance with indexes
	if strings.Contains(strings.ToLower(primaryKey), "uuid") {
		// For UUID columns, force index scan
		query = strings.Replace(query, "SELECT", "SELECT /*+ IndexScan */", 1)
	}

	// Execute the query with proper parameter binding
	rows, err := r.db.Query(ctx, query, lastId, idBatchSize)
	if err != nil {
		log.Printf("Failed to fetch primary key batch: %v", err)
		return nil, fmt.Errorf("failed to fetch primary key batch: %w", err)
	}

	//log.Printf("Fetching %d UUIDs took %s", idBatchSize, time.Now().Sub(startTime))

	var ids []any
	for rows.Next() {
		var id any
		if err := rows.Scan(&id); err != nil {
			log.Printf("Row Scan Error: %v", err)
			continue
		}
		ids = append(ids, id)
	}
	rows.Close()

	return ids, nil
}

func (r Repo) GetFirstIdByPrimaryKey(ctx context.Context, schemaName string, tableName string, colName string) (any, error) {
	var id any
	err := config.Db.QueryRow(ctx, fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY %s ASC LIMIT 1", colName, schemaName, tableName, colName)).Scan(&id)
	if err != nil {
		return nil, err
	}

	return id, nil
}

func (r Repo) GetFirstIdsByMultiPrimaryKeys(ctx context.Context, columnMeta []dtos.ColumnInfo, schemaName string, tableName string, keys []dtos.PrimaryKey) (map[string]any, error) {

	columnMetaMap := lo.SliceToMap(columnMeta, func(item dtos.ColumnInfo) (string, dtos.ColumnInfo) {
		return item.Name, item
	})

	keysStr := strings.Join(lo.Map(keys, func(key dtos.PrimaryKey, index int) string { return key.ColumnName }), ", ")

	row := config.Db.QueryRow(ctx, fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY %s ASC LIMIT 1", keysStr, schemaName, tableName, keysStr))

	var values []any
	for _ = range keys {
		values = append(values, new(any))
	}

	if err := row.Scan(values...); err != nil {
		return nil, err
	}

	firstIds := make(map[string]any)
	for i, key := range keys {

		if values[i] == nil {
			firstIds[key.ColumnName] = nil
			continue
		}
		firstIds[key.ColumnName] = getRecord(columnMetaMap, key.ColumnName, values[i])
	}

	return firstIds, nil

}

func (r Repo) GetRecordsById(ctx context.Context, columnMeta []dtos.ColumnInfo, colName string, tableSchema string, tableName string, idStart any, idEnd any) ([]map[string]any, error) {
	// Fetch 100K UUIDs

	columnMetaMap := lo.SliceToMap(columnMeta, func(item dtos.ColumnInfo) (string, dtos.ColumnInfo) {
		return item.Name, item
	})

	var columnNames []string
	for i := range columnMeta {
		columnNames = append(columnNames, columnMeta[i].Name)
	}

	columnList := strings.Join(columnNames, ", ")

	rows, err := config.Db.Query(ctx, fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s >= $1 AND %s <= $2 ORDER BY %s", columnList, tableSchema, tableName, colName, colName, colName), idStart, idEnd)
	if err != nil {
		log.Printf("DB Query Error: %v", err)
		return nil, err
	}

	return deserializeRecords(rows, columnMetaMap, columnNames), nil
}

func (r Repo) GetRecordsByMultiPrimaryKeys(ctx context.Context, columns []dtos.ColumnInfo, keys []dtos.PrimaryKey, tableSchema string, tableName string, idsStart map[string]any, idsEnd map[string]any) ([]map[string]any, error) {
	columnMetaMap := lo.SliceToMap(columns, func(item dtos.ColumnInfo) (string, dtos.ColumnInfo) {
		return item.Name, item
	})

	var columnNames []string
	for i := range columns {
		columnNames = append(columnNames, columns[i].Name)
	}

	columnList := strings.Join(columnNames, ", ")

	var value1 []any
	var value2 []any

	for _, key := range keys {
		value1 = append(value1, idsStart[key.ColumnName])
		value2 = append(value2, idsEnd[key.ColumnName])
	}

	var rhsValuePlaceHolder1 []string
	var rhsValuePlaceHolder2 []string
	for i := range len(keys) {
		rhsValuePlaceHolder1 = append(rhsValuePlaceHolder1, fmt.Sprintf("$%d", i+1))
		rhsValuePlaceHolder2 = append(rhsValuePlaceHolder2, fmt.Sprintf("$%d", i+len(keys)+1))
	}

	rhsValuePlaceHolder1Str := strings.Join(rhsValuePlaceHolder1, ", ")
	rhsValuePlaceHolder2Str := strings.Join(rhsValuePlaceHolder2, ", ")

	value1 = append(value1, value2...)

	keysStr := strings.Join(lo.Map(keys, func(key dtos.PrimaryKey, index int) string { return key.ColumnName }), ", ")

	rows, err := config.Db.Query(ctx,
		fmt.Sprintf("SELECT %s FROM %s.%s WHERE (%s) >= (%s) AND (%s) <= (%s) ORDER BY %s",
			columnList, tableSchema, tableName, keysStr, rhsValuePlaceHolder1Str, keysStr, rhsValuePlaceHolder2Str, keysStr), value1...)
	if err != nil {
		log.Printf("Failed to fetch records by multi primary keys: %v", err)
		return nil, err
	}

	return deserializeRecords(rows, columnMetaMap, columnNames), nil
}

func getRecord(meta map[string]dtos.ColumnInfo, name string, rawValue interface{}) any {
	switch meta[name].DataType {
	case "uuid":
		if uuidBytes, ok := (*rawValue.(*interface{})).([16]uint8); ok {
			return uuid.UUID(uuidBytes).String()
		} else {
			return rawValue
		}
	case "json", "jsonb":
		bytesData, err := json.Marshal((*rawValue.(*interface{})))

		if err == nil {
			return string(bytesData)
		}
	}

	return rawValue
}

func deserializeRecords(rows pgx.Rows, columnMetaMap map[string]dtos.ColumnInfo, columnNames []string) []map[string]any {
	var records []map[string]any

	for rows.Next() {

		values := make([]any, len(columnNames))

		for i := range values {
			values[i] = new(any)
		}

		if err := rows.Scan(values...); err != nil {
			log.Printf("Row Scan Error: %v", err)
			continue
		}

		record := make(map[string]any)
		for i, colName := range columnNames {

			if values[i] == nil {
				record[colName] = nil
				continue
			}

			record[colName] = getRecord(columnMetaMap, colName, values[i])

		}
		records = append(records, record)
	}

	return records
}
