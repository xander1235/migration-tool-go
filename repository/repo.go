package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"migration-tool-go/config"
	"migration-tool-go/dtos"
	"migration-tool-go/dtos/sources/postgres"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
	"regexp"
)

type Repo struct {
	db *pgxpool.Pool
}

func NewRepo(db *pgxpool.Pool) *Repo {
	return &Repo{
		db: db,
	}
}

func (r Repo) GetTableInfo(ctx context.Context, schemas []any, config postgres.Configuration) ([]dtos.TableInfo, error) {
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
		WHERE c.table_schema in (%s) -- Ensures only the selected schema
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

	// Set query strategies based on configuration
	for schemaName, tables := range tableMap {
		for tableName, tableInfo := range tables {
			// Check if there's a strategy in the include tables list
			for _, tableList := range config.IncludeTablesList {
				if tableList.Schema == schemaName {
					for _, includedTable := range tableList.Tables {
						if includedTable == tableName && tableList.QueryStrategy != nil {
							tableInfo.QueryStrategy = tableList.QueryStrategy
							break
						}
					}
				}
				if tableInfo.QueryStrategy != nil {
					break
				}
			}

			// If no strategy found, check the include table regex list
			if tableInfo.QueryStrategy == nil {
				for _, regexList := range config.IncludeTableRegexList {
					if regexList.Schema == schemaName && regexList.QueryStrategy != nil {
						// Check if the table name matches any of the regex patterns
						for _, pattern := range regexList.Regex {
							matched, err := regexp.MatchString(pattern, tableName)
							if err == nil && matched {
								tableInfo.QueryStrategy = regexList.QueryStrategy
								break
							}
						}
					}
					if tableInfo.QueryStrategy != nil {
						break
					}
				}
			}
		}
	}

	var tableInfoList []dtos.TableInfo

	for _, v := range tableMap {
		for _, v2 := range v {
			if v2.TableName == "asset" {
				tableInfoList = append(tableInfoList, *v2)
			}
			//tableInfoList = append(tableInfoList, *v2)
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

func (r Repo) GetMinValue(ctx context.Context, tableSchema string, tableName string, columnName string) (any, error) {
	query := fmt.Sprintf("SELECT MIN(%s) FROM %s.%s", columnName, tableSchema, tableName)

	var minValue any
	err := r.db.QueryRow(ctx, query).Scan(&minValue)
	if err != nil {
		return nil, fmt.Errorf("failed to get min value: %w", err)
	}

	return minValue, nil
}

func (r Repo) GetMaxValue(ctx context.Context, tableSchema string, tableName string, columnName string) (any, error) {
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s.%s", columnName, tableSchema, tableName)

	var maxValue any
	err := r.db.QueryRow(ctx, query).Scan(&maxValue)
	if err != nil {
		return nil, fmt.Errorf("failed to get max value: %w", err)
	}

	return maxValue, nil
}

func (r Repo) GenerateRanges(ctx context.Context, min any, max any, frequency any, columnName string) ([]any, error) {
	// Query to determine the data type of the column
	query := fmt.Sprintf(`
		SELECT data_type 
		FROM information_schema.columns 
		WHERE table_schema = $1 
		AND table_name = $2 
		AND column_name = $3
	`)

	var dataType string
	err := r.db.QueryRow(ctx, query, "public", "table_name", columnName).Scan(&dataType)
	if err != nil {
		return nil, fmt.Errorf("failed to get column data type: %w", err)
	}

	var ranges []any

	// Handle different data types
	switch dataType {
	case "integer", "bigint", "smallint":
		// For numeric types
		minVal, ok := min.(int64)
		if !ok {
			return nil, fmt.Errorf("min value is not an integer")
		}

		maxVal, ok := max.(int64)
		if !ok {
			return nil, fmt.Errorf("max value is not an integer")
		}

		freqVal, ok := frequency.(int64)
		if !ok {
			return nil, fmt.Errorf("frequency is not an integer")
		}

		// Generate ranges
		for val := minVal; val <= maxVal; val += freqVal {
			ranges = append(ranges, val)
		}

		// Add the max value if it's not already included
		if ranges[len(ranges)-1] != maxVal {
			ranges = append(ranges, maxVal)
		}

	case "numeric", "decimal", "double precision", "real":
		// For floating point types
		minVal, ok := min.(float64)
		if !ok {
			return nil, fmt.Errorf("min value is not a float")
		}

		maxVal, ok := max.(float64)
		if !ok {
			return nil, fmt.Errorf("max value is not a float")
		}

		freqVal, ok := frequency.(float64)
		if !ok {
			return nil, fmt.Errorf("frequency is not a float")
		}

		// Generate ranges
		for val := minVal; val <= maxVal; val += freqVal {
			ranges = append(ranges, val)
		}

		// Add the max value if it's not already included
		if ranges[len(ranges)-1] != maxVal {
			ranges = append(ranges, maxVal)
		}

	case "timestamp", "timestamptz", "date":
		// For timestamp types, use a different approach
		// This would require parsing the timestamp strings and incrementing by the frequency
		// For simplicity, we'll just return an error here
		return nil, fmt.Errorf("timestamp ranges should use the time window strategy instead")

	default:
		return nil, fmt.Errorf("unsupported data type for range strategy: %s", dataType)
	}

	return ranges, nil
}

func (r Repo) GetMinTimeValue(ctx context.Context, tableSchema string, tableName string, columnName string) (string, error) {
	query := fmt.Sprintf("SELECT MIN(%s)::text FROM %s.%s", columnName, tableSchema, tableName)

	var minTime string
	err := r.db.QueryRow(ctx, query).Scan(&minTime)
	if err != nil {
		return "", fmt.Errorf("failed to get min time value: %w", err)
	}

	return minTime, nil
}

func (r Repo) GetMaxTimeValue(ctx context.Context, tableSchema string, tableName string, columnName string) (string, error) {
	query := fmt.Sprintf("SELECT MAX(%s)::text FROM %s.%s", columnName, tableSchema, tableName)

	var maxTime string
	err := r.db.QueryRow(ctx, query).Scan(&maxTime)
	if err != nil {
		return "", fmt.Errorf("failed to get max time value: %w", err)
	}

	return maxTime, nil
}

func (r Repo) GenerateTimeWindows(ctx context.Context, startTime string, endTime string, windowSize string) ([]any, error) {
	// Query to generate time windows using PostgreSQL's generate_series function
	query := fmt.Sprintf(`
		SELECT generate_series::text 
		FROM generate_series(
			$1::timestamp, 
			$2::timestamp, 
			$3::interval
		)
	`)

	rows, err := r.db.Query(ctx, query, startTime, endTime, windowSize)
	if err != nil {
		return nil, fmt.Errorf("failed to generate time windows: %w", err)
	}
	defer rows.Close()

	var timeWindows []any
	for rows.Next() {
		var timeWindow string
		if err := rows.Scan(&timeWindow); err != nil {
			return nil, fmt.Errorf("failed to scan time window: %w", err)
		}
		timeWindows = append(timeWindows, timeWindow)
	}

	// Add the end time if it's not already included
	if len(timeWindows) > 0 && timeWindows[len(timeWindows)-1] != endTime {
		timeWindows = append(timeWindows, endTime)
	}

	return timeWindows, nil
}

func (r Repo) GetRecordsByColumnRange(ctx context.Context, columnMeta []dtos.ColumnInfo, tableSchema string, tableName string, rangeType string, startValue any, endValue any, columnName string) ([]map[string]any, error) {
	// Build the column list
	var columnNames []string
	for _, col := range columnMeta {
		columnNames = append(columnNames, col.Name)
	}

	// Build the column meta map for deserialization
	columnMetaMap := lo.SliceToMap(columnMeta, func(item dtos.ColumnInfo) (string, dtos.ColumnInfo) {
		return item.Name, item
	})

	var operator string

	switch rangeType {
	case "column_range":
		// For regular column ranges, use >= and <
		operator = fmt.Sprintf("%s BETWEEN $1 AND $2", columnName)
	case "time_window":
		// For time windows, use >= and <
		operator = fmt.Sprintf("%s >= $1 AND %s < $2", columnName, columnName)
	case "fixed_value":
		// For fixed values, use =
		operator = "= $1"
	default:
		return nil, fmt.Errorf("unsupported range type: %s", rangeType)
	}

	// Build the query
	var query string
	var args []any

	if rangeType == "fixed_value" {
		query = fmt.Sprintf(
			"SELECT %s FROM %s.%s WHERE %s %s",
			strings.Join(columnNames, ", "),
			tableSchema,
			tableName,
			columnName,
			operator,
		)
		args = []any{startValue}
	} else {
		query = fmt.Sprintf(
			"SELECT %s FROM %s.%s WHERE %s",
			strings.Join(columnNames, ", "),
			tableSchema,
			tableName,
			operator,
		)
		args = []any{startValue, endValue}
	}

	// Execute the query
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch records by column range: %w", err)
	}

	return deserializeRecords(rows, columnMetaMap, columnNames), nil
}
