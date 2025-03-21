package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"migration-tool-go/logger"
	"os"
)

const parquetBatchSize = 100000 // Adjust batch size based on testing

// PostgreSQL → Parquet Type Mapping
var postgresToParquet = map[string]parquet.Type{
	"uuid":        parquet.Type_BYTE_ARRAY,
	"_varchar":    parquet.Type_BYTE_ARRAY,
	"varchar":     parquet.Type_BYTE_ARRAY,
	"_text":       parquet.Type_BYTE_ARRAY,
	"text":        parquet.Type_BYTE_ARRAY,
	"jsonb":       parquet.Type_BYTE_ARRAY,
	"json":        parquet.Type_BYTE_ARRAY,
	"date":        parquet.Type_INT32,
	"char":        parquet.Type_BYTE_ARRAY,
	"_char":       parquet.Type_BYTE_ARRAY,
	"timestamptz": parquet.Type_INT64,
	"timestamp":   parquet.Type_INT64,
	"float8":      parquet.Type_DOUBLE,
	"float4":      parquet.Type_FLOAT,
	"_float8":     parquet.Type_DOUBLE,
	"_float4":     parquet.Type_FLOAT,
	"int8":        parquet.Type_INT64,
	"int4":        parquet.Type_INT32,
	"int2":        parquet.Type_INT32,
	"_int2":       parquet.Type_INT32,
	"int2vector":  parquet.Type_BYTE_ARRAY,
	"bool":        parquet.Type_BOOLEAN,
	"_bool":       parquet.Type_BOOLEAN,
	"bytea":       parquet.Type_BYTE_ARRAY,
	"numeric":     parquet.Type_BYTE_ARRAY, // Store as string
	"anyarray":    parquet.Type_BYTE_ARRAY,
}

func ConvertRecordsToParquet(records []map[string]any) [][]byte {
	if len(records) == 0 {
		logger.Sugar.Fatal("No records to write")
	}

	var parquetFiles [][]byte
	for i := 0; i < len(records); i += parquetBatchSize {
		end := i + parquetBatchSize
		if end > len(records) {
			end = len(records)
		}

		// Generate unique filename
		parquetFile := fmt.Sprintf("%s_%d.parquet", "filePrefix", i/parquetBatchSize)

		// Create Parquet file
		f, err := os.Create(parquetFile)
		if err != nil {
			logger.Sugar.Fatalf("Failed to create Parquet file: %v", err)
		}

		// Convert batch to Parquet
		parquetBuffer := new(bytes.Buffer)
		pw, _ := writer.NewParquetWriterFromWriter(f, make(map[string]any), 1)
		pw.CompressionType = parquet.CompressionCodec_SNAPPY

		for _, rec := range records[i:end] {
			_ = pw.Write(rec)
		}

		err = pw.WriteStop()
		if err != nil {
			return nil
		}

		f.Close()

		parquetFiles = append(parquetFiles, parquetBuffer.Bytes())
	}
	return parquetFiles
}

// ConvertRecordsToJSON converts records to JSON bytes (with optional debug file)
func ConvertRecordsToJSON[T any](records []T, debugFile string, writeToFile bool) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to write")
	}

	// Convert records to JSON
	jsonData, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// If a debug file is provided, write the JSON data to it
	if debugFile != "" && writeToFile {
		err = os.WriteFile(debugFile, jsonData, 0644)
		if err != nil {
			logger.Sugar.Warnf("Failed to write debug JSON file %s: %v", debugFile, err)
		} else {
			logger.Sugar.Infof("✅ Debug JSON file saved: %s", debugFile)
		}
	}

	return jsonData, nil
}
