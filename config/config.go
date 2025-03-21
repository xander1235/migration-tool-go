package config

import (
	"encoding/json"
	"migration-tool-go/dtos/common"
	"migration-tool-go/dtos/destinations/doris"
	"migration-tool-go/dtos/sources/postgres"
	"migration-tool-go/logger"
	"os"
)

var (
	SourceConfig      = common.Source[any]{}
	DestinationConfig = common.Destination[any]{}
	WorkerConfig      common.WorkerConfiguration
	TrackingConfig    common.TackingConfiguration
	StatsConfig       common.StatsConfiguration
)

func InitializeConfig(configPath string) {

	// Read the config file using the path
	configFile, err := os.Open(configPath)
	if err != nil {
		logger.Sugar.Fatalf("Failed to open config file: %v", err)
	}

	defer func(configFile *os.File) {
		err := configFile.Close()
		if err != nil {

		}
	}(configFile)

	jsonData, err := os.ReadFile(configPath)
	if err != nil {
		logger.Sugar.Fatalf("Failed to read config file: %v", err)
	}

	// Step 1: Extract raw JSON to check types
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(jsonData, &raw); err != nil {
		logger.Sugar.Fatalf("Error parsing JSON: %v", err)
	}

	// Step 2: Extract source type
	var sourceType struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw["source"], &sourceType); err != nil {
		logger.Sugar.Fatalf("Error extracting source type: %v", err)
	}

	switch sourceType.Type {
	case "postgres":
		var source postgres.Postgres
		if err := json.Unmarshal(sourceType.Value, &source); err != nil {
			logger.Sugar.Fatalf("Error parsing common.Source[postgres.Postgres]: %v", err)
		}
		SourceConfig.Type = sourceType.Type
		SourceConfig.Value = source
	}

	// Step 3: Extract destination type
	var destType struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw["destination"], &destType); err != nil {
		logger.Sugar.Fatalf("Error extracting destination type: %v", err)
	}

	switch destType.Type {
	case "doris":
		destination := doris.Doris{}
		if err := json.Unmarshal(destType.Value, &destination); err != nil {
			logger.Sugar.Fatalf("Error parsing common.Destination[doris.Doris]: %v", err)
		}
		DestinationConfig.Type = destType.Type
		DestinationConfig.Value = destination
	}

	if err := json.Unmarshal(raw["worker_configuration"], &WorkerConfig); err != nil {
		logger.Sugar.Fatalf("Error extracting worker configuration: %v", err)
	}

	if err := json.Unmarshal(raw["tracking_configuration"], &TrackingConfig); err != nil {
		logger.Sugar.Fatalf("Error extracting tracking configuration: %v", err)
	}

	// Parse stats configuration if it exists
	if statsConfigRaw, ok := raw["stats_configuration"]; ok {
		if err := json.Unmarshal(statsConfigRaw, &StatsConfig); err != nil {
			logger.Sugar.Fatalf("Error extracting stats configuration: %v", err)
		}
	}

	logger.Sugar.Info("Configuration loaded successfully")

}
