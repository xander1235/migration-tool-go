package services

import (
	"context"
	"fmt"
	"migration-tool-go/config"
	"migration-tool-go/dtos"
	"migration-tool-go/dtos/common"
	"migration-tool-go/dtos/sources/postgres"
	"migration-tool-go/logger"
	"migration-tool-go/repository"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var PostgresMigration = &postgresMigration{}

type postgresMigration struct {
	configuration postgres.Configuration
	workerConfig  common.WorkerConfiguration
	repo          *repository.Repo
}

//type TableInfoChan struct {
//	TableInfo          dtos.TableInfo
//	PrimaryKeyRange    chan dtos.PrimaryKeyRange
//	RecordsChan        chan map[string]any
//	TotalUuidsRead     *int
//	TotalRecordsRead   *int
//	ReadingIdsDone     *bool
//	ReadingRecordsDone *bool
//}

func NewPostgresMigration(source common.Source[any], workerConfig common.WorkerConfiguration) {
	PostgresMigration = &postgresMigration{
		configuration: source.Value.(postgres.Postgres).Configuration,
		workerConfig:  workerConfig,
		repo:          repository.NewRepo(config.NewConnection(source.Value.(postgres.Postgres), workerConfig.NoOfWorkers)),
	}
}

func (p postgresMigration) GetRecordsFromSource(ctx context.Context, tableInfoChan chan *dtos.TableInfoChan, processedAllTables *bool) error {
	var schemas []any

	for _, schema := range p.configuration.Schemas {
		schemas = append(schemas, schema)
	}

	tableInfoList, err := p.repo.GetTableInfo(ctx, schemas, p.configuration)

	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	concurrentTables := make(chan bool, p.workerConfig.ConcurrentTables)

	for _, tableInfo := range tableInfoList {
		infoChan := dtos.NewTableInfoChan(tableInfo, p.workerConfig.WorkerBatchSize, p.workerConfig.IdBatchSize)
		tableInfoChan <- infoChan
		concurrentTables <- true

		wg.Add(1)
		go p.processTable(ctx, infoChan, concurrentTables, &wg)
	}

	defer close(concurrentTables)

	wg.Wait()

	*processedAllTables = true

	return nil
}

func (p postgresMigration) processTable(ctx context.Context, tableInfoChan *dtos.TableInfoChan, concurrentTables chan bool, wg *sync.WaitGroup) {
	// Check if there's a custom query strategy defined for this table
	if tableInfoChan.TableInfo.QueryStrategy != nil {
		// Use the custom query strategy
		logger.Sugar.Infof("Using custom query strategy for table %s.%s: %s",
			tableInfoChan.TableInfo.TableSchema,
			tableInfoChan.TableInfo.TableName,
			tableInfoChan.TableInfo.QueryStrategy.Type)
		go p.processWithQueryStrategy(ctx, tableInfoChan, tableInfoChan.TableInfo.QueryStrategy)
	} else {
		// Fall back to the default primary key based approach
		logger.Sugar.Infof("No custom query strategy defined for table %s.%s, using default primary key strategy",
			tableInfoChan.TableInfo.TableSchema,
			tableInfoChan.TableInfo.TableName)

		p.processWithPrimaryKeyStrategy(ctx, tableInfoChan)
	}

	p.getRecordsFromPrimaryKeyRange(ctx, tableInfoChan)

	<-concurrentTables
	wg.Done()
}

// processWithPrimaryKeyStrategy processes a table using the default primary key strategy
func (p postgresMigration) processWithPrimaryKeyStrategy(ctx context.Context, tableInfoChan *dtos.TableInfoChan) {
	if len(tableInfoChan.TableInfo.PrimaryKeys) == 0 {
		logger.Sugar.Errorf("Table %s.%s has no primary key", tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName)
		tableInfoChan.ReadingIdsDone.Store(true)
		return
	}

	if len(tableInfoChan.TableInfo.PrimaryKeys) > 1 {
		firstIds, err := p.repo.GetFirstIdsByMultiPrimaryKeys(ctx, tableInfoChan.TableInfo.Columns, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, tableInfoChan.TableInfo.PrimaryKeys)

		if err != nil {
			logger.Sugar.Errorf("Failed to fetch first primary key: %v", err)
			tableInfoChan.ReadingIdsDone.Store(true)
			return
		}

		go p.getMultiPrimaryKeyRange(ctx, firstIds, true, p.workerConfig.IdBatchSize, p.workerConfig.WorkerBatchSize, tableInfoChan)

	} else {
		firstId, err := p.repo.GetFirstIdByPrimaryKey(ctx, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, tableInfoChan.TableInfo.PrimaryKeys[0].ColumnName)

		if err != nil {
			logger.Sugar.Errorf("Failed to fetch first primary key: %v", err)
			tableInfoChan.ReadingIdsDone.Store(true)
			return
		}

		go p.getPrimaryKeyRange(ctx, firstId, true, tableInfoChan.TableInfo.PrimaryKeys[0].ColumnName, p.workerConfig.IdBatchSize, p.workerConfig.WorkerBatchSize, tableInfoChan)
	}
}

// processWithQueryStrategy processes a table using the specified query strategy
func (p postgresMigration) processWithQueryStrategy(ctx context.Context, tableInfoChan *dtos.TableInfoChan, strategy *postgres.QueryStrategy) {
	switch strategy.Type {
	case postgres.RangeStrategy:
		p.processRangeStrategy(ctx, tableInfoChan, strategy)
	case postgres.FixedValuesStrategy:
		p.processFixedValuesStrategy(ctx, tableInfoChan, strategy)
	case postgres.TimeWindowStrategy:
		p.processTimeWindowStrategy(ctx, tableInfoChan, strategy)
	case postgres.BatchSizeStrategy:
		p.processBatchSizeStrategy(ctx, tableInfoChan, strategy)
	default:
		logger.Sugar.Errorf("Unknown query strategy type: %s", strategy.Type)
		tableInfoChan.ReadingIdsDone.Store(true)
	}
}

// processRangeStrategy processes a table using the range strategy
func (p postgresMigration) processRangeStrategy(ctx context.Context, tableInfoChan *dtos.TableInfoChan, strategy *postgres.QueryStrategy) {
	if strategy.RangeParams == nil {
		logger.Sugar.Errorf("Range strategy parameters are missing")
		tableInfoChan.ReadingIdsDone.Store(true)
		return
	}

	// Get min and max values if not provided
	min := strategy.RangeParams.Min
	max := strategy.RangeParams.Max
	var err error

	if min == nil {
		min, err = p.repo.GetMinValue(ctx, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, strategy.Column)
		if err != nil {
			logger.Sugar.Errorf("Failed to get min value: %v", err)
			tableInfoChan.ReadingIdsDone.Store(true)
			return
		}
	}

	if max == nil {
		max, err = p.repo.GetMaxValue(ctx, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, strategy.Column)
		if err != nil {
			logger.Sugar.Errorf("Failed to get max value: %v", err)
			tableInfoChan.ReadingIdsDone.Store(true)
			return
		}
	}

	// Generate ranges based on the frequency
	ranges, err := p.GenerateTimeWindows(strategy.RangeParams.Min, strategy.RangeParams.Max, strategy.RangeParams.Frequency)
	if err != nil {
		logger.Sugar.Errorf("Failed to generate ranges: %v", err)
		tableInfoChan.ReadingIdsDone.Store(true)
		return
	}

	// Send ranges to the channel
	for i := 0; i < len(ranges)-1; i++ {
		tableInfoChan.PrimaryKeyRange <- dtos.PrimaryKeyRange{
			Type:    "column_range",
			IdRange: [2]any{ranges[i], ranges[i+1]},
		}
	}

	tableInfoChan.IncrementTotalUuidsRead(uint64(len(ranges) - 1))
	tableInfoChan.ReadingIdsDone.Store(true)
}

// processFixedValuesStrategy processes a table using fixed values
func (p postgresMigration) processFixedValuesStrategy(ctx context.Context, tableInfoChan *dtos.TableInfoChan, strategy *postgres.QueryStrategy) {
	if strategy.FixedValuesParams == nil || len(strategy.FixedValuesParams.Values) == 0 {
		logger.Sugar.Errorf("Fixed values strategy parameters are missing or empty")
		tableInfoChan.ReadingIdsDone.Store(true)
		return
	}

	batchSize := strategy.FixedValuesParams.BatchSize
	if batchSize <= 0 {
		batchSize = p.workerConfig.WorkerBatchSize
	}

	// Send each fixed value as a separate range
	for _, value := range strategy.FixedValuesParams.Values {
		tableInfoChan.PrimaryKeyRange <- dtos.PrimaryKeyRange{
			Type:    "fixed_value",
			IdRange: [2]any{value, value},
		}
	}

	tableInfoChan.IncrementTotalUuidsRead(uint64(len(strategy.FixedValuesParams.Values)))
	tableInfoChan.ReadingIdsDone.Store(true)
}

// processTimeWindowStrategy processes a table using time windows
func (p postgresMigration) processTimeWindowStrategy(ctx context.Context, tableInfoChan *dtos.TableInfoChan, strategy *postgres.QueryStrategy) {
	if strategy.TimeWindowParams == nil {
		logger.Sugar.Errorf("Time window strategy parameters are missing")
		tableInfoChan.ReadingIdsDone.Store(true)
		return
	}

	// Get start and end times if not provided
	startTime := strategy.TimeWindowParams.StartTime
	endTime := strategy.TimeWindowParams.EndTime
	var err error

	if startTime == "" {
		startTime, err = p.repo.GetMinTimeValue(ctx, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, strategy.Column)
		if err != nil {
			logger.Sugar.Errorf("Failed to get min time value: %v", err)
			tableInfoChan.ReadingIdsDone.Store(true)
			return
		}
	}

	if endTime == "" {
		endTime, err = p.repo.GetMaxTimeValue(ctx, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, strategy.Column)
		if err != nil {
			logger.Sugar.Errorf("Failed to get max time value: %v", err)
			tableInfoChan.ReadingIdsDone.Store(true)
			return
		}
	}

	// Generate time windows
	timeWindows, err := p.GenerateTimeWindows(startTime, endTime, strategy.TimeWindowParams.WindowSize)
	if err != nil {
		logger.Sugar.Errorf("Failed to generate time windows: %v", err)
		tableInfoChan.ReadingIdsDone.Store(true)
		return
	}

	// Send time windows to the channel
	for i := 0; i < len(timeWindows)-1; i++ {
		tableInfoChan.PrimaryKeyRange <- dtos.PrimaryKeyRange{
			Type:    "time_window",
			IdRange: [2]any{timeWindows[i], timeWindows[i+1]},
		}
	}

	tableInfoChan.IncrementTotalUuidsRead(uint64(len(timeWindows) - 1))
	tableInfoChan.ReadingIdsDone.Store(true)
}

// processBatchSizeStrategy processes a table using simple batch size approach
func (p postgresMigration) processBatchSizeStrategy(ctx context.Context, tableInfoChan *dtos.TableInfoChan, strategy *postgres.QueryStrategy) {
	if strategy.BatchSizeParams == nil {
		logger.Sugar.Errorf("Batch size strategy parameters are missing")
		tableInfoChan.ReadingIdsDone.Store(true)
		return
	}

	batchSize := strategy.BatchSizeParams.BatchSize
	if batchSize <= 0 {
		batchSize = p.workerConfig.WorkerBatchSize
	}

	orderBy := strategy.BatchSizeParams.OrderBy
	if orderBy == "" {
		// Default to the first primary key if available
		if len(tableInfoChan.TableInfo.PrimaryKeys) > 0 {
			orderBy = tableInfoChan.TableInfo.PrimaryKeys[0].ColumnName
		} else {
			// Fall back to the strategy column if no primary key
			orderBy = strategy.Column
		}
	}

	// Use the existing primary key range function with the specified column
	firstId, err := p.repo.GetFirstIdByPrimaryKey(ctx, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, orderBy)
	if err != nil {
		logger.Sugar.Errorf("Failed to fetch first value: %v", err)
		tableInfoChan.ReadingIdsDone.Store(true)
		return
	}

	go p.getPrimaryKeyRange(ctx, firstId, true, orderBy, p.workerConfig.IdBatchSize, batchSize, tableInfoChan)
}

func (p postgresMigration) getRecordsFromPrimaryKeyRange(ctx context.Context, infoChan *dtos.TableInfoChan) {

	wg := sync.WaitGroup{}
	parallelProcessingChan := make(chan bool, p.workerConfig.NoOfWorkers)
	processingDone := false
	for {
		select {
		case primaryKeyRange := <-infoChan.PrimaryKeyRange:
			parallelProcessingChan <- true
			wg.Add(1)
			go func(wgIn *sync.WaitGroup, parallelProcessingChanIn chan bool) {
				switch primaryKeyRange.Type {
				case "id_range":
					records, err := p.repo.GetRecordsById(ctx, infoChan.TableInfo.Columns, infoChan.TableInfo.PrimaryKeys[0].ColumnName, infoChan.TableInfo.TableSchema, infoChan.TableInfo.TableName, primaryKeyRange.IdRange[0], primaryKeyRange.IdRange[1])

					if err != nil {
						logger.Sugar.Errorf("Failed to fetch records by uuids: %v", err)
					} else {
						//uuidStr := uuid.New().String()
						//utils.ConvertRecordsToJSON(records, fmt.Sprintf("records_json/%s_debug.json", uuidStr), true)
						//
						//utils.ConvertRecordsToJSON([]any{primaryKeyRange.IdRange}, fmt.Sprintf("uuid_range_json/%s_debug.json", uuidStr), true)

						for _, record := range records {
							infoChan.RecordsChan <- record
						}

						infoChan.IncrementTotalRecordsRead(uint64(len(records)))
						infoChan.IncrementTotalUuidsProcessed(1)
					}

				case "multi_key":
					records, err := p.repo.GetRecordsByMultiPrimaryKeys(ctx, infoChan.TableInfo.Columns, infoChan.TableInfo.PrimaryKeys, infoChan.TableInfo.TableSchema, infoChan.TableInfo.TableName, primaryKeyRange.MultiKeyRange[0], primaryKeyRange.MultiKeyRange[1])

					if err != nil {
						logger.Sugar.Errorf("Failed to fetch records by multi primary keys: %v", err)

					} else {
						for _, record := range records {
							infoChan.RecordsChan <- record
						}

						infoChan.IncrementTotalRecordsRead(uint64(len(records)))
						infoChan.IncrementTotalUuidsProcessed(1)
					}

				case "column_range", "time_window", "fixed_value":
					var columnName = infoChan.TableInfo.PrimaryKeys[0].ColumnName

					if infoChan.TableInfo.QueryStrategy != nil && infoChan.TableInfo.QueryStrategy.Column != "" {
						columnName = infoChan.TableInfo.QueryStrategy.Column
					}

					records, err := p.repo.GetRecordsByColumnRange(ctx, infoChan.TableInfo.Columns, infoChan.TableInfo.TableSchema, infoChan.TableInfo.TableName, primaryKeyRange.Type, primaryKeyRange.IdRange[0], primaryKeyRange.IdRange[1], columnName)

					if err != nil {
						logger.Sugar.Errorf("Failed to fetch records by column range: %v", err)
					} else {
						for _, record := range records {
							infoChan.RecordsChan <- record
						}

						infoChan.IncrementTotalRecordsRead(uint64(len(records)))
						infoChan.IncrementTotalUuidsProcessed(1)
					}
				}

				<-parallelProcessingChanIn
				wgIn.Done()
			}(&wg, parallelProcessingChan)

			// Add the timeout to read the chan to prevent it from blocking
		case <-time.After(5 * time.Second):
			if infoChan.ReadingIdsDone.Load().(bool) {
				if len(infoChan.PrimaryKeyRange) == 0 {
					logger.Sugar.Infof("Finished reading the ids %s.%s", infoChan.TableInfo.TableSchema, infoChan.TableInfo.TableName)
				}

				if infoChan.GetTotalUuidsRead() == infoChan.GetTotalUuidsProcessed() {
					infoChan.ReadingRecordsDone.Store(true)
					logger.Sugar.Infof("Finished reading the records %s.%s", infoChan.TableInfo.TableSchema, infoChan.TableInfo.TableName)
					processingDone = true
					break
				}
			}
		}

		if processingDone {
			break
		}
	}

	defer close(parallelProcessingChan)
	wg.Wait()
}

func (p postgresMigration) getPrimaryKeyRange(ctx context.Context, lastId any, includeLastId bool, primaryKey string, idBatchSize int, workerBatchSize int, tableInfoChan *dtos.TableInfoChan) {

	for {

		ids, err := p.repo.FetchBatchPrimaryKeys(ctx, lastId, includeLastId, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, primaryKey, idBatchSize)

		if err != nil {
			logger.Sugar.Errorf("Failed to fetch primary key batch: %v", err)
			return
		}

		if includeLastId {
			includeLastId = false
		}

		if len(ids) == 0 {
			tableInfoChan.ReadingIdsDone.Store(true)
			break
		}

		//utils.ConvertRecordsToJSON(ids, fmt.Sprintf("uuids_json/%s_debug.json", uuid.New().String()), true)

		// Divide into batches of 10K (first & last UUID)
		for i := 0; i < len(ids); i += workerBatchSize {
			end := i + workerBatchSize
			if end > len(ids) {
				end = len(ids)
			}

			tableInfoChan.PrimaryKeyRange <- dtos.PrimaryKeyRange{Type: "id_range", IdRange: [2]any{ids[i], ids[end-1]}}

			//switch tableInfo.PrimaryKeys[0].DataType {
			//case "uuid":
			//	tableInfoChan.PrimaryKeyRange <- dtos.PrimaryKeyRange{Type: "id_range", IdRange: [2]any{ids[i], ids[end-1]}}
			//case "int", "int4", "int8":
			//	tableInfoChan.PrimaryKeyRange <- dtos.PrimaryKeyRange{Type: "id_range", IdRange: [2]any{ids[i], ids[end-1]}}
			//}
		}

		tableInfoChan.IncrementTotalUuidsRead(uint64(len(ids)))

		// Update lastUUID for next iteration
		lastId = ids[len(ids)-1]
	}
}

func (p postgresMigration) getMultiPrimaryKeyRange(ctx context.Context, lastIds map[string]any, includeLastId bool, idBatchSize int, workerBatchSize int, tableInfoChan *dtos.TableInfoChan) {
	for {

		ids, err := p.repo.FetchBatchMultiPrimaryKeys(ctx, lastIds, includeLastId, tableInfoChan.TableInfo.Columns, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, tableInfoChan.TableInfo.PrimaryKeys, idBatchSize)

		if err != nil {
			logger.Sugar.Errorf("Failed to fetch multi primary keys batch: %v", err)
			return
		}

		if includeLastId {
			includeLastId = false
		}

		if len(ids) == 0 {
			tableInfoChan.ReadingIdsDone.Store(true)
			break
		}

		// Divide into batches of 10K (first & last UUID)
		for i := 0; i < len(ids); i += workerBatchSize {
			end := i + workerBatchSize
			if end > len(ids) {
				end = len(ids)
			}

			tableInfoChan.PrimaryKeyRange <- dtos.PrimaryKeyRange{Type: "multi_key", MultiKeyRange: [2]map[string]any{ids[i], ids[end-1]}}
		}

		tableInfoChan.IncrementTotalUuidsRead(uint64(len(ids)))

		// Update lastUUID for next iteration
		lastIds = ids[len(ids)-1]
	}

}

// GenerateTimeWindows creates time windows between start and end times
func (p postgresMigration) GenerateTimeWindows(startTime, endTime any, windowSize any) ([]any, error) {
	var start, end time.Time
	var interval time.Duration
	var err error

	// Convert startTime to time.Time
	switch st := startTime.(type) {
	case time.Time:
		start = st
	case string:
		start, err = time.Parse(time.RFC3339, st)
		if err != nil {
			err = nil
			start, err = time.Parse(time.DateTime, st)
			if err != nil {
				return nil, fmt.Errorf("invalid start time format: %w", err)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported start time type: %T", startTime)
	}

	// Convert endTime to time.Time
	switch et := endTime.(type) {
	case time.Time:
		end = et
	case string:
		end, err = time.Parse(time.RFC3339, et)
		if err != nil {
			err = nil
			end, err = time.Parse(time.DateTime, et)
			if err != nil {
				return nil, fmt.Errorf("invalid end time format: %w", err)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported end time type: %T", endTime)
	}

	// Convert windowSize to time.Duration
	switch ws := windowSize.(type) {
	case time.Duration:
		interval = ws
	case string:
		interval, err = parseDuration(ws)
		if err != nil {
			return nil, fmt.Errorf("invalid window size format: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported window size type: %T", windowSize)
	}

	if interval <= 0 {
		return nil, fmt.Errorf("invalid interval duration")
	}

	var windows []any
	for t := start; t.Before(end); t = t.Add(interval) {
		windows = append(windows, t)
	}

	// Add the end time if it's not already included
	if len(windows) > 0 && windows[len(windows)-1] != end {
		windows = append(windows, end)
	}

	return windows, nil
}

// parseDuration parses a PostgreSQL interval string into a time.Duration
func parseDuration(interval string) (time.Duration, error) {
	// Handle common PostgreSQL interval formats
	switch interval {
	case "1 second":
		return time.Second, nil
	case "1 minute":
		return time.Minute, nil
	case "1 hour":
		return time.Hour, nil
	case "1 day":
		return 24 * time.Hour, nil
	case "1 week":
		return 7 * 24 * time.Hour, nil
	case "1 month":
		return 30 * 24 * time.Hour, nil
	case "1 year":
		return 365 * 24 * time.Hour, nil
	}

	// Try to parse more complex intervals
	// This is a simplified version and may need to be expanded based on your needs
	re := regexp.MustCompile(`(\d+)\s+(\w+)`)
	matches := re.FindStringSubmatch(interval)
	if len(matches) == 3 {
		value, err := strconv.Atoi(matches[1])
		if err != nil {
			return 0, fmt.Errorf("invalid interval value: %w", err)
		}

		unit := matches[2]
		switch {
		case strings.HasPrefix(unit, "second"):
			return time.Duration(value) * time.Second, nil
		case strings.HasPrefix(unit, "minute"):
			return time.Duration(value) * time.Minute, nil
		case strings.HasPrefix(unit, "hour"):
			return time.Duration(value) * time.Hour, nil
		case strings.HasPrefix(unit, "day"):
			return time.Duration(value) * 24 * time.Hour, nil
		case strings.HasPrefix(unit, "week"):
			return time.Duration(value) * 7 * 24 * time.Hour, nil
		case strings.HasPrefix(unit, "month"):
			return time.Duration(value) * 30 * 24 * time.Hour, nil
		case strings.HasPrefix(unit, "year"):
			return time.Duration(value) * 365 * 24 * time.Hour, nil
		}
	}

	return 0, fmt.Errorf("unsupported interval format: %s", interval)
}
