package services

import (
	"context"
	"migration-tool-go/config"
	"migration-tool-go/dtos"
	"migration-tool-go/dtos/common"
	"migration-tool-go/dtos/sources/postgres"
	"migration-tool-go/logger"
	"migration-tool-go/repository"
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

	tableInfoList, err := p.repo.GetTableInfo(ctx, schemas)

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

	if len(tableInfoChan.TableInfo.PrimaryKeys) == 0 {
		logger.Sugar.Errorf("Table %s.%s has no primary key", tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName)
		return
	}

	if len(tableInfoChan.TableInfo.PrimaryKeys) > 1 {
		firstIds, err := p.repo.GetFirstIdsByMultiPrimaryKeys(ctx, tableInfoChan.TableInfo.Columns, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, tableInfoChan.TableInfo.PrimaryKeys)

		if err != nil {
			logger.Sugar.Errorf("Failed to fetch first primary key: %v", err)
			return
		}

		go p.getMultiPrimaryKeyRange(ctx, firstIds, true, p.workerConfig.IdBatchSize, p.workerConfig.WorkerBatchSize, tableInfoChan)

	} else {
		firstId, err := p.repo.GetFirstIdByPrimaryKey(ctx, tableInfoChan.TableInfo.TableSchema, tableInfoChan.TableInfo.TableName, tableInfoChan.TableInfo.PrimaryKeys[0].ColumnName)

		if err != nil {
			logger.Sugar.Errorf("Failed to fetch first primary key: %v", err)
			return
		}

		go p.getPrimaryKeyRange(ctx, firstId, true, tableInfoChan.TableInfo.PrimaryKeys[0].ColumnName, p.workerConfig.IdBatchSize, p.workerConfig.WorkerBatchSize, tableInfoChan)
	}

	p.getRecordsFromPrimaryKeyRange(ctx, tableInfoChan)

	<-concurrentTables
	wg.Done()
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

				if infoChan.GetTotalUuidsRead() == infoChan.GetTotalRecordsRead() {
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
