package dtos

import (
	"sync/atomic"
)

type TableInfoChan struct {
	TableInfo          TableInfo
	PrimaryKeyRange    chan PrimaryKeyRange
	RecordsChan        chan map[string]any
	totalUuidsRead     uint64
	totalRecordsRead   uint64
	ReadingIdsDone     atomic.Value
	ReadingRecordsDone atomic.Value
}

func NewTableInfoChan(tableInfo TableInfo, primaryKeyRangeSize int, recordsChanSize int) *TableInfoChan {
	tableInfoChan := &TableInfoChan{
		TableInfo:          tableInfo,
		PrimaryKeyRange:    make(chan PrimaryKeyRange, primaryKeyRangeSize),
		RecordsChan:        make(chan map[string]any, recordsChanSize),
		totalUuidsRead:     0,
		totalRecordsRead:   0,
	}
	tableInfoChan.ReadingIdsDone.Store(false)
	tableInfoChan.ReadingRecordsDone.Store(false)
	return tableInfoChan
}

func (t *TableInfoChan) IncrementTotalUuidsRead(count uint64) {
	atomic.AddUint64(&t.totalUuidsRead, count)
}

func (t *TableInfoChan) IncrementTotalRecordsRead(count uint64) {
	atomic.AddUint64(&t.totalRecordsRead, count)
}

func (t *TableInfoChan) GetTotalUuidsRead() uint64 {
	return atomic.LoadUint64(&t.totalUuidsRead)
}

func (t *TableInfoChan) GetTotalRecordsRead() uint64 {
	return atomic.LoadUint64(&t.totalRecordsRead)
}
