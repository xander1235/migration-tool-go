package dtos

import (
	"sync/atomic"
)

type TableInfoChan struct {
	TableInfo             TableInfo
	PrimaryKeyRange       chan PrimaryKeyRange
	RecordsChan           chan map[string]any
	totalUuidsRead        *uint64
	totalRecordsRead      *uint64
	totalRecordsProcessed *uint64
	totalUuidsProcessed   *uint64
	ReadingIdsDone        atomic.Value
	ReadingRecordsDone    atomic.Value
}

func NewTableInfoChan(tableInfo TableInfo, primaryKeyRangeSize int, recordsChanSize int) *TableInfoChan {
	var totalUuidsRead uint64 = 0
	var totalRecordsRead uint64 = 0
	var totalRecordsProcessed uint64 = 0
	var totalUuidsProcessed uint64 = 0
	tableInfoChan := &TableInfoChan{
		TableInfo:             tableInfo,
		PrimaryKeyRange:       make(chan PrimaryKeyRange, primaryKeyRangeSize),
		RecordsChan:           make(chan map[string]any, recordsChanSize),
		totalUuidsRead:        &totalUuidsRead,
		totalRecordsRead:      &totalRecordsRead,
		totalRecordsProcessed: &totalRecordsProcessed,
		totalUuidsProcessed:   &totalUuidsProcessed,
	}
	tableInfoChan.ReadingIdsDone.Store(false)
	tableInfoChan.ReadingRecordsDone.Store(false)
	return tableInfoChan
}

func (t *TableInfoChan) IncrementTotalUuidsRead(count uint64) {
	atomic.AddUint64(t.totalUuidsRead, count)
}

func (t *TableInfoChan) IncrementTotalUuidsProcessed(count uint64) {
	atomic.AddUint64(t.totalUuidsProcessed, count)
}

func (t *TableInfoChan) IncrementTotalRecordsRead(count uint64) {
	atomic.AddUint64(t.totalRecordsRead, count)
}

func (t *TableInfoChan) IncrementTotalRecordsProcessed(count uint64) {
	atomic.AddUint64(t.totalRecordsProcessed, count)
}

func (t *TableInfoChan) GetTotalUuidsRead() uint64 {
	return atomic.LoadUint64(t.totalUuidsRead)
}

func (t *TableInfoChan) GetTotalUuidsProcessed() uint64 {
	return atomic.LoadUint64(t.totalUuidsProcessed)
}

func (t *TableInfoChan) GetTotalRecordsRead() uint64 {
	return atomic.LoadUint64(t.totalRecordsRead)
}

func (t *TableInfoChan) GetTotalRecordsProcessed() uint64 {
	return atomic.LoadUint64(t.totalRecordsProcessed)
}
