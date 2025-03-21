package common

type WorkerConfiguration struct {
	NoOfWorkers           int `json:"no_of_workers"`
	WorkerBatchSize       int `json:"worker_batch_size"`
	IdBatchSize           int `json:"id_batch_size"`
	ConcurrentTables      int `json:"concurrent_tables"`
	BatchProcessingTimeoutMs int `json:"batch_processing_timeout_ms"`
	RecordBatchSize       int `json:"record_batch_size"`
}
