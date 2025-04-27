package kafka

type Kafka struct {
	ConnectionDetails ConnectionDetails `json:"connection_details"`
	Configuration     Configuration     `json:"configuration"`
}

type Configuration struct {
	Pool                int    `json:"pool"`
	PoolSize            int    `json:"pool_size"`
	Topic               string `json:"topic"`
	BatchSize           int    `json:"batch_size"`
	MaxOpenRequests     int    `json:"max_open_requests"`
	ChannelBufferSize   int    `json:"channel_buffer_size"`
	FlushBytes          int    `json:"flush_bytes"`
	FlushMessages       int    `json:"flush_messages"`
	FlushFrequencyMs    int    `json:"flush_frequency_ms"`
	MaxMessageBytes     int    `json:"max_message_bytes"`
	CompressionEnabled  bool   `json:"compression_enabled"`
	CompressionType     string `json:"compression_type"`
	RetryMax            int    `json:"retry_max"`
	RequiredAcks        string `json:"required_acks"`
}
