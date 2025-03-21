package common

import "time"

// StatsConfiguration holds settings for the system statistics collector
type StatsConfiguration struct {
	Enabled        bool   `json:"enabled"`
	IntervalSeconds int    `json:"interval_seconds"`
	OutputFile     string `json:"output_file"`
}

// GetInterval returns the interval as a time.Duration
func (s *StatsConfiguration) GetInterval() time.Duration {
	if s.IntervalSeconds <= 0 {
		return 30 * time.Second // Default to 30 seconds
	}
	return time.Duration(s.IntervalSeconds) * time.Second
}
