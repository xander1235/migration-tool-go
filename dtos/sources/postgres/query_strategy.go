package postgres

// QueryStrategyType defines the type of strategy to use for querying data
type QueryStrategyType string

const (
	// RangeStrategy divides the data into ranges based on a specified column
	RangeStrategy QueryStrategyType = "range"
	
	// FixedValuesStrategy uses fixed values for querying data
	FixedValuesStrategy QueryStrategyType = "fixed_values"
	
	// TimeWindowStrategy divides the data into time windows
	TimeWindowStrategy QueryStrategyType = "time_window"
	
	// BatchSizeStrategy uses a simple batch size approach
	BatchSizeStrategy QueryStrategyType = "batch_size"
)

// QueryStrategy defines how to query data from a table
type QueryStrategy struct {
	// Type of strategy to use
	Type QueryStrategyType `json:"type"`
	
	// Column to use for the strategy (required for all strategies)
	Column string `json:"column"`
	
	// Parameters specific to each strategy type
	RangeParams         *RangeStrategyParams         `json:"range_params,omitempty"`
	FixedValuesParams   *FixedValuesStrategyParams   `json:"fixed_values_params,omitempty"`
	TimeWindowParams    *TimeWindowStrategyParams    `json:"time_window_params,omitempty"`
	BatchSizeParams     *BatchSizeStrategyParams     `json:"batch_size_params,omitempty"`
}

// RangeStrategyParams contains parameters for the range strategy
type RangeStrategyParams struct {
	// Frequency defines the step size for the range
	Frequency any `json:"frequency"`
	
	// Min is the minimum value to start from (optional)
	Min any `json:"min,omitempty"`
	
	// Max is the maximum value to end at (optional)
	Max any `json:"max,omitempty"`
}

// FixedValuesStrategyParams contains parameters for the fixed values strategy
type FixedValuesStrategyParams struct {
	// Values is a list of fixed values to use for querying
	Values []any `json:"values"`
	
	// BatchSize is the number of records to fetch per batch
	BatchSize int `json:"batch_size"`
}

// TimeWindowStrategyParams contains parameters for the time window strategy
type TimeWindowStrategyParams struct {
	// WindowSize is the size of each time window (e.g., "1h", "1d", "1w", "1m")
	WindowSize string `json:"window_size"`
	
	// StartTime is the start time for the first window (optional)
	StartTime string `json:"start_time,omitempty"`
	
	// EndTime is the end time for the last window (optional)
	EndTime string `json:"end_time,omitempty"`
}

// BatchSizeStrategyParams contains parameters for the batch size strategy
type BatchSizeStrategyParams struct {
	// BatchSize is the number of records to fetch per batch
	BatchSize int `json:"batch_size"`
	
	// OrderBy is the column to order by (defaults to the primary key)
	OrderBy string `json:"order_by,omitempty"`
}
