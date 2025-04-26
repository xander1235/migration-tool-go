package postgres

type Configuration struct {
	Schemas               []string         `json:"schemas"`
	ExcludedSchemas       []string         `json:"excluded_schemas"`
	ExcludeTableRegexList []TableRegexList `json:"exclude_table_regex_list"`
	ExcludeTablesList     []TablesList     `json:"exclude_tables_list"`
	IncludeTableRegexList []TableRegexList `json:"include_table_regex_list"`
	IncludeTablesList     []TablesList     `json:"include_tables_list"`
	Pool                  uint             `json:"pool"`
}

type TableRegexList struct {
	Schema        string        `json:"schema"`
	Regex         []string      `json:"regex"`
	QueryStrategy *QueryStrategy `json:"query_strategy,omitempty"`
}

type TablesList struct {
	Schema        string        `json:"schema"`
	Tables        []string      `json:"tables"`
	QueryStrategy *QueryStrategy `json:"query_strategy,omitempty"`
}
