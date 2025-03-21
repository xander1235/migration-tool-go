package postgres

type Configuration struct {
	Schemas               []string                `json:"schemas"`
	ExcludedSchemas       []string                `json:"excluded_schemas"`
	ExcludeTableRegexList []ExcludeTableRegexList `json:"exclude_table_regex_list"`
	ExcludeTablesList     []ExcludeTablesList     `json:"exclude_tables_list"`
	Pool                  uint                    `json:"pool"`
}

type ExcludeTableRegexList struct {
	Schema string   `json:"schema"`
	Regex  []string `json:"regex"`
}

type ExcludeTablesList struct {
	Schema string   `json:"schema"`
	Tables []string `json:"tables"`
}
