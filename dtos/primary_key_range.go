package dtos

type PrimaryKeyRange struct {
	Type          string            `json:"type"`
	IdRange       [2]any            `json:"id_range"`
	MultiKeyRange [2]map[string]any `json:"multi_key_range"`
}
