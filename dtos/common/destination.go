package common

type Destination[T any] struct {
	Type  string `json:"type"`
	Value T      `json:"value"`
}
