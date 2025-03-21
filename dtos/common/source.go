package common

type Source[T any] struct {
	Type  string `json:"type"`
	Value T      `json:"value"`
}
