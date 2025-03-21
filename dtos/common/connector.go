package common

type Connector[T any, K any] struct {
	Source               Source[T]            `json:"source"`
	Destination          Destination[K]       `json:"destination"`
	WorkerConfiguration  WorkerConfiguration  `json:"worker_configuration"`
	TackingConfiguration TackingConfiguration `json:"tacking_configuration"`
}
