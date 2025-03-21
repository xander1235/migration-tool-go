package doris

type Doris struct {
	ConnectionDetails ConnectionDetails `json:"connection_details"`
	Configuration     Configuration     `json:"configuration"`
}

type Configuration struct {
	Pool int `json:"pool"`
}
