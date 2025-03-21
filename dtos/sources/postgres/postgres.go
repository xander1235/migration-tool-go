package postgres

type Postgres struct {
	ConnectionDetails ConnectionDetails `json:"connection_details"`
	Configuration     Configuration     `json:"configuration"`
}
