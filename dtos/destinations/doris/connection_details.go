package doris

type ConnectionDetails struct {
	FeNodes  string `json:"fe_nodes"`
	FePort   int    `json:"fe_port"`
	BeNodes  string `json:"be_nodes"`
	BePort   int    `json:"be_port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}
