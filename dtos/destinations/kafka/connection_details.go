package kafka

type ConnectionDetails struct {
	Brokers  string `json:"brokers"`
	Username string `json:"username"`
	Password string `json:"password"`
	// Additional fields for SSL/SASL configuration
	UseSASL           bool   `json:"use_sasl"`
	UseTLS            bool   `json:"use_tls"`
	SASLMechanism     string `json:"sasl_mechanism"`
	TLSSkipVerify     bool   `json:"tls_skip_verify"`
	ClientCertFile    string `json:"client_cert_file"`
	ClientKeyFile     string `json:"client_key_file"`
	CACertFile        string `json:"ca_cert_file"`
	WarpStreamEnabled bool   `json:"warpstream_enabled"`
}
