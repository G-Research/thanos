package store

import (
	"github.com/prometheus/prometheus/discovery/file"
	"sigs.k8s.io/yaml"
)

type Config struct {
	TlsConfig *TlsConfig    `yaml:"tls_config"`
	EndpointsConfig  EndpointsConfig `yaml:",inline"`
}

type TlsConfig struct {
	// TLS Certificates to use to identify this client to the server
	Cert string `yaml:"cert_file"`
	// TLS Key for the client's certificate
	Key string `yaml:"key_file"`
	// TLS CA Certificates to use to verify gRPC servers
	CaCert string `yaml:"ca_file"`
	// Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1
	ServerName string `yaml:"server_name"`
}

type EndpointsConfig struct {
	// List of addresses with DNS prefixes.
	StaticAddresses []string `yaml:"static_configs"`
	// List of file  configurations (our FileSD supports different DNS lookups).
	FileSDConfigs []file.SDConfig `yaml:"file_sd_configs"`
}



func DefaultConfig() Config {
	return Config{
		EndpointsConfig: EndpointsConfig{
			StaticAddresses: []string{},
			FileSDConfigs:   []file.SDConfig{},
		},
	}
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig()
	type plain Config
	return unmarshal((*plain)(c))
}

// LoadConfigs loads a list of Config from YAML data.
func LoadConfig(confYAML []byte) ([]Config, error) {
	var queryCfg []Config
	if err := yaml.UnmarshalStrict(confYAML, queryCfg); err != nil {
		return nil, err
	}
	return queryCfg, nil
}
