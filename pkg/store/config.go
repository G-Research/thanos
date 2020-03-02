package store

import (
	http_util "github.com/thanos-io/thanos/pkg/http"
	"sigs.k8s.io/yaml"
)

type Config struct {
	TlsConfig *TlsConfig    `yaml:"tls_config"`
	EndpointsConfig  EndpointsConfig `yaml:",inline"`
}

type TlsConfig struct {
	Cert string
	Key string
	CaCert string
	ServerName string
}

type EndpointsConfig struct {
	// List of addresses with DNS prefixes.
	StaticAddresses []string `yaml:"static_configs"`
	// List of file  configurations (our FileSD supports different DNS lookups).
	FileSDConfigs []http_util.FileSDConfig `yaml:"file_sd_configs"`
}

func DefaultConfig() Config {
	return Config{
		EndpointsConfig: EndpointsConfig{
			StaticAddresses: []string{},
			FileSDConfigs:   []http_util.FileSDConfig{},
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
func LoadConfigs(confYAML []byte) ([]Config, error) {
	var queryCfg []Config
	if err := yaml.UnmarshalStrict(confYAML, &queryCfg); err != nil {
		return nil, err
	}
	return queryCfg, nil
}
