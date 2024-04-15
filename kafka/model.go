package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/IBM/sarama"
	"os"
)

type (
	ProducerConfig struct {
		Config `json:"config" yaml:"config" mapstructure:"config"`
		Async  bool `yaml:"async" json:"async" mapstructure:"async"`
	}

	ConsumerConfig struct {
		Config   `json:"config" yaml:"config" mapstructure:"config"`
		Topics   string `json:"topics" yaml:"topics" mapstructure:"topics"`
		Group    string `json:"group" yaml:"group" mapstructure:"group"`
		Strategy string `yaml:"strategy" json:"strategy" mapstructure:"strategy"`
		Offset   Offset `json:"offset" yaml:"offset" mapstructure:"offset"`
	}

	Config struct {
		Brokers string     `json:"brokers" yaml:"brokers" mapstructure:"brokers"`
		Auth    AuthConfig `json:"auth" json:"auth" mapstructure:"auth"`
		TLS     TLSConfig  `json:"tls" json:"tls" mapstructure:"tls"`
		Version string     `yaml:"version" json:"version" mapstructure:"version"`
	}

	AuthConfig struct {
		Enable    bool          `json:"enable" yaml:"enable" mapstructure:"enable"`
		Mechanism SASLMechanism `yaml:"mechanism" json:"mechanism" mapstructure:"mechanism"`
		Username  string        `json:"username" yaml:"username" mapstructure:"username"`
		Password  string        `yaml:"password" json:"password" mapstructure:"password"`
	}

	TLSConfig struct {
		Enable             bool   `json:"enable" yaml:"enable" mapstructure:"enable"`
		InsecureSkipVerify bool   `yaml:"insecure_skip_verify" json:"insecure_skip_verify" mapstructure:"insecure_skip_verify"`
		CertFile           string `json:"cert_file" json:"cert_file" mapstructure:"cert_file"`
		KeyFile            string `json:"key_file" json:"key_file" mapstructure:"key_file"`
		CAFile             string `yaml:"ca_file" json:"ca_file" mapstructure:"ca_file"`
	}

	Offset        string
	SASLMechanism string
)

func (conf Config) parseVersion() (sarama.KafkaVersion, error) {
	return sarama.ParseKafkaVersion(conf.Version)
}

func (conf TLSConfig) createTLSConfiguration() (*tls.Config, error) {
	if !conf.Enable {
		return nil, nil
	}
	t := &tls.Config{
		InsecureSkipVerify: conf.InsecureSkipVerify,
	}
	if conf.CertFile != "" && conf.KeyFile != "" && conf.CAFile != "" {
		cert, err := tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			return nil, err
		}
		caCert, err := os.ReadFile(conf.CAFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		t.Certificates = []tls.Certificate{cert}
		t.RootCAs = caCertPool
	}
	return t, nil
}
