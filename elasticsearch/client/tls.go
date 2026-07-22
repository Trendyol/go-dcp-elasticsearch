package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
)

func buildTLSConfig(cfg *config.TLS) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.SkipVerify,
	}

	if cfg.CaCertPath != "" {
		caCert, err := os.ReadFile(cfg.CaCertPath)
		if err != nil {
			return nil, fmt.Errorf("read CA cert from path '%s': %w", cfg.CaCertPath, err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA cert")
		}
		tlsConfig.RootCAs = caPool
	}

	if cfg.CertPath != "" && cfg.KeyPath != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func isZeroTLS(cfg *config.TLS) bool {
	if cfg == nil {
		return true
	}
	return !cfg.SkipVerify &&
		cfg.CaCertPath == "" &&
		cfg.CertPath == "" &&
		cfg.KeyPath == ""
}
