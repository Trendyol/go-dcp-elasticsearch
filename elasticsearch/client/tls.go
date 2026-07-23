package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
)

type resolvedTLSConfig struct {
	skipVerify bool
	caCert     []byte
	cert       []byte
	key        []byte
}

func resolveTLS(cfg *config.TLS) (*resolvedTLSConfig, error) {
	resolvedTLSCfg := new(resolvedTLSConfig)

	if cfg.CaCertPath != "" {
		caCert, err := os.ReadFile(cfg.CaCertPath)
		if err != nil {
			return nil, fmt.Errorf("read CA cert from path '%s': %w", cfg.CaCertPath, err)
		}
		resolvedTLSCfg.caCert = caCert
	}

	if cfg.CertPath != "" {
		cert, err := os.ReadFile(cfg.CertPath)
		if err != nil {
			return nil, fmt.Errorf("read client cert from path '%s': %w", cfg.CertPath, err)
		}
		resolvedTLSCfg.cert = cert
	}

	if cfg.KeyPath != "" {
		key, err := os.ReadFile(cfg.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("read client key from path '%s': %w", cfg.KeyPath, err)
		}
		resolvedTLSCfg.key = key
	}

	return resolvedTLSCfg, nil
}

func buildTLSConfig(cfg *resolvedTLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.skipVerify,
	}

	if len(cfg.caCert) > 0 {
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(cfg.caCert) {
			return nil, fmt.Errorf("failed to append CA cert")
		}
		tlsConfig.RootCAs = caPool
	}

	if len(cfg.cert) > 0 && len(cfg.key) > 0 {
		cert, err := tls.X509KeyPair(cfg.cert, cfg.key)
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
