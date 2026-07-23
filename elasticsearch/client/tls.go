package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
)

func buildTLSConfig(cfg *config.TLS) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.SkipVerify,
	}

	if len(cfg.CaCert) > 0 {
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(cfg.CaCert) {
			return nil, fmt.Errorf("failed to append CA cert")
		}
		tlsConfig.RootCAs = caPool
	}

	if len(cfg.Cert) > 0 && len(cfg.Key) > 0 {
		cert, err := tls.X509KeyPair(cfg.Cert, cfg.Key)
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
		len(cfg.CaCert) == 0 &&
		len(cfg.Cert) == 0 &&
		len(cfg.Key) == 0
}
