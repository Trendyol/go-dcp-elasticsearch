package client

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
)

func generateTestCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create test certificate: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	pKeyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal test private key: %v", err)
	}

	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pKeyBytes})

	return certPEM, keyPEM
}

func TestBuildTLSConfig(t *testing.T) {
	certPEM, keyPEM := generateTestCert(t)

	tests := []struct {
		name    string
		cfg     *resolvedTLSConfig
		wantErr bool
		check   func(t *testing.T, c *tls.Config)
	}{
		{
			name: "skip verify only",
			cfg:  &resolvedTLSConfig{skipVerify: true},
			check: func(t *testing.T, c *tls.Config) {
				if !c.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify=true")
				}
			},
		},
		{
			name: "ca cert only",
			cfg:  &resolvedTLSConfig{caCert: certPEM},
			check: func(t *testing.T, c *tls.Config) {
				if c.RootCAs == nil {
					t.Error("expected non-nil RootCAs")
				}
			},
		},
		{
			name: "client cert and key",
			cfg:  &resolvedTLSConfig{cert: certPEM, key: keyPEM},
			check: func(t *testing.T, c *tls.Config) {
				if len(c.Certificates) != 1 {
					t.Errorf("expected 1 certificate, got %d", len(c.Certificates))
				}
			},
		},
		{
			name:    "invalid ca cert bytes",
			cfg:     &resolvedTLSConfig{caCert: []byte("not a valid pem")},
			wantErr: true,
		},
		{
			name:    "invalid cert key pair",
			cfg:     &resolvedTLSConfig{cert: []byte("invalid"), key: []byte("invalid")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tlsCfg, err := buildTLSConfig(tt.cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, tlsCfg)
			}
		})
	}
}

func TestIsZeroTLS(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.TLS
		want bool
	}{
		{"nil config", nil, true},
		{"empty config", &config.TLS{}, true},
		{"skip verify only", &config.TLS{SkipVerify: true}, false},
		{"ca cert path only", &config.TLS{CaCertPath: "/path/ca.crt"}, false},
		{"cert path only", &config.TLS{CertPath: "/path/client.crt"}, false},
		{"key path only", &config.TLS{KeyPath: "/path/client.key"}, false},
		{
			name: "all fields set",
			cfg: &config.TLS{
				SkipVerify: true,
				CaCertPath: "/path/ca.crt",
				CertPath:   "/path/client.crt",
				KeyPath:    "/path/client.key",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isZeroTLS(tt.cfg)
			if got != tt.want {
				t.Errorf("isZeroTLS() = %v, want %v", got, tt.want)
			}
		})
	}
}
