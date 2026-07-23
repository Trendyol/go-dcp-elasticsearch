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
		cfg     *config.TLS
		wantErr bool
		check   func(t *testing.T, c *tls.Config)
	}{
		{
			name: "skip verify only",
			cfg:  &config.TLS{SkipVerify: true},
			check: func(t *testing.T, c *tls.Config) {
				if !c.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify=true")
				}
			},
		},
		{
			name: "ca cert only",
			cfg:  &config.TLS{CaCert: certPEM},
			check: func(t *testing.T, c *tls.Config) {
				if c.RootCAs == nil {
					t.Error("expected non-nil RootCAs")
				}
			},
		},
		{
			name: "client cert and key",
			cfg:  &config.TLS{Cert: certPEM, Key: keyPEM},
			check: func(t *testing.T, c *tls.Config) {
				if len(c.Certificates) != 1 {
					t.Errorf("expected 1 certificate, got %d", len(c.Certificates))
				}
			},
		},
		{
			name: "full config",
			cfg: &config.TLS{
				SkipVerify: false,
				CaCert:     certPEM,
				Cert:       certPEM,
				Key:        keyPEM,
			},
			check: func(t *testing.T, c *tls.Config) {
				if c.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify=true")
				}
				if c.RootCAs == nil {
					t.Error("expected non-nil RootCAs")
				}
				if len(c.Certificates) != 1 {
					t.Errorf("expected 1 certificate, got %d", len(c.Certificates))
				}
			},
		},
		{
			name:    "invalid ca cert bytes",
			cfg:     &config.TLS{CaCert: []byte("not a valid pem")},
			wantErr: true,
		},
		{
			name:    "invalid cert key pair",
			cfg:     &config.TLS{Cert: []byte("invalid"), Key: []byte("invalid")},
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
	certPEM, keyPEM := generateTestCert(t)
	tests := []struct {
		name string
		cfg  *config.TLS
		want bool
	}{
		{"nil config", nil, true},
		{"empty config", &config.TLS{}, true},
		{"skip verify only", &config.TLS{SkipVerify: true}, false},
		{"ca cert only", &config.TLS{CaCert: certPEM}, false},
		{"cert only", &config.TLS{Cert: certPEM}, false},
		{"key only", &config.TLS{Key: keyPEM}, false},
		{"cert and key", &config.TLS{Cert: certPEM, Key: keyPEM}, false},
		{
			name: "all fields set",
			cfg: &config.TLS{
				SkipVerify: true,
				CaCert:     certPEM,
				Cert:       certPEM,
				Key:        certPEM,
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
