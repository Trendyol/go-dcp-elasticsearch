package client

import (
	"testing"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
)

func TestNewTransport_MaxIdemponentCallAttempts(t *testing.T) {
	t.Run("zero defaults to one", func(t *testing.T) {
		tr, err := newTransport(config.Elasticsearch{})
		if err != nil {
			t.Fatalf("newTransport: %v", err)
		}
		if tr.client.MaxIdemponentCallAttempts != 1 {
			t.Fatalf("expected MaxIdemponentCallAttempts 1, got %d", tr.client.MaxIdemponentCallAttempts)
		}
	})

	t.Run("explicit value", func(t *testing.T) {
		tr, err := newTransport(config.Elasticsearch{MaxIdemponentCallAttempts: 3})
		if err != nil {
			t.Fatalf("newTransport: %v", err)
		}
		if tr.client.MaxIdemponentCallAttempts != 3 {
			t.Fatalf("expected MaxIdemponentCallAttempts 3, got %d", tr.client.MaxIdemponentCallAttempts)
		}
	})
}
