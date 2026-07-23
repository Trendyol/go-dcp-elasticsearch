package client

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
)

// newTestES builds a minimal config.Elasticsearch pointing at addr with node
// discovery disabled so the only requests hitting the server come from the bulk
// call under test.
func newTestES(addr string, retry *config.Retry) *config.Elasticsearch {
	interval := time.Minute
	return &config.Elasticsearch{
		Urls:                        []string{addr},
		DisableDiscoverNodesOnStart: true,
		DiscoverNodesInterval:       &interval,
		Retry:                       retry,
	}
}

// TestClientRetryShadowing guards against the client's built-in retry shadowing
// the bulk retry layer. With retry enabled the client must not retry a whole
// response 503 itself (that is the new layer's job); with retry disabled the
// legacy behavior — the client retries 5xx — must be preserved.
func TestClientRetryShadowing(t *testing.T) {
	// serve503 counts bulk hits and always answers 503. The X-Elastic-Product
	// header lets the v7 client's product check pass so Bulk returns the response
	// (rather than a product-check error) and the hit count reflects only retries.
	serve503 := func(hits *int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.Header().Set("Content-Type", "application/json")
			// Non-bulk requests (the v7 client's product/info check) must succeed,
			// otherwise Bulk fails the product check before ever hitting _bulk.
			if !strings.Contains(r.URL.Path, "_bulk") {
				_, _ = w.Write([]byte(`{}`))
				return
			}
			atomic.AddInt32(hits, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{}`))
		}))
	}

	t.Run("retry enabled disables client retry", func(t *testing.T) {
		var hits int32
		srv := serve503(&hits)
		defer srv.Close()

		esClient, err := NewElasticClientFromElasticsearch(
			newTestES(srv.URL, &config.Retry{Enabled: true, MaxRetries: 3}),
		)
		if err != nil {
			t.Fatalf("build client: %v", err)
		}

		res, err := esClient.Bulk(strings.NewReader("{}\n"))
		if err != nil {
			t.Fatalf("bulk: %v", err)
		}
		res.Body.Close()

		if got := atomic.LoadInt32(&hits); got != 1 {
			t.Fatalf("client retried a 503 while retry layer is enabled: bulk hits = %d, want 1", got)
		}
	})

	t.Run("retry disabled preserves client retry", func(t *testing.T) {
		var hits int32
		srv := serve503(&hits)
		defer srv.Close()

		// MaxRetries left at 0 so the ES transport falls back to its own default
		// (3 retries => 4 attempts), keeping this bounded instead of math.MaxInt.
		esClient, err := NewElasticClientFromElasticsearch(newTestES(srv.URL, nil))
		if err != nil {
			t.Fatalf("build client: %v", err)
		}

		res, err := esClient.Bulk(strings.NewReader("{}\n"))
		if err != nil {
			t.Fatalf("bulk: %v", err)
		}
		res.Body.Close()

		if got := atomic.LoadInt32(&hits); got <= 1 {
			t.Fatalf("client did not retry a 503 with retry disabled: bulk hits = %d, want > 1", got)
		}
	})
}
