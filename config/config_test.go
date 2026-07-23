package config

import (
	"testing"
	"time"
)

func Test_ApplyDefaults_RetryDefaultsOnlyWhenEnabled(t *testing.T) {
	t.Run("nil_retry_stays_nil_on_default_cluster", func(t *testing.T) {
		c := &Config{Elasticsearch: Elasticsearch{Urls: []string{"http://localhost:9200"}}}
		c.ApplyDefaults()
		if c.Elasticsearch.Retry != nil {
			t.Fatalf("expected nil retry, got %+v", c.Elasticsearch.Retry)
		}
	})

	t.Run("disabled_retry_keeps_zero_values", func(t *testing.T) {
		c := &Config{Elasticsearch: Elasticsearch{
			Urls:  []string{"http://localhost:9200"},
			Retry: &Retry{Enabled: false, MaxRetries: 0},
		}}
		c.ApplyDefaults()
		if c.Elasticsearch.Retry.MaxRetries != 0 {
			t.Fatalf("disabled retry must not be defaulted, got MaxRetries=%d", c.Elasticsearch.Retry.MaxRetries)
		}
		if len(c.Elasticsearch.Retry.RetryOnStatus) != 0 {
			t.Fatalf("disabled retry must not gain RetryOnStatus, got %v", c.Elasticsearch.Retry.RetryOnStatus)
		}
	})

	t.Run("enabled_retry_gets_defaults", func(t *testing.T) {
		c := &Config{Elasticsearch: Elasticsearch{
			Urls:  []string{"http://localhost:9200"},
			Retry: &Retry{Enabled: true},
		}}
		c.ApplyDefaults()
		r := c.Elasticsearch.Retry
		if r == nil {
			t.Fatal("expected retry to be set")
		}
		if r.MaxRetries != 3 {
			t.Fatalf("MaxRetries = %d, want 3", r.MaxRetries)
		}
		if r.InitialInterval != 200*time.Millisecond {
			t.Fatalf("InitialInterval = %v, want 200ms", r.InitialInterval)
		}
		if r.MaxInterval != 5*time.Second {
			t.Fatalf("MaxInterval = %v, want 5s", r.MaxInterval)
		}
		if len(r.RetryOnStatus) != 4 {
			t.Fatalf("RetryOnStatus = %v, want [429 502 503 504]", r.RetryOnStatus)
		}
	})
}

func Test_ApplyDefaults_ClusterInheritsRetry(t *testing.T) {
	c := &Config{Elasticsearch: Elasticsearch{
		Urls:  []string{"http://localhost:9200"},
		Retry: &Retry{Enabled: true, MaxRetries: 7},
		Clusters: map[string]Elasticsearch{
			"analytics": {Urls: []string{"http://localhost:9201"}}, // no retry -> inherits
			"reporting": {Urls: []string{"http://localhost:9202"}, Retry: &Retry{Enabled: true, MaxRetries: 9}},
		},
	}}
	c.ApplyDefaults()

	analytics := c.Elasticsearch.Clusters["analytics"]
	if analytics.Retry == nil || analytics.Retry.MaxRetries != 7 {
		t.Fatalf("analytics must inherit default retry (MaxRetries=7), got %+v", analytics.Retry)
	}

	reporting := c.Elasticsearch.Clusters["reporting"]
	if reporting.Retry == nil || reporting.Retry.MaxRetries != 9 {
		t.Fatalf("reporting must keep its own retry (MaxRetries=9), got %+v", reporting.Retry)
	}
	// explicit block still gets defaults filled for unset fields
	if len(reporting.Retry.RetryOnStatus) != 4 {
		t.Fatalf("reporting retry defaults not applied: %v", reporting.Retry.RetryOnStatus)
	}
}

func Test_ApplyDefaults_ClusterNoInheritWhenDefaultRetryNil(t *testing.T) {
	c := &Config{Elasticsearch: Elasticsearch{
		Urls: []string{"http://localhost:9200"},
		Clusters: map[string]Elasticsearch{
			"analytics": {Urls: []string{"http://localhost:9201"}},
		},
	}}
	c.ApplyDefaults()

	if c.Elasticsearch.Clusters["analytics"].Retry != nil {
		t.Fatal("cluster must not gain retry when default cluster has none")
	}
}
