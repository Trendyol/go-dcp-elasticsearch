package config

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/Trendyol/go-dcp/config"
)

func NormalizeClusterKey(clusterKey string) string {
	s := strings.TrimSpace(clusterKey)
	if strings.EqualFold(s, "default") {
		return ""
	}
	return s
}

type Elasticsearch struct {
	BatchByteSizeLimit          any                      `yaml:"batchByteSizeLimit"`
	BatchCommitTickerDuration   *time.Duration           `yaml:"batchCommitTickerDuration"`
	CollectionIndexMapping      map[string]string        `yaml:"collectionIndexMapping"`
	MaxConnsPerHost             *int                     `yaml:"maxConnsPerHost"`
	MaxIdleConnDuration         *time.Duration           `yaml:"maxIdleConnDuration"`
	DiscoverNodesInterval       *time.Duration           `yaml:"discoverNodesInterval"`
	Retry                       *Retry                   `yaml:"retry"`
	TypeName                    string                   `yaml:"typeName"`
	Password                    string                   `yaml:"password"`
	Username                    string                   `yaml:"username"`
	RejectionLog                RejectionLog             `yaml:"rejectionLog"`
	Urls                        []string                 `yaml:"urls"`
	BatchSizeLimit              int                      `yaml:"batchSizeLimit"`
	BatchTickerDuration         time.Duration            `yaml:"batchTickerDuration"`
	ConcurrentRequest           int                      `yaml:"concurrentRequest"`
	CompressionEnabled          bool                     `yaml:"compressionEnabled"`
	DisableDiscoverNodesOnStart bool                     `yaml:"disableDiscoverNodesOnStart"`
	MaxRetries                  int                      `yaml:"maxRetries"`
	Clusters                    map[string]Elasticsearch `yaml:"clusters"`
}

// Retry configures an optional layer that re-submits only the retryable items
// of a failed bulk request (transient statuses / connection errors) with
// exponential backoff before falling through to OnError / panic. Disabled by
// default. Each cluster may define its own block; named clusters that omit it
// inherit the default cluster's retry settings.
//
// Backoff sleeps happen while the flush lock is held, so a single flush can be
// delayed by up to MaxInterval*MaxRetries when a cluster is unhealthy; size
// these values with that latency ceiling in mind.
type Retry struct {
	RetryOnStatus   []int         `yaml:"retryOnStatus"`
	MaxRetries      int           `yaml:"maxRetries"`
	InitialInterval time.Duration `yaml:"initialInterval"`
	MaxInterval     time.Duration `yaml:"maxInterval"`
	Enabled         bool          `yaml:"enabled"`
}

type RejectionLog struct {
	Index         string `yaml:"index"`
	IncludeSource bool   `yaml:"includeSource"`
	TargetCluster string `yaml:"targetCluster"`
}

type Config struct {
	Elasticsearch Elasticsearch `yaml:"elasticsearch" mapstructure:"elasticsearch"`
	Dcp           config.Dcp    `yaml:",inline" mapstructure:",squash"`
}

func ApplyElasticsearchDefaults(es *Elasticsearch) {
	if es.BatchTickerDuration == 0 {
		es.BatchTickerDuration = 10 * time.Second
	}

	if es.BatchSizeLimit == 0 {
		es.BatchSizeLimit = 1000
	}

	if es.BatchByteSizeLimit == nil {
		es.BatchByteSizeLimit = helpers.ResolveUnionIntOrStringValue("10mb")
	}

	if es.ConcurrentRequest == 0 {
		es.ConcurrentRequest = 1
	}

	if es.DiscoverNodesInterval == nil {
		duration := 5 * time.Minute
		es.DiscoverNodesInterval = &duration
	}

	if es.MaxRetries == 0 {
		es.MaxRetries = math.MaxInt
	}

	if es.Retry != nil && es.Retry.Enabled {
		ApplyRetryDefaults(es.Retry)
	}
}

func ApplyRetryDefaults(r *Retry) {
	if r.MaxRetries == 0 {
		r.MaxRetries = 3
	}

	if len(r.RetryOnStatus) == 0 {
		r.RetryOnStatus = []int{429, 502, 503, 504}
	}

	if r.InitialInterval == 0 {
		r.InitialInterval = 200 * time.Millisecond
	}

	if r.MaxInterval == 0 {
		r.MaxInterval = 5 * time.Second
	}
}

func (c *Config) NormalizeElasticsearchClusterKeys() error {
	if len(c.Elasticsearch.Clusters) == 0 {
		return nil
	}
	out := make(map[string]Elasticsearch, len(c.Elasticsearch.Clusters))
	for name, block := range c.Elasticsearch.Clusters {
		nk := NormalizeClusterKey(name)
		if nk == "" {
			return fmt.Errorf("elasticsearch.clusters: invalid or reserved cluster name %q", name)
		}
		if _, dup := out[nk]; dup {
			return fmt.Errorf("elasticsearch.clusters: duplicate cluster key after normalization %q", nk)
		}
		out[nk] = block
	}
	c.Elasticsearch.Clusters = out
	return nil
}

func (c *Config) ApplyDefaults() {
	ApplyElasticsearchDefaults(&c.Elasticsearch)

	for name := range c.Elasticsearch.Clusters {
		block := c.Elasticsearch.Clusters[name]
		// Named clusters that omit a retry block inherit the default cluster's
		// resolved retry settings. Copy the struct (not the pointer) so the two
		// clusters don't share one *Retry instance.
		if block.Retry == nil && c.Elasticsearch.Retry != nil {
			inherited := *c.Elasticsearch.Retry
			block.Retry = &inherited
		}
		ApplyElasticsearchDefaults(&block)
		c.Elasticsearch.Clusters[name] = block
	}
}
