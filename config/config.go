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
		ApplyElasticsearchDefaults(&block)
		c.Elasticsearch.Clusters[name] = block
	}
}
