package config

import (
	"math"
	"testing"
)

func TestNormalizeElasticsearchClusterKeys_trimsAndValidates(t *testing.T) {
	c := &Config{}
	c.Elasticsearch.Clusters = map[string]Elasticsearch{
		" eu ": {Urls: []string{"http://localhost:9201"}},
	}
	if err := c.NormalizeElasticsearchClusterKeys(); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Elasticsearch.Clusters["eu"]; !ok {
		t.Fatalf("expected normalized key eu, got %#v", c.Elasticsearch.Clusters)
	}
}

func TestNormalizeElasticsearchClusterKeys_reservedName(t *testing.T) {
	c := &Config{}
	c.Elasticsearch.Clusters = map[string]Elasticsearch{
		"default": {Urls: []string{"http://localhost:9201"}},
	}
	if err := c.NormalizeElasticsearchClusterKeys(); err == nil {
		t.Fatal("expected error for reserved cluster name")
	}
}

func TestNormalizeElasticsearchClusterKeys_duplicateAfterNormalize(t *testing.T) {
	c := &Config{}
	c.Elasticsearch.Clusters = map[string]Elasticsearch{
		" x ": {Urls: []string{"http://a:9200"}},
		"x":   {Urls: []string{"http://b:9200"}},
	}
	if err := c.NormalizeElasticsearchClusterKeys(); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestApplyElasticsearchDefaults_clustersBlocks(t *testing.T) {
	c := &Config{}
	c.Elasticsearch.Clusters = map[string]Elasticsearch{
		"eu": {Urls: []string{"http://localhost:9201"}},
	}
	c.ApplyDefaults()
	if c.Elasticsearch.Clusters["eu"].MaxRetries != math.MaxInt {
		t.Fatalf("cluster block should get MaxRetries default, got %d", c.Elasticsearch.Clusters["eu"].MaxRetries)
	}
}
