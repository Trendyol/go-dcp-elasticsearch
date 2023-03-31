package config

import (
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/logger"
	"time"

	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
)

type Elasticsearch struct {
	CollectionIndexMapping map[string]string `yaml:"collectionIndexMapping"`
	TypeName               string            `yaml:"typeName"`
	Urls                   []string          `yaml:"urls"`
	BulkSize               int               `yaml:"bulkSize"`
	BulkTickerDuration     time.Duration     `yaml:"bulkTickerDuration"`
}

type Config struct {
	Elasticsearch *Elasticsearch `yaml:"elasticsearch"`
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
}

func NewConfig(name string, filePath string, errorLogger logger.Logger) *Config {
	conf := config.New(name).WithOptions(Options).WithDriver(yamlv3.Driver)

	err := conf.LoadFiles(filePath)
	if err != nil {
		errorLogger.Printf("Error while reading config %v", err)
	}

	_config := &Config{}
	err = conf.Decode(_config)

	if err != nil {
		errorLogger.Printf("Error while reading config %v", err)
	}

	return _config
}
