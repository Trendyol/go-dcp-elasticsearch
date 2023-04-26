package config

import (
	"time"

	"github.com/Trendyol/go-elasticsearch-connect-couchbase/logger"

	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
)

type Elasticsearch struct {
	CollectionIndexMapping map[string]string `yaml:"collectionIndexMapping"`
	MaxConnsPerHost        *int              `yaml:"maxConnsPerHost"`
	MaxIdleConnDuration    *time.Duration    `yaml:"maxIdleConnDuration"`
	TypeName               string            `yaml:"typeName" default:"_doc"`
	Urls                   []string          `yaml:"urls"`
	BatchSizeLimit         int               `yaml:"batchSizeLimit" default:"1000"`
	BatchByteSizeLimit     int               `yaml:"batchByteSizeLimit" default:"10485760"`
	BatchTickerDuration    time.Duration     `yaml:"batchTickerDuration"`
}

type Config struct {
	Elasticsearch Elasticsearch `yaml:"elasticsearch"`
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
	opts.ParseDefault = true
}

func applyUnhandledDefaults(_config *Config) {
	if _config.Elasticsearch.BatchTickerDuration == 0 {
		_config.Elasticsearch.BatchTickerDuration = 10 * time.Second
	}
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

	applyUnhandledDefaults(_config)

	return _config
}
