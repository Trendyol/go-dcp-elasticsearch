package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-client/config"
)

func TestApplyDefaults(t *testing.T) {
	// given
	tests := []struct {
		name     string
		config   Config
		expected Config
	}{
		{
			name: "Default values not set",
			config: Config{
				Elasticsearch: Elasticsearch{},
				Dcp:           config.Dcp{},
			},
			expected: Config{
				Elasticsearch: Elasticsearch{
					BatchTickerDuration: 10 * time.Second,
					BatchSizeLimit:      1000,
					BatchByteSizeLimit:  10485760,
				},
				Dcp: config.Dcp{},
			},
		},
		{
			name: "Default values already set",
			config: Config{
				Elasticsearch: Elasticsearch{
					BatchTickerDuration: 20 * time.Second,
					BatchSizeLimit:      2000,
					BatchByteSizeLimit:  20971520,
				},
				Dcp: config.Dcp{},
			},
			expected: Config{
				Elasticsearch: Elasticsearch{
					BatchTickerDuration: 20 * time.Second,
					BatchSizeLimit:      2000,
					BatchByteSizeLimit:  20971520,
				},
				Dcp: config.Dcp{},
			},
		},
	}

	// when & then
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := test.config
			c.ApplyDefaults()
			if !reflect.DeepEqual(c, test.expected) {
				t.Errorf("ApplyDefaults() = %v, expected %v", c, test.expected)
			}
		})
	}
}
