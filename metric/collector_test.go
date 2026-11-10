package metric

import (
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/bulk"
	"github.com/Trendyol/go-dcp/helpers"
	esv7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/prometheus/client_golang/prometheus"
)

func TestCollector_BulkSizeMetrics(t *testing.T) {
	b, err := bulk.NewBulk(&config.Config{Elasticsearch: config.Elasticsearch{
		BatchTickerDuration: time.Hour,
	}}, func() {}, map[string]*esv7.Client{"": {}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewMetricCollector(b))
	for _, values := range [][2]int64{{0, 0}, {42, 1024}, {1, 64}} {
		b.LockMetrics()
		b.GetMetric().BulkRequestSize = values[0]
		b.GetMetric().BulkRequestByteSize = values[1]
		b.UnlockMetrics()

		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]float64{
			prometheus.BuildFQName(helpers.Name, "elasticsearch_connector_bulk_request_size", "current"):      float64(values[0]),
			prometheus.BuildFQName(helpers.Name, "elasticsearch_connector_bulk_request_byte_size", "current"): float64(values[1]),
		}
		for _, family := range families {
			value, ok := want[family.GetName()]
			if !ok {
				continue
			}
			metrics := family.GetMetric()
			if len(metrics) != 1 || metrics[0].Gauge == nil || metrics[0].GetGauge().GetValue() != value || len(metrics[0].GetLabel()) != 0 {
				t.Fatalf("unexpected metric for %s: %v", family.GetName(), metrics)
			}
			delete(want, family.GetName())
		}
		if len(want) != 0 {
			t.Fatalf("missing metrics: %v", want)
		}
	}
}
