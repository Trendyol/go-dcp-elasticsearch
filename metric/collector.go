package metric

import (
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch/bulk"
	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	bulk *bulk.Bulk

	elasticsearchConnectorLatency *prometheus.Desc
}

func (s *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *Collector) Collect(ch chan<- prometheus.Metric) {
	bulkMetric := s.bulk.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.elasticsearchConnectorLatency,
		prometheus.GaugeValue,
		bulkMetric.ESConnectorLatency.Value(),
		[]string{}...,
	)
}

func NewMetricCollector(bulk *bulk.Bulk) *Collector {
	return &Collector{
		bulk: bulk,

		elasticsearchConnectorLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "elasticsearch_connector_latency_ms", "current"),
			"Elasticsearch connector latency ms at 10sec windows",
			[]string{},
			nil,
		),
	}
}
