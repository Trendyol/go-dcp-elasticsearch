package metric

import (
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/bulk"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	bulk *bulk.Bulk

	processLatency            *prometheus.Desc
	bulkRequestProcessLatency *prometheus.Desc
	actionCounter             *prometheus.Desc
}

func (s *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *Collector) Collect(ch chan<- prometheus.Metric) {
	bulkMetric := s.bulk.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.processLatency,
		prometheus.GaugeValue,
		float64(bulkMetric.ProcessLatencyMs),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.bulkRequestProcessLatency,
		prometheus.GaugeValue,
		float64(bulkMetric.BulkRequestProcessLatencyMs),
		[]string{}...,
	)

	for indexName, count := range bulkMetric.IndexingSuccessActionCounter {
		ch <- prometheus.MustNewConstMetric(
			s.actionCounter,
			prometheus.CounterValue,
			float64(count),
			"index", "success", indexName,
		)
	}

	for indexName, count := range bulkMetric.IndexingErrorActionCounter {
		ch <- prometheus.MustNewConstMetric(
			s.actionCounter,
			prometheus.CounterValue,
			float64(count),
			"index", "error", indexName,
		)
	}

	for indexName, count := range bulkMetric.DeletionSuccessActionCounter {
		ch <- prometheus.MustNewConstMetric(
			s.actionCounter,
			prometheus.CounterValue,
			float64(count),
			"delete", "success", indexName,
		)
	}

	for indexName, count := range bulkMetric.DeletionErrorActionCounter {
		ch <- prometheus.MustNewConstMetric(
			s.actionCounter,
			prometheus.CounterValue,
			float64(count),
			"delete", "error", indexName,
		)
	}
}

func NewMetricCollector(bulk *bulk.Bulk) *Collector {
	return &Collector{
		bulk: bulk,

		processLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "elasticsearch_connector_latency_ms", "current"),
			"Elasticsearch connector latency ms",
			[]string{},
			nil,
		),

		bulkRequestProcessLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "elasticsearch_connector_bulk_request_process_latency_ms", "current"),
			"Elasticsearch connector bulk request process latency ms",
			[]string{},
			nil,
		),

		actionCounter: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "elasticsearch_connector_action_total", "current"),
			"Elasticsearch connector action counter",
			[]string{"action_type", "result", "index_name"},
			nil,
		),
	}
}
