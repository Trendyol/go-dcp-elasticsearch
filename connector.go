package dcpelasticsearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"os"
	"regexp"
	"strings"

	jsoniter "github.com/json-iterator/go"

	dcpCouchbase "github.com/Trendyol/go-dcp/couchbase"

	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/client"
	"github.com/elastic/go-elasticsearch/v7"

	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"github.com/Trendyol/go-dcp/helpers"

	"github.com/sirupsen/logrus"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	dcpElasticsearch "github.com/Trendyol/go-dcp-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/bulk"
	"github.com/Trendyol/go-dcp-elasticsearch/metric"
	"gopkg.in/yaml.v3"

	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/models"
)

type Connector interface {
	Start()
	Close()
	GetDcpClient() dcpCouchbase.Client
}

type connector struct {
	dcp                 dcp.Dcp
	mapper              Mapper
	config              *config.Config
	bulk                *bulk.Bulk
	esClient            *elasticsearch.Client
	sinkResponseHandler dcpElasticsearch.SinkResponseHandler
}

func (c *connector) Start() {
	go func() {
		<-c.dcp.WaitUntilReady()
		c.bulk.StartBulk()
	}()
	c.dcp.Start()
}

func (c *connector) Close() {
	c.dcp.Close()
	c.bulk.Close()
}

func (c *connector) GetDcpClient() dcpCouchbase.Client {
	return c.dcp.GetClient()
}

func (c *connector) listener(ctx *models.ListenerContext) {
	// Initialize ListenerTrace for current listen operation
	listenerTrace := ctx.ListenerTracerComponent.InitializeListenerTrace("Listen", nil)
	defer listenerTrace.Finish()

	var e couchbase.Event
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		e = couchbase.NewMutateEvent(
			c.esClient,
			event.Key, event.Value,
			event.CollectionName, event.Cas, event.EventTime, event.VbID, event.SeqNo, event.RevNo,
		)
	case models.DcpExpiration:
		e = couchbase.NewExpireEvent(
			c.esClient,
			event.Key, nil,
			event.CollectionName, event.Cas, event.EventTime, event.VbID, event.SeqNo, event.RevNo,
		)
	case models.DcpDeletion:
		e = couchbase.NewDeleteEvent(
			c.esClient,
			event.Key, nil,
			event.CollectionName, event.Cas, event.EventTime, event.VbID, event.SeqNo, event.RevNo,
		)
	default:
		return
	}

	e.ListenerTrace = listenerTrace
	actions := c.mapper(e)

	if len(actions) == 0 {
		ctx.Ack()
		return
	}

	batchSizeLimit := c.config.Elasticsearch.BatchSizeLimit
	if len(actions) > batchSizeLimit {
		chunks := helpers.ChunkSliceWithSize[document.ESActionDocument](actions, batchSizeLimit)
		lastChunkIndex := len(chunks) - 1
		for idx, chunk := range chunks {
			c.bulk.AddActions(ctx, e.EventTime, chunk, e.CollectionName, idx == lastChunkIndex)
		}
	} else {
		c.bulk.AddActions(ctx, e.EventTime, actions, e.CollectionName, true)
	}
}

func newConnectorConfigFromPath(path string) (*config.Config, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c config.Config
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return nil, err
	}
	envPattern := regexp.MustCompile(`\${([^}]+)}`)
	matches := envPattern.FindAllStringSubmatch(string(file), -1)
	for _, match := range matches {
		envVar := match[1]
		if value, exists := os.LookupEnv(envVar); exists {
			updatedFile := strings.ReplaceAll(string(file), "${"+envVar+"}", value)
			file = []byte(updatedFile)
		}
	}
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func newConfig(cf any) (*config.Config, error) {
	switch v := cf.(type) {
	case *config.Config:
		return v, nil
	case config.Config:
		return &v, nil
	case string:
		return newConnectorConfigFromPath(v)
	default:
		return nil, errors.New("invalid config")
	}
}

func newConnector(cf any, mapper Mapper, sinkResponseHandler dcpElasticsearch.SinkResponseHandler, metricCollectors ...prometheus.Collector) (Connector, error) {
	cfg, err := newConfig(cf)
	if err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	connector := &connector{
		mapper:              mapper,
		config:              cfg,
		sinkResponseHandler: sinkResponseHandler,
	}

	dcp, err := dcp.NewDcp(&cfg.Dcp, connector.listener)
	if err != nil {
		logger.Log.Error("Dcp error: %v", err)
		return nil, err
	}

	dcp.SetMetricCollectors(metricCollectors...)

	copyOfConfig := cfg.Elasticsearch
	printConfiguration(copyOfConfig)

	dcpConfig := dcp.GetConfig()
	dcpConfig.Checkpoint.Type = "manual"

	esClient, err := client.NewElasticClient(cfg)
	if err != nil {
		return nil, err
	}
	connector.esClient = esClient

	connector.dcp = dcp
	connector.bulk, err = bulk.NewBulk(
		cfg,
		dcp.Commit,
		esClient,
		sinkResponseHandler,
	)
	if err != nil {
		return nil, err
	}

	connector.dcp.SetEventHandler(
		&DcpEventHandler{
			isFinite: dcpConfig.IsDcpModeFinite(),
			bulk:     connector.bulk,
		})

	metricCollector := metric.NewMetricCollector(connector.bulk)
	dcp.SetMetricCollectors(metricCollector)

	return connector, nil
}

type ConnectorBuilder struct {
	mapper              Mapper
	config              any
	sinkResponseHandler dcpElasticsearch.SinkResponseHandler
	metricCollectors    []prometheus.Collector
}

func NewConnectorBuilder(config any) *ConnectorBuilder {
	return &ConnectorBuilder{
		config: config,
		mapper: DefaultMapper,
	}
}

func (c *ConnectorBuilder) Build() (Connector, error) {
	return newConnector(c.config, c.mapper, c.sinkResponseHandler, c.metricCollectors...)
}

func (c *ConnectorBuilder) SetMapper(mapper Mapper) *ConnectorBuilder {
	c.mapper = mapper
	return c
}

func (c *ConnectorBuilder) SetLogger(logrus *logrus.Logger) *ConnectorBuilder {
	logger.Log = &logger.Loggers{
		Logrus: logrus,
	}
	return c
}

func (c *ConnectorBuilder) SetMetricCollectors(collectors ...prometheus.Collector) {
	c.metricCollectors = append(c.metricCollectors, collectors...)
}

func (c *ConnectorBuilder) SetSinkResponseHandler(sinkResponseHandler dcpElasticsearch.SinkResponseHandler) *ConnectorBuilder {
	c.sinkResponseHandler = sinkResponseHandler
	return c
}

func printConfiguration(config config.Elasticsearch) {
	config.Password = "*****"
	configJSON, _ := jsoniter.Marshal(config)

	dst := &bytes.Buffer{}
	if err := json.Compact(dst, configJSON); err != nil {
		logger.Log.Error("error while print elasticsearch configuration, err: %v", err)
		panic(err)
	}

	logger.Log.Info("using elasticsearch config: %v", dst.String())
}
