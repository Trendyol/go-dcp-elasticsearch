package dcpelasticsearch

import (
	"errors"
	"os"

	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/client"
	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/bulk"
	"github.com/Trendyol/go-dcp-elasticsearch/metric"
	"gopkg.in/yaml.v3"

	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/models"
)

type Connector interface {
	Start()
	Close()
}

type connector struct {
	dcp         dcp.Dcp
	mapper      Mapper
	config      *config.Config
	logger      logger.Logger
	errorLogger logger.Logger
	bulk        *bulk.Processor
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

func (c *connector) listener(ctx *models.ListenerContext) {
	var e couchbase.Event
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		e = couchbase.NewMutateEvent(event.Key, event.Value, event.CollectionName)
	case models.DcpExpiration:
		e = couchbase.NewExpireEvent(event.Key, nil, event.CollectionName)
	case models.DcpDeletion:
		e = couchbase.NewDeleteEvent(event.Key, nil, event.CollectionName)
	default:
		return
	}

	actions := c.mapper(e)

	if len(actions) == 0 {
		ctx.Ack()
		return
	}

	c.bulk.AddActions(ctx, e.EventTime, actions, e.CollectionName)
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

func newConnector(cf any, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	cfg, err := newConfig(cf)
	if err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	connector := &connector{
		mapper:      mapper,
		config:      cfg,
		logger:      logger,
		errorLogger: errorLogger,
	}

	dcp, err := dcp.NewDcpWithLoggers(&cfg.Dcp, connector.listener, logger, errorLogger)
	if err != nil {
		connector.errorLogger.Printf("Dcp error: %v", err)
		return nil, err
	}

	dcpConfig := dcp.GetConfig()
	dcpConfig.Checkpoint.Type = "manual"

	connector.dcp = dcp

	esClient, err := client.New(cfg)
	if err != nil {
		return nil, err
	}

	connector.bulk, err = bulk.NewProcessor(
		cfg,
		logger,
		errorLogger,
		dcp.Commit,
		esClient,
	)
	if err != nil {
		return nil, err
	}

	metricCollector := metric.NewMetricCollector(connector.bulk)
	dcp.SetMetricCollectors(metricCollector)

	return connector, nil
}

type ConnectorBuilder struct {
	logger      logger.Logger
	errorLogger logger.Logger
	mapper      Mapper
	config      any
}

func NewConnectorBuilder(config any) ConnectorBuilder {
	return ConnectorBuilder{
		config:      config,
		mapper:      DefaultMapper,
		logger:      logger.Log,
		errorLogger: logger.Log,
	}
}

func (c ConnectorBuilder) SetMapper(mapper Mapper) ConnectorBuilder {
	c.mapper = mapper
	return c
}

func (c ConnectorBuilder) SetLogger(logger logger.Logger) ConnectorBuilder {
	c.logger = logger
	return c
}

func (c ConnectorBuilder) SetErrorLogger(errorLogger logger.Logger) ConnectorBuilder {
	c.errorLogger = errorLogger
	return c
}

func (c ConnectorBuilder) Build() (Connector, error) {
	return newConnector(c.config, c.mapper, c.logger, c.errorLogger)
}
