package goelasticsearchconnectcouchbase

import (
	"go-elasticsearch-connect-couchbase/config"
	"go-elasticsearch-connect-couchbase/couchbase"
	"go-elasticsearch-connect-couchbase/elasticsearch/bulk"
	"go-elasticsearch-connect-couchbase/logger"

	godcpclient "github.com/Trendyol/go-dcp-client"
	"github.com/Trendyol/go-dcp-client/models"
)

type Connector interface {
	Start()
	Close()
}

type connector struct {
	dcp         godcpclient.Dcp
	mapper      Mapper
	config      *config.Config
	logger      logger.Logger
	errorLogger logger.Logger
	bulk        *bulk.Bulk
}

func (c *connector) Start() {
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

	for i := range actions {
		c.bulk.AddAction(ctx, actions[i], e.CollectionName)
	}
}

func newConnector(configPath string, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	c := config.NewConfig("cbgoelasticsearch", configPath, errorLogger)

	connector := &connector{
		mapper:      mapper,
		config:      c,
		logger:      logger,
		errorLogger: errorLogger,
	}

	dcp, err := godcpclient.NewDcp(configPath, connector.listener)
	if err != nil {
		connector.errorLogger.Printf("Dcp error: %v", err)
		return nil, err
	}
	connector.dcp = dcp
	connector.bulk, err = bulk.NewBulk(
		c.Elasticsearch,
		logger,
		errorLogger,
		dcp.Commit,
	)
	if err != nil {
		return nil, err
	}

	return connector, nil
}

type ConnectorBuilder struct {
	logger      logger.Logger
	errorLogger logger.Logger
	mapper      Mapper
	configPath  string
}

func NewConnectorBuilder(configPath string) ConnectorBuilder {
	return ConnectorBuilder{
		configPath:  configPath,
		mapper:      DefaultMapper,
		logger:      &logger.Log,
		errorLogger: &logger.Log,
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
	return newConnector(c.configPath, c.mapper, c.logger, c.errorLogger)
}
