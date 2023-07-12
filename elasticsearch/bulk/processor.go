package bulk

import (
	"bytes"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/client"
	"github.com/Trendyol/go-dcp-elasticsearch/helper"
)

type Processor struct {
	errorLogger            logger.Logger
	logger                 logger.Logger
	dcpCheckpointCommit    func()
	batchTicker            *time.Ticker
	isClosed               chan bool
	actionCh               chan elasticsearch.ActionDocument
	esClient               client.ESClient
	metric                 *Metric
	collectionIndexMapping map[string]string
	typeName               []byte
	batch                  []byte
	batchSize              int
	batchSizeLimit         int
	batchTickerDuration    time.Duration
	flushLock              sync.Mutex
	batchByteSizeLimit     int
}

type Metric struct {
	ProcessLatencyMs            int64
	BulkRequestProcessLatencyMs int64
}

func NewProcessor(
	config *config.Config,
	logger logger.Logger,
	errorLogger logger.Logger,
	dcpCheckpointCommit func(),
	client client.ESClient,
) (*Processor, error) {
	p := &Processor{
		batchTickerDuration:    config.Elasticsearch.BatchTickerDuration,
		batchTicker:            time.NewTicker(config.Elasticsearch.BatchTickerDuration),
		actionCh:               make(chan elasticsearch.ActionDocument, config.Elasticsearch.BatchSizeLimit),
		batchSizeLimit:         config.Elasticsearch.BatchSizeLimit,
		batchByteSizeLimit:     config.Elasticsearch.BatchByteSizeLimit,
		isClosed:               make(chan bool, 1),
		logger:                 logger,
		errorLogger:            errorLogger,
		dcpCheckpointCommit:    dcpCheckpointCommit,
		esClient:               client,
		metric:                 &Metric{},
		collectionIndexMapping: config.Elasticsearch.CollectionIndexMapping,
		typeName:               helper.Byte(config.Elasticsearch.TypeName),
	}
	return p, nil
}

func (p *Processor) StartBulk() {
	for range p.batchTicker.C {
		p.flushMessages()
	}
}

func (p *Processor) AddActions(
	ctx *models.ListenerContext,
	eventTime time.Time,
	actions []elasticsearch.ActionDocument,
	collectionName string,
) {
	p.flushLock.Lock()

	for _, action := range actions {
		p.batch = append(
			p.batch,
			getEsActionJSON(
				action.ID,
				action.Type,
				p.collectionIndexMapping[collectionName],
				action.Routing,
				action.Source,
				p.typeName,
			)...,
		)
	}
	ctx.Ack()

	p.batchSize += len(actions)

	p.flushLock.Unlock()

	p.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	if p.batchSize >= p.batchSizeLimit || len(p.batch) >= p.batchByteSizeLimit {
		p.flushMessages()
	}
}

var (
	indexPrefix   = helper.Byte(`{"index":{"_index":"`)
	deletePrefix  = helper.Byte(`{"delete":{"_index":"`)
	idPrefix      = helper.Byte(`","_id":"`)
	typePrefix    = helper.Byte(`","_type":"`)
	routingPrefix = helper.Byte(`","_routing":"`)
	postFix       = helper.Byte(`"}}`)
)

func getEsActionJSON(docID []byte,
	action elasticsearch.Action,
	indexName string,
	routing *string,
	source []byte,
	typeName []byte,
) []byte {
	var meta []byte
	if action == elasticsearch.Index {
		meta = indexPrefix
	} else {
		meta = deletePrefix
	}
	meta = append(meta, helper.Byte(indexName)...)
	meta = append(meta, idPrefix...)
	meta = append(meta, docID...)
	if routing != nil {
		meta = append(meta, routingPrefix...)
		meta = append(meta, helper.Byte(*routing)...)
	}
	if typeName != nil {
		meta = append(meta, typePrefix...)
		meta = append(meta, typeName...)
	}
	meta = append(meta, postFix...)
	if action == elasticsearch.Index {
		meta = append(meta, '\n')
		meta = append(meta, source...)
	}
	meta = append(meta, '\n')
	return meta
}

func (p *Processor) Close() {
	p.batchTicker.Stop()

	p.flushMessages()
}

func (p *Processor) flushMessages() {
	p.flushLock.Lock()
	defer p.flushLock.Unlock()

	if len(p.batch) > 0 {
		err := p.bulkRequest()
		if err != nil {
			panic(err)
		}
		p.batchTicker.Reset(p.batchTickerDuration)
		p.batch = p.batch[:0]
		p.batchSize = 0
	}

	p.dcpCheckpointCommit()
}

func (p *Processor) bulkRequest() error {
	startedTime := time.Now()
	reader := bytes.NewReader(p.batch)
	err := p.esClient.Bulk(reader)
	p.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
	if err != nil {
		return err
	}
	return nil
}

func (p *Processor) GetMetric() *Metric {
	return p.metric
}
