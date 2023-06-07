package bulk

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v7/esapi"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/models"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/config"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch/client"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch/document"
	"github.com/elastic/go-elasticsearch/v7"
)

type Bulk struct {
	errorLogger            logger.Logger
	logger                 logger.Logger
	dcpCheckpointCommit    func()
	batchTicker            *time.Ticker
	isClosed               chan bool
	actionCh               chan document.ESActionDocument
	esClient               *elasticsearch.Client
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

func NewBulk(
	config *config.Config,
	logger logger.Logger,
	errorLogger logger.Logger,
	dcpCheckpointCommit func(),
) (*Bulk, error) {
	esClient, err := client.NewElasticClient(config)
	if err != nil {
		return nil, err
	}

	bulk := &Bulk{
		batchTickerDuration:    config.Elasticsearch.BatchTickerDuration,
		batchTicker:            time.NewTicker(config.Elasticsearch.BatchTickerDuration),
		actionCh:               make(chan document.ESActionDocument, config.Elasticsearch.BatchSizeLimit),
		batchSizeLimit:         config.Elasticsearch.BatchSizeLimit,
		batchByteSizeLimit:     config.Elasticsearch.BatchByteSizeLimit,
		isClosed:               make(chan bool, 1),
		logger:                 logger,
		errorLogger:            errorLogger,
		dcpCheckpointCommit:    dcpCheckpointCommit,
		esClient:               esClient,
		metric:                 &Metric{},
		collectionIndexMapping: config.Elasticsearch.CollectionIndexMapping,
		typeName:               StringToByte(config.Elasticsearch.TypeName),
	}
	return bulk, nil
}

func (b *Bulk) StartBulk() {
	for range b.batchTicker.C {
		err := b.flushMessages()
		if err != nil {
			b.errorLogger.Printf("Batch producer flush error %v", err)
		}
	}
}

func (b *Bulk) AddActions(
	ctx *models.ListenerContext,
	eventTime time.Time,
	actions []document.ESActionDocument,
	collectionName string,
) {
	b.flushLock.Lock()

	for _, action := range actions {
		b.batch = append(
			b.batch,
			getEsActionJSON(
				action.ID,
				action.Type,
				b.collectionIndexMapping[collectionName],
				action.Routing,
				action.Source,
				b.typeName,
			)...,
		)
	}
	ctx.Ack()

	b.batchSize += len(actions)

	b.flushLock.Unlock()

	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	if b.batchSize >= b.batchSizeLimit || len(b.batch) >= b.batchByteSizeLimit {
		err := b.flushMessages()
		if err != nil {
			b.errorLogger.Printf("Bulk writer error %v", err)
		}
	}
}

var (
	indexPrefix   = StringToByte(`{"index":{"_index":"`)
	deletePrefix  = StringToByte(`{"delete":{"_index":"`)
	idPrefix      = StringToByte(`","_id":"`)
	typePrefix    = StringToByte(`","_type":"`)
	routingPrefix = StringToByte(`","_routing":"`)
	postFix       = StringToByte(`"}}`)
)

func getEsActionJSON(docID []byte, action document.EsAction, indexName string, routing *string, source []byte, typeName []byte) []byte {
	var meta []byte
	if action == document.Index {
		meta = indexPrefix
	} else {
		meta = deletePrefix
	}
	meta = append(meta, StringToByte(indexName)...)
	meta = append(meta, idPrefix...)
	meta = append(meta, docID...)
	if routing != nil {
		meta = append(meta, routingPrefix...)
		meta = append(meta, StringToByte(*routing)...)
	}
	if typeName != nil {
		meta = append(meta, typePrefix...)
		meta = append(meta, typeName...)
	}
	meta = append(meta, postFix...)
	if action == document.Index {
		meta = append(meta, '\n')
		meta = append(meta, source...)
	}
	meta = append(meta, '\n')
	return meta
}

func (b *Bulk) Close() {
	b.batchTicker.Stop()

	err := b.flushMessages()
	if err != nil {
		b.errorLogger.Printf("Bulk error %v", err)
	}
}

func (b *Bulk) flushMessages() error {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if len(b.batch) > 0 {
		err := b.bulkRequest()
		if err != nil {
			return err
		}
		b.batchTicker.Reset(b.batchTickerDuration)
		b.batch = b.batch[:0]
		b.batchSize = 0
	}

	b.dcpCheckpointCommit()

	return nil
}

func (b *Bulk) bulkRequest() error {
	startedTime := time.Now()
	reader := bytes.NewReader(b.batch)
	r, err := b.esClient.Bulk(reader)
	if err != nil {
		return err
	}

	if hasResponseError(r) != nil {
		return err
	}

	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()

	return nil
}

func hasResponseError(r *esapi.Response) error {
	if r.IsError() {
		var body []byte
		_, _ = r.Body.Read(body)

		return fmt.Errorf("elasticsearch bulk request has an error. Status code: %s, response body: %s",
			r.Status(), ByteToString(body))
	}

	return nil
}

func (b *Bulk) GetMetric() *Metric {
	return b.metric
}
