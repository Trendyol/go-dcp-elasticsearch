package bulk

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp/helpers"
	"golang.org/x/sync/errgroup"

	"github.com/elastic/go-elasticsearch/v7/esapi"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/client"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"github.com/Trendyol/go-dcp-elasticsearch/helper"
	"github.com/Trendyol/go-dcp/models"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/json-iterator/go"
)

type Bulk struct {
	errorLogger            logger.Logger
	logger                 logger.Logger
	metric                 *Metric
	collectionIndexMapping map[string]string
	batchKeys              map[string]int
	dcpCheckpointCommit    func()
	batchTicker            *time.Ticker
	isClosed               chan bool
	actionCh               chan document.ESActionDocument
	esClient               *elasticsearch.Client
	batch                  [][]byte
	typeName               []byte
	readers                []*helper.MultiDimByteReader
	batchIndex             int
	batchSize              int
	batchSizeLimit         int
	batchTickerDuration    time.Duration
	batchByteSizeLimit     int
	batchByteSize          int
	concurrentRequest      int
	flushLock              sync.Mutex
	isDcpRebalancing       bool
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

	readers := make([]*helper.MultiDimByteReader, config.Elasticsearch.ConcurrentRequest)
	for i := 0; i < config.Elasticsearch.ConcurrentRequest; i++ {
		readers[i] = helper.NewMultiDimByteReader(nil)
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
		typeName:               helper.Byte(config.Elasticsearch.TypeName),
		readers:                readers,
		concurrentRequest:      config.Elasticsearch.ConcurrentRequest,
		batchKeys:              make(map[string]int, config.Elasticsearch.BatchSizeLimit),
	}
	return bulk, nil
}

func (b *Bulk) StartBulk() {
	for range b.batchTicker.C {
		b.flushMessages()
	}
}

func (b *Bulk) PrepareStartRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = true
	b.batch = b.batch[:0]
	b.batchKeys = make(map[string]int, b.batchSizeLimit)
	b.batchIndex = 0
	b.batchSize = 0
	b.batchByteSize = 0
}

func (b *Bulk) PrepareEndRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = false
}

func (b *Bulk) AddActions(
	ctx *models.ListenerContext,
	eventTime time.Time,
	actions []document.ESActionDocument,
	collectionName string,
) {
	b.flushLock.Lock()
	if b.isDcpRebalancing {
		b.errorLogger.Printf("could not add new message to batch while rebalancing")
		b.flushLock.Unlock()
		return
	}
	for _, action := range actions {
		key := helper.String(action.ID)
		var meta []byte
		getEsActionJSON(
			action.ID,
			action.Type,
			b.collectionIndexMapping[collectionName],
			action.Routing,
			action.Source,
			b.typeName,
			&meta,
		)

		if batchIndex, ok := b.batchKeys[key]; ok {
			b.batch[batchIndex] = meta
		} else {
			b.batch = append(b.batch, meta)
			b.batchKeys[key] = b.batchIndex
			b.batchIndex++
		}

		b.batchByteSize += len(meta)
	}
	ctx.Ack()

	b.batchSize += len(actions)

	b.flushLock.Unlock()

	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	if b.batchSize >= b.batchSizeLimit || len(b.batch) >= b.batchByteSizeLimit {
		b.flushMessages()
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

func getEsActionJSON(
	docID []byte,
	action document.EsAction,
	indexName string,
	routing *string,
	source []byte,
	typeName []byte,
	meta *[]byte,
) {
	if action == document.Index {
		*meta = indexPrefix
	} else {
		*meta = deletePrefix
	}
	*meta = append(*meta, helper.Byte(indexName)...)
	*meta = append(*meta, idPrefix...)
	*meta = append(*meta, docID...)
	if routing != nil {
		*meta = append(*meta, routingPrefix...)
		*meta = append(*meta, helper.Byte(*routing)...)
	}
	if typeName != nil {
		*meta = append(*meta, typePrefix...)
		*meta = append(*meta, typeName...)
	}
	*meta = append(*meta, postFix...)
	if action == document.Index {
		*meta = append(*meta, '\n')
		*meta = append(*meta, source...)
	}
	*meta = append(*meta, '\n')
}

func (b *Bulk) Close() {
	b.batchTicker.Stop()

	b.flushMessages()
}

func (b *Bulk) flushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if b.isDcpRebalancing {
		return
	}
	if len(b.batch) > 0 {
		err := b.bulkRequest()
		if err != nil {
			panic(err)
		}
		b.batchTicker.Reset(b.batchTickerDuration)
		b.batch = b.batch[:0]
		b.batchKeys = make(map[string]int, b.batchSizeLimit)
		b.batchIndex = 0
		b.batchSize = 0
		b.batchByteSize = 0
	}

	b.dcpCheckpointCommit()
}

func (b *Bulk) requestFunc(concurrentRequestIndex int, batch [][]byte) func() error {
	return func() error {
		reader := b.readers[concurrentRequestIndex]
		reader.Reset(batch)
		r, err := b.esClient.Bulk(reader)
		if err != nil {
			return err
		}
		err = hasResponseError(r)
		if err != nil {
			return err
		}
		return nil
	}
}

func (b *Bulk) bulkRequest() error {
	eg, _ := errgroup.WithContext(context.Background())

	chunks := helpers.ChunkSlice(b.batch, b.concurrentRequest)

	startedTime := time.Now()

	for i, chunk := range chunks {
		if len(chunk) > 0 {
			eg.Go(b.requestFunc(i, chunk))
		}
	}

	err := eg.Wait()

	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()

	return err
}

func (b *Bulk) GetMetric() *Metric {
	return b.metric
}

func hasResponseError(r *esapi.Response) error {
	if r == nil {
		return fmt.Errorf("esapi response is nil")
	}
	if r.IsError() {
		return fmt.Errorf("bulk request has error %v", r.String())
	}
	rb := new(bytes.Buffer)

	defer r.Body.Close()
	_, err := rb.ReadFrom(r.Body)
	if err != nil {
		return err
	}
	b := make(map[string]any)
	err = jsoniter.Unmarshal(rb.Bytes(), &b)
	if err != nil {
		return err
	}
	hasError, ok := b["errors"].(bool)
	if !ok || !hasError {
		return nil
	}
	return joinErrors(b)
}

func joinErrors(body map[string]any) error {
	var sb strings.Builder
	sb.WriteString("bulk request has error. Errors will be listed below:\n")

	items, ok := body["items"].([]any)
	if !ok {
		return nil
	}

	for _, i := range items {
		item, ok := i.(map[string]any)
		if !ok {
			continue
		}

		for _, v := range item {
			iv, ok := v.(map[string]any)
			if !ok {
				continue
			}

			if iv["error"] != nil {
				sb.WriteString(fmt.Sprintf("%v\n", i))
			}
		}
	}
	return fmt.Errorf(sb.String())
}
