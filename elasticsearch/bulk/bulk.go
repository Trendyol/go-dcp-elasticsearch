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
	dcpElasticsearch "github.com/Trendyol/go-dcp-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"github.com/Trendyol/go-dcp-elasticsearch/helper"
	"github.com/Trendyol/go-dcp/models"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/json-iterator/go"
)

type Bulk struct {
	sinkResponseHandler    dcpElasticsearch.SinkResponseHandler
	metric                 *Metric
	collectionIndexMapping map[string]string
	config                 *config.Config
	batchKeys              map[string]int
	dcpCheckpointCommit    func()
	batchTicker            *time.Ticker
	isClosed               chan bool
	actionCh               chan document.ESActionDocument
	esClient               *elasticsearch.Client
	readers                []*helper.MultiDimByteReader
	typeName               []byte
	batch                  []BatchItem
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

type BatchItem struct {
	Action *document.ESActionDocument
	Bytes  []byte
}

func NewBulk(
	config *config.Config,
	dcpCheckpointCommit func(),
	esClient *elasticsearch.Client,
	sinkResponseHandler dcpElasticsearch.SinkResponseHandler,
) (*Bulk, error) {
	readers := make([]*helper.MultiDimByteReader, config.Elasticsearch.ConcurrentRequest)
	for i := 0; i < config.Elasticsearch.ConcurrentRequest; i++ {
		readers[i] = helper.NewMultiDimByteReader(nil)
	}

	bulk := &Bulk{
		batchTickerDuration:    config.Elasticsearch.BatchTickerDuration,
		batchTicker:            time.NewTicker(config.Elasticsearch.BatchTickerDuration),
		actionCh:               make(chan document.ESActionDocument, config.Elasticsearch.BatchSizeLimit),
		batchSizeLimit:         config.Elasticsearch.BatchSizeLimit,
		batchByteSizeLimit:     helpers.ResolveUnionIntOrStringValue(config.Elasticsearch.BatchByteSizeLimit),
		isClosed:               make(chan bool, 1),
		dcpCheckpointCommit:    dcpCheckpointCommit,
		esClient:               esClient,
		metric:                 &Metric{},
		collectionIndexMapping: config.Elasticsearch.CollectionIndexMapping,
		config:                 config,
		typeName:               helper.Byte(config.Elasticsearch.TypeName),
		readers:                readers,
		concurrentRequest:      config.Elasticsearch.ConcurrentRequest,
		batchKeys:              make(map[string]int, config.Elasticsearch.BatchSizeLimit),
		sinkResponseHandler:    sinkResponseHandler,
	}

	if sinkResponseHandler != nil {
		sinkResponseHandler.OnInit(&dcpElasticsearch.SinkResponseHandlerInitContext{
			Config:              config,
			ElasticsearchClient: esClient,
		})
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
	isLastChunk bool,
) {
	b.flushLock.Lock()
	if b.isDcpRebalancing {
		logger.Log.Warn("could not add new message to batch while rebalancing")
		b.flushLock.Unlock()
		return
	}
	for i, action := range actions {
		indexName := b.getIndexName(collectionName, action.IndexName)
		actions[i].IndexName = indexName
		value := getEsActionJSON(
			action.ID,
			action.Type,
			actions[i].IndexName,
			action.Routing,
			action.Source,
			b.typeName,
		)

		key := getActionKey(actions[i])
		if batchIndex, ok := b.batchKeys[key]; ok {
			b.batchByteSize += len(value) - len(b.batch[batchIndex].Bytes)
			b.batch[batchIndex] = BatchItem{
				Action: &actions[i],
				Bytes:  value,
			}
		} else {
			b.batch = append(b.batch, BatchItem{
				Action: &actions[i],
				Bytes:  value,
			})
			b.batchKeys[key] = b.batchIndex
			b.batchIndex++
			b.batchSize++
			b.batchByteSize += len(value)
		}
	}
	if isLastChunk {
		ctx.Ack()
	}

	b.flushLock.Unlock()

	if isLastChunk {
		b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	}
	if b.batchSize >= b.batchSizeLimit || b.batchByteSize >= b.batchByteSizeLimit {
		b.flushMessages()
	}
}

var (
	indexPrefix   = helper.Byte(`{"index":{"_index":"`)
	deletePrefix  = helper.Byte(`{"delete":{"_index":"`)
	idPrefix      = helper.Byte(`","_id":"`)
	typePrefix    = helper.Byte(`","_type":"`)
	routingPrefix = helper.Byte(`","routing":"`)
	postFix       = helper.Byte(`"}}`)
)

var metaPool = sync.Pool{
	New: func() interface{} {
		return []byte{}
	},
}

func getEsActionJSON(docID []byte, action document.EsAction, indexName string, routing *string, source []byte, typeName []byte) []byte {
	meta := metaPool.Get().([]byte)[:0]

	if action == document.Index {
		meta = append(meta, indexPrefix...)
	} else {
		meta = append(meta, deletePrefix...)
	}
	meta = append(meta, helper.Byte(indexName)...)
	meta = append(meta, idPrefix...)
	meta = append(meta, helper.EscapePredefinedBytes(docID)...)
	if routing != nil {
		meta = append(meta, routingPrefix...)
		meta = append(meta, helper.Byte(*routing)...)
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
		if err != nil && b.sinkResponseHandler == nil {
			panic(err)
		}
		b.batchTicker.Reset(b.batchTickerDuration)
		for _, batch := range b.batch {
			//nolint:staticcheck
			metaPool.Put(batch.Bytes)
		}
		b.batch = b.batch[:0]
		b.batchKeys = make(map[string]int, b.batchSizeLimit)
		b.batchIndex = 0
		b.batchSize = 0
		b.batchByteSize = 0
	}

	b.dcpCheckpointCommit()
}

func (b *Bulk) requestFunc(concurrentRequestIndex int, batchItems []BatchItem) func() error {
	return func() error {
		reader := b.readers[concurrentRequestIndex]
		reader.Reset(getBytes(batchItems))
		r, err := b.esClient.Bulk(reader)
		if err != nil {
			return err
		}
		errorData, err := hasResponseError(r)
		b.executeSinkResponseHandler(getActions(batchItems), errorData)

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

func hasResponseError(r *esapi.Response) (map[string]string, error) {
	if r == nil {
		return nil, fmt.Errorf("esapi response is nil")
	}
	if r.IsError() {
		return nil, fmt.Errorf("bulk request has error %v", r.String())
	}
	rb := new(bytes.Buffer)

	defer r.Body.Close()
	_, err := rb.ReadFrom(r.Body)
	if err != nil {
		return nil, err
	}
	b := make(map[string]any)
	err = jsoniter.Unmarshal(rb.Bytes(), &b)
	if err != nil {
		return nil, err
	}
	hasError, ok := b["errors"].(bool)
	if !ok || !hasError {
		return nil, nil
	}
	return joinErrors(b)
}

func joinErrors(body map[string]any) (map[string]string, error) {
	var sb strings.Builder
	ivd := make(map[string]string)
	sb.WriteString("bulk request has error. Errors will be listed below:\n")

	items, ok := body["items"].([]any)
	if !ok {
		return nil, nil
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
				itemValue := fmt.Sprintf("%v\n", i)
				sb.WriteString(itemValue)
				itemValueDataKey := fmt.Sprintf("%s:%s", iv["_id"].(string), iv["_index"].(string))
				ivd[itemValueDataKey] = itemValue
			}
		}
	}
	return ivd, fmt.Errorf(sb.String())
}

func (b *Bulk) getIndexName(collectionName, actionIndexName string) string {
	if actionIndexName != "" {
		return actionIndexName
	}

	indexName := b.collectionIndexMapping[collectionName]
	if indexName == "" {
		panic(fmt.Sprintf("there is no index mapping for collection: %s on your configuration", collectionName))
	}

	return indexName
}

func (b *Bulk) executeSinkResponseHandler(batchActions []*document.ESActionDocument, errorData map[string]string) {
	if b.sinkResponseHandler == nil {
		return
	}

	for _, action := range batchActions {
		key := getActionKey(*action)
		if _, ok := errorData[key]; ok {
			b.sinkResponseHandler.OnError(&dcpElasticsearch.SinkResponseHandlerContext{
				Action: action,
				Err:    fmt.Errorf(errorData[key]),
			})
		} else {
			b.sinkResponseHandler.OnSuccess(&dcpElasticsearch.SinkResponseHandlerContext{
				Action: action,
			})
		}
	}
}

func getActionKey(action document.ESActionDocument) string {
	if action.Routing != nil {
		return fmt.Sprintf("%s:%s:%s", action.ID, action.IndexName, *action.Routing)
	}
	return fmt.Sprintf("%s:%s", action.ID, action.IndexName)
}

func getBytes(batchItems []BatchItem) [][]byte {
	batchBytes := make([][]byte, 0, len(batchItems))
	for _, batchItem := range batchItems {
		batchBytes = append(batchBytes, batchItem.Bytes)
	}
	return batchBytes
}

func getActions(batchItems []BatchItem) []*document.ESActionDocument {
	batchActions := make([]*document.ESActionDocument, 0, len(batchItems))
	for _, batchItem := range batchItems {
		batchActions = append(batchActions, batchItem.Action)
	}
	return batchActions
}
