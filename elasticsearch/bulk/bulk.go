package bulk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
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
	jsoniter "github.com/json-iterator/go"
)

type Bulk struct {
	sinkResponseHandler dcpElasticsearch.SinkResponseHandler
	metric              *Metric
	config              *config.Config
	batchKeys           map[string]int
	dcpCheckpointCommit func()
	batchTicker         *time.Ticker
	batchCommitTicker   *time.Ticker
	isClosed            chan bool
	actionCh            chan document.ESActionDocument
	esClients           map[string]*elasticsearch.Client
	readers             []*helper.MultiDimByteReader
	typeName            []byte
	batch               []*dcpElasticsearch.BatchItem
	batchIndex          int
	batchSize           int
	batchSizeLimit      int
	batchTickerDuration time.Duration
	batchByteSizeLimit  int
	batchByteSize       int
	concurrentRequest   int
	flushLock           sync.Mutex
	metricCounterMutex  sync.Mutex
	isDcpRebalancing    bool
}

type Metric struct {
	IndexingSuccessActionCounter map[string]int64
	IndexingErrorActionCounter   map[string]int64
	DeletionSuccessActionCounter map[string]int64
	DeletionErrorActionCounter   map[string]int64
	ProcessLatencyMs             int64
	BulkRequestProcessLatencyMs  int64
}

func NewBulk(
	config *config.Config,
	dcpCheckpointCommit func(),
	esClients map[string]*elasticsearch.Client,
	sinkResponseHandler dcpElasticsearch.SinkResponseHandler,
) (*Bulk, error) {
	if esClients == nil || esClients[""] == nil {
		return nil, fmt.Errorf("bulk: elasticsearch clients map must include default cluster (empty key)")
	}

	readers := make([]*helper.MultiDimByteReader, config.Elasticsearch.ConcurrentRequest)
	for i := 0; i < config.Elasticsearch.ConcurrentRequest; i++ {
		readers[i] = helper.NewMultiDimByteReader(nil)
	}

	bulk := &Bulk{
		batchTickerDuration: config.Elasticsearch.BatchTickerDuration,
		batchTicker:         time.NewTicker(config.Elasticsearch.BatchTickerDuration),
		actionCh:            make(chan document.ESActionDocument, config.Elasticsearch.BatchSizeLimit),
		batchSizeLimit:      config.Elasticsearch.BatchSizeLimit,
		batchByteSizeLimit:  helpers.ResolveUnionIntOrStringValue(config.Elasticsearch.BatchByteSizeLimit),
		isClosed:            make(chan bool, 1),
		dcpCheckpointCommit: dcpCheckpointCommit,
		esClients:           esClients,
		metric: &Metric{
			IndexingSuccessActionCounter: make(map[string]int64),
			IndexingErrorActionCounter:   make(map[string]int64),
			DeletionSuccessActionCounter: make(map[string]int64),
			DeletionErrorActionCounter:   make(map[string]int64),
		},
		config:              config,
		typeName:            helper.Byte(config.Elasticsearch.TypeName),
		readers:             readers,
		concurrentRequest:   config.Elasticsearch.ConcurrentRequest,
		batchKeys:           make(map[string]int, config.Elasticsearch.BatchSizeLimit),
		sinkResponseHandler: sinkResponseHandler,
	}

	if config.Elasticsearch.BatchCommitTickerDuration != nil {
		bulk.batchCommitTicker = time.NewTicker(*config.Elasticsearch.BatchCommitTickerDuration)
	}

	if sinkResponseHandler != nil {
		sinkResponseHandler.OnInit(&dcpElasticsearch.SinkResponseHandlerInitContext{
			Config:               config,
			ElasticsearchClient:  esClients[""],
			ElasticsearchClients: esClients,
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
	for i := range actions {
		clusterKey := config.NormalizeClusterKey(actions[i].ClusterKey)
		actions[i].ClusterKey = clusterKey

		if clusterKey != "" {
			if _, ok := b.esClients[clusterKey]; !ok {
				err := fmt.Errorf("unknown elasticsearch cluster key %q", clusterKey)
				logger.Log.Error("error while validating cluster key, err: %v", err)
				panic(err)
			}
		}

		indexName := b.getIndexName(collectionName, actions[i].IndexName, clusterKey)
		actions[i].IndexName = indexName
		value := getEsActionJSON(
			actions[i].ID,
			actions[i].Type,
			actions[i].IndexName,
			actions[i].Routing,
			actions[i].Source,
			b.typeName,
		)

		key := getActionKey(actions[i])
		if batchIndex, ok := b.batchKeys[key]; ok {
			b.batchByteSize += len(value) - len(b.batch[batchIndex].Bytes)
			b.batch[batchIndex] = &dcpElasticsearch.BatchItem{
				Action: &actions[i],
				Bytes:  value,
			}
		} else {
			b.batch = append(b.batch, &dcpElasticsearch.BatchItem{
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
	indexPrefix       = helper.Byte(`{"index":{"_index":"`)
	deletePrefix      = helper.Byte(`{"delete":{"_index":"`)
	updatePrefix      = helper.Byte(`{"update":{"_index":"`)
	scriptPrefix      = helper.Byte(`{"script":`)
	idPrefix          = helper.Byte(`","_id":"`)
	typePrefix        = helper.Byte(`","_type":"`)
	routingPrefix     = helper.Byte(`","routing":"`)
	postFix           = helper.Byte(`"}}`)
	scriptPostfix     = helper.Byte(`,"scripted_upsert":true}`)
	updateDocTemplate = `{"doc":%s, "doc_as_upsert":true}`
)

var metaPool = sync.Pool{
	New: func() interface{} {
		return []byte{}
	},
}

func getEsActionJSON(docID []byte, action document.EsAction, indexName string, routing *string, source []byte, typeName []byte) []byte {
	meta := metaPool.Get().([]byte)[:0]

	switch action {
	case document.Index:
		meta = append(meta, indexPrefix...)
	case document.DocUpdate, document.ScriptUpdate:
		meta = append(meta, updatePrefix...)
	case document.Delete:
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

	switch action {
	case document.Index:
		meta = append(meta, '\n')
		meta = append(meta, source...)
	case document.DocUpdate:
		meta = append(meta, '\n')
		meta = append(meta, []byte(fmt.Sprintf(updateDocTemplate, source))...)
	case document.ScriptUpdate:
		meta = append(meta, '\n')
		meta = append(meta, scriptPrefix...)
		meta = append(meta, source...)
		meta = append(meta, scriptPostfix...)
	case document.Delete:
		// Delete action doesn't need a body
	}

	meta = append(meta, '\n')
	return meta
}

func (b *Bulk) Close() {
	b.batchTicker.Stop()
	if b.batchCommitTicker != nil {
		b.batchCommitTicker.Stop()
	}

	b.flushMessages()
}

func (b *Bulk) flushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if b.isDcpRebalancing {
		return
	}
	if len(b.batch) > 0 {
		if b.sinkResponseHandler != nil {
			b.sinkResponseHandler.OnBeforeBulk(&dcpElasticsearch.SinkResponseHandlerBulkContext{
				BatchItems: b.batch,
			})
		}
		err := b.bulkRequest()
		if err != nil && b.sinkResponseHandler == nil {
			logger.Log.Error("error while bulk request, err: %v", err)
			panic(err)
		}
		if b.sinkResponseHandler != nil {
			b.sinkResponseHandler.OnAfterBulk(&dcpElasticsearch.SinkResponseHandlerBulkContext{
				BatchItems: b.batch,
			})
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
	b.CheckAndCommit()
}

func (b *Bulk) CheckAndCommit() {
	if b.batchCommitTicker == nil {
		b.dcpCheckpointCommit()
		return
	}

	select {
	case <-b.batchCommitTicker.C:
		b.dcpCheckpointCommit()
	default:
		return
	}
}

func (b *Bulk) requestFunc(
	concurrentRequestIndex int,
	batchItems []*dcpElasticsearch.BatchItem,
	esClient *elasticsearch.Client,
	maxRetries int,
) func() error {
	return func() error {
		reader := b.readers[concurrentRequestIndex]
		actionsOfBatchItems := getActions(batchItems)
		batchItemBytes := getBytes(batchItems)
		reader.Reset(batchItemBytes)

		for attempt := 1; attempt <= maxRetries; attempt++ {
			r, err := esClient.Bulk(reader)
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) {
					logger.Log.Warn(fmt.Sprintf("unexpected eof error in attempt: %d", attempt))
					if attempt != maxRetries {
						reader.ResetPositions()
						continue
					}
				}

				b.finalizeProcess(actionsOfBatchItems, fillErrorDataWithBulkRequestError(actionsOfBatchItems, err))
				return err
			}

			errorData, err := hasResponseError(r, actionsOfBatchItems)
			b.finalizeProcess(actionsOfBatchItems, errorData)
			if err != nil {
				return err
			}
			return nil
		}

		return fmt.Errorf("max retry cannot be 0")
	}
}

func (b *Bulk) bulkRequest() error {
	byCluster := make(map[string][]*dcpElasticsearch.BatchItem)
	for _, item := range b.batch {
		if item.Action == nil {
			continue
		}
		ck := config.NormalizeClusterKey(item.Action.ClusterKey)
		byCluster[ck] = append(byCluster[ck], item)
	}

	clusterKeys := make([]string, 0, len(byCluster))
	for k := range byCluster {
		clusterKeys = append(clusterKeys, k)
	}
	sort.Strings(clusterKeys)

	eg, _ := errgroup.WithContext(context.Background())
	startedTime := time.Now()

	for _, ck := range clusterKeys {
		ck := ck
		partition := byCluster[ck]
		esClient := b.esClients[ck]
		esSettings := b.elasticsearchSettingsForCluster(ck)

		eg.Go(func() error {
			return b.bulkRequestPartition(partition, esClient, esSettings.MaxRetries)
		})
	}

	err := eg.Wait()

	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()

	return err
}

func (b *Bulk) bulkRequestPartition(partition []*dcpElasticsearch.BatchItem, esClient *elasticsearch.Client, maxRetries int) error {
	if len(partition) == 0 {
		return nil
	}

	eg, _ := errgroup.WithContext(context.Background())
	chunks := helpers.ChunkSlice(partition, b.concurrentRequest)

	for i, chunk := range chunks {
		if len(chunk) > 0 {
			eg.Go(b.requestFunc(i, chunk, esClient, maxRetries))
		}
	}

	return eg.Wait()
}

func (b *Bulk) GetMetric() *Metric {
	return b.metric
}

func hasResponseError(r *esapi.Response, batchActions []*document.ESActionDocument) (map[string]string, error) {
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
	body := make(map[string]any)
	err = jsoniter.Unmarshal(rb.Bytes(), &body)
	if err != nil {
		return nil, err
	}
	hasError, ok := body["errors"].(bool)
	if !ok || !hasError {
		return nil, nil
	}
	return joinErrors(body, batchActions)
}

func joinErrors(body map[string]any, batchActions []*document.ESActionDocument) (map[string]string, error) {
	var sb strings.Builder
	ivd := make(map[string]string)
	sb.WriteString("bulk request has error. Errors will be listed below:\n")

	items, ok := body["items"].([]any)
	if !ok {
		return nil, nil
	}

	for idx, i := range items {
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
				actionKey := bulkErrorItemKey(batchActions, idx, iv)
				ivd[actionKey] = itemValue
			}
		}
	}
	return ivd, fmt.Errorf("%s", sb.String())
}

func bulkErrorItemKey(batchActions []*document.ESActionDocument, itemIdx int, iv map[string]any) string {
	if itemIdx < len(batchActions) && batchActions[itemIdx] != nil {
		return getActionKey(*batchActions[itemIdx])
	}
	id, _ := iv["_id"].(string)
	index, _ := iv["_index"].(string)
	return fmt.Sprintf("%s:%s", id, index)
}

func (b *Bulk) elasticsearchSettingsForCluster(clusterKey string) config.Elasticsearch {
	if clusterKey == "" {
		return b.config.Elasticsearch
	}
	return b.config.Elasticsearch.Clusters[clusterKey]
}

func (b *Bulk) collectionMappingForCluster(clusterKey string) map[string]string {
	if clusterKey == "" {
		return b.config.Elasticsearch.CollectionIndexMapping
	}
	return b.config.Elasticsearch.Clusters[clusterKey].CollectionIndexMapping
}

func (b *Bulk) getIndexName(collectionName, actionIndexName, clusterKey string) string {
	if actionIndexName != "" {
		return actionIndexName
	}

	mapping := b.collectionMappingForCluster(clusterKey)
	indexName := mapping[collectionName]
	if indexName == "" {
		err := fmt.Errorf(
			"there is no index mapping for collection: %s on your elasticsearch cluster configuration (clusterKey=%q)",
			collectionName,
			clusterKey,
		)
		logger.Log.Error("error while get index name, err: %v", err)
		panic(err)
	}

	return indexName
}

func fillErrorDataWithBulkRequestError(batchActions []*document.ESActionDocument, err error) map[string]string {
	errorData := make(map[string]string, len(batchActions))
	for _, action := range batchActions {
		key := getActionKey(*action)
		errorData[key] = err.Error()
	}
	return errorData
}

func (b *Bulk) LockMetrics() {
	b.metricCounterMutex.Lock()
}

func (b *Bulk) UnlockMetrics() {
	b.metricCounterMutex.Unlock()
}

func (b *Bulk) finalizeProcess(batchActions []*document.ESActionDocument, errorData map[string]string) {
	for _, action := range batchActions {
		key := getActionKey(*action)
		if _, ok := errorData[key]; ok {
			go b.countError(action)
			if b.sinkResponseHandler != nil {
				b.sinkResponseHandler.OnError(&dcpElasticsearch.SinkResponseHandlerContext{
					Action: action,
					Err:    fmt.Errorf("%s", errorData[key]),
				})
			}
		} else {
			go b.countSuccess(action)
			if b.sinkResponseHandler != nil {
				b.sinkResponseHandler.OnSuccess(&dcpElasticsearch.SinkResponseHandlerContext{
					Action: action,
				})
			}
		}
	}
}

func (b *Bulk) countError(action *document.ESActionDocument) {
	b.LockMetrics()
	defer b.UnlockMetrics()

	switch action.Type {
	case document.Index, document.DocUpdate, document.ScriptUpdate:
		b.metric.IndexingErrorActionCounter[action.IndexName]++
	case document.Delete:
		b.metric.DeletionErrorActionCounter[action.IndexName]++
	}
}

func (b *Bulk) countSuccess(action *document.ESActionDocument) {
	b.LockMetrics()
	defer b.UnlockMetrics()

	switch action.Type {
	case document.Index, document.DocUpdate, document.ScriptUpdate:
		b.metric.IndexingSuccessActionCounter[action.IndexName]++
	case document.Delete:
		b.metric.DeletionSuccessActionCounter[action.IndexName]++
	}
}

func getActionKey(action document.ESActionDocument) string {
	clusterKey := config.NormalizeClusterKey(action.ClusterKey)
	var base string
	if action.Routing != nil {
		base = fmt.Sprintf("%s:%s:%s", action.ID, action.IndexName, *action.Routing)
	} else {
		base = fmt.Sprintf("%s:%s", action.ID, action.IndexName)
	}
	if clusterKey != "" {
		return clusterKey + "::" + base
	}
	return base
}

func getBytes(batchItems []*dcpElasticsearch.BatchItem) [][]byte {
	batchBytes := make([][]byte, 0, len(batchItems))
	for _, batchItem := range batchItems {
		if batchItem.IsSkipped {
			continue
		}

		batchBytes = append(batchBytes, batchItem.Bytes)
	}
	return batchBytes
}

func getActions(batchItems []*dcpElasticsearch.BatchItem) []*document.ESActionDocument {
	result := make([]*document.ESActionDocument, 0, len(batchItems))
	for _, batchItem := range batchItems {
		if batchItem.IsSkipped {
			continue
		}

		result = append(result, batchItem.Action)
	}
	return result
}
