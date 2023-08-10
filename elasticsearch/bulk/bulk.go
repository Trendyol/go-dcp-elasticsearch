package bulk

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp/helpers"
	"golang.org/x/sync/errgroup"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/client"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"github.com/Trendyol/go-dcp-elasticsearch/helper"
	"github.com/Trendyol/go-dcp/models"
	"github.com/json-iterator/go"
)

type Bulk struct {
	logger                 logger.Logger
	errorLogger            logger.Logger
	elasticClient          client.ElasticClient
	batchTicker            *time.Ticker
	metric                 *Metric
	collectionIndexMapping map[string]string
	batchKeys              map[string]int
	dcpCheckpointCommit    func()
	typeName               []byte
	batchKeyData           []helper.BatchKeyData
	batch                  []byte
	readers                []*helper.BatchKeyDataReader
	batchTickerDuration    time.Duration
	batchSize              int
	batchSizeLimit         int
	batchKeyDataIndex      int
	batchByteSizeLimit     int
	batchByteSize          int
	concurrentRequest      int
	batchLen               int
	flushLock              sync.Mutex
	isDcpRebalancing       bool
	forceFlush             bool
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
	elasticClient := client.NewElasticClient(config)

	readers := make([]*helper.BatchKeyDataReader, config.Elasticsearch.ConcurrentRequest)
	for i := 0; i < config.Elasticsearch.ConcurrentRequest; i++ {
		readers[i] = helper.NewBatchKeyDataReader(nil, nil)
	}

	bulk := &Bulk{
		batch:                  make([]byte, config.Elasticsearch.BatchByteSizeLimit),
		batchTickerDuration:    config.Elasticsearch.BatchTickerDuration,
		batchTicker:            time.NewTicker(config.Elasticsearch.BatchTickerDuration),
		batchSizeLimit:         config.Elasticsearch.BatchSizeLimit,
		batchByteSizeLimit:     config.Elasticsearch.BatchByteSizeLimit,
		logger:                 logger,
		errorLogger:            errorLogger,
		dcpCheckpointCommit:    dcpCheckpointCommit,
		elasticClient:          elasticClient,
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

//nolint:staticcheck
func (b *Bulk) PrepareStartRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = true
	b.batch = b.batch[:cap(b.batch)]
	b.batchKeys = make(map[string]int, b.batchSizeLimit)
	b.batchKeyData = b.batchKeyData[:0]
	b.batchLen = 0
	b.batchKeyDataIndex = 0
	b.batchSize = 0
	b.batchByteSize = 0
	b.forceFlush = false
}

func (b *Bulk) PrepareEndRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = false
}

func (b *Bulk) copyToBatch(src []byte) bool {
	if len(src) > b.batchByteSizeLimit-b.batchLen {
		return true
	}

	b.batchLen += copy(b.batch[b.batchLen:], src)
	return false
}

func (b *Bulk) eolToBatch() bool {
	if 1 > b.batchByteSizeLimit-b.batchLen {
		return true
	}

	b.batchLen += copy(b.batch[b.batchLen:], "\n")
	return false
}

func (b *Bulk) addActionToBatch(action document.ESActionDocument, collectionName string) bool {
	var overflow bool
	if action.Type == document.Index {
		overflow = b.copyToBatch(indexPrefix)
	} else {
		overflow = b.copyToBatch(deletePrefix)
	}
	if !overflow {
		overflow = b.copyToBatch(helper.Byte(b.collectionIndexMapping[collectionName]))
	}
	if !overflow {
		overflow = b.copyToBatch(idPrefix)
	}
	if !overflow {
		overflow = b.copyToBatch(action.ID)
	}
	if action.Routing != nil {
		if !overflow {
			overflow = b.copyToBatch(routingPrefix)
		}
		if !overflow {
			overflow = b.copyToBatch(helper.Byte(*action.Routing))
		}
	}
	if b.typeName != nil {
		if !overflow {
			overflow = b.copyToBatch(typePrefix)
		}
		if !overflow {
			overflow = b.copyToBatch(b.typeName)
		}
	}
	if !overflow {
		overflow = b.copyToBatch(postFix)
	}
	if action.Type == document.Index {
		if !overflow {
			overflow = b.eolToBatch()
		}
		if !overflow {
			overflow = b.copyToBatch(action.Source)
		}
	}
	if !overflow {
		overflow = b.eolToBatch()
	}

	return overflow
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

	var remainingActions []document.ESActionDocument

	for index, action := range actions {
		key := helper.String(action.ID)
		startIndex := b.batchLen
		overflow := b.addActionToBatch(action, collectionName)

		if overflow {
			b.batchLen = startIndex
			b.batch = b.batch[:b.batchLen]
			remainingActions = actions[index:]
			b.forceFlush = true
			break
		} else {
			size := b.batchLen - startIndex
			batchKeyData := helper.BatchKeyData{Index: startIndex, Size: size}

			if batchIndex, ok := b.batchKeys[key]; ok {
				b.batchKeyData[batchIndex] = batchKeyData
			} else {
				b.batchKeyData = append(b.batchKeyData, batchKeyData)
				b.batchKeys[key] = b.batchKeyDataIndex
				b.batchKeyDataIndex++
			}

			b.batchByteSize += size
		}
	}
	ctx.Ack()

	b.batchSize += len(actions) - len(remainingActions)

	b.flushLock.Unlock()

	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	if b.forceFlush || b.batchSize >= b.batchSizeLimit || b.batchLen >= b.batchByteSizeLimit {
		b.flushMessages()
		if len(remainingActions) > 0 {
			b.AddActions(ctx, eventTime, remainingActions, collectionName)
		}
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

func (b *Bulk) Close() {
	b.batchTicker.Stop()

	b.flushMessages()
}

//nolint:staticcheck
func (b *Bulk) flushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if b.isDcpRebalancing {
		return
	}
	if b.batchLen > 0 {
		err := b.bulkRequest()
		if err != nil {
			panic(err)
		}
		b.batchTicker.Reset(b.batchTickerDuration)
		b.batch = b.batch[:cap(b.batch)]
		b.batchKeys = make(map[string]int, b.batchSizeLimit)
		b.batchKeyData = b.batchKeyData[:0]
		b.batchLen = 0
		b.batchKeyDataIndex = 0
		b.batchSize = 0
		b.batchByteSize = 0
		b.forceFlush = false
	}

	b.dcpCheckpointCommit()
}

func (b *Bulk) requestFunc(concurrentRequestIndex int, batch []helper.BatchKeyData) func() error {
	return func() error {
		reader := b.readers[concurrentRequestIndex]
		reader.Reset(b.batch, batch)
		size := 0
		for _, i := range batch {
			size += i.Size
		}
		r, err := b.elasticClient.Bulk(reader, size)
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

	chunks := helpers.ChunkSlice(b.batchKeyData, b.concurrentRequest)

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

func hasResponseError(r []byte) error {
	if r == nil {
		return fmt.Errorf("bulk response is nil")
	}
	b := make(map[string]any)
	err := jsoniter.Unmarshal(r, &b)
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
