package bulk

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/config"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/mock"
)

// nolint:staticcheck
func TestProcessor_StartBulk(t *testing.T) {
	// given
	c := &config.Config{
		Elasticsearch: config.Elasticsearch{
			BatchSizeLimit:      1000,
			BatchByteSizeLimit:  1000,
			BatchTickerDuration: 1,
		},
	}
	dcpCheckpointCommit := func() {}

	mockEsClient := mock.NewMockEsClient()
	mockEsClient.OnBulk(func(reader *bytes.Reader) error {
		return nil
	})

	processor, err := NewProcessor(c, logger.Log, logger.Log, dcpCheckpointCommit, mockEsClient)
	if err != nil {
		t.Fatalf("Failed to create Processor: %v", err)
	}

	mockListenerContext := mock.NewMockListenerContext()

	processor.AddActions(&models.ListenerContext{
		Commit: mockListenerContext.Commit,
		Event:  nil,
		Ack:    mockListenerContext.Ack,
	}, time.Now(), make([]elasticsearch.ActionDocument, 100), "example-collection")
	time.Sleep(100)

	// when
	go processor.StartBulk()

	// then
	if !reflect.DeepEqual(true, mockEsClient.BulkFnCalled) {
		t.Errorf("Bulk should be called")
	}

	if !reflect.DeepEqual(true, mockListenerContext.AckCalled) {
		t.Errorf("Ack should be called")
	}
}

func TestProcessor_AddActions(t *testing.T) {
	// given
	c := &config.Config{
		Elasticsearch: config.Elasticsearch{
			BatchSizeLimit:      1,
			BatchByteSizeLimit:  100,
			BatchTickerDuration: 1,
		},
	}

	dcpCheckpointCommit := func() {}

	mockEsClient := mock.NewMockEsClient()
	mockEsClient.OnBulk(func(reader *bytes.Reader) error {
		if reader.Len() < 100 {
			t.Errorf("Length of bulk is less than limit. got %v, expected %v", reader.Len(), c.Elasticsearch.BatchByteSizeLimit)
		}

		return nil
	})

	processor, err := NewProcessor(c, logger.Log, logger.Log, dcpCheckpointCommit, mockEsClient)
	if err != nil {
		t.Fatalf("Failed to create Processor: %v", err)
	}

	mockListenerContext := mock.NewMockListenerContext()

	// when
	processor.AddActions(&models.ListenerContext{
		Commit: mockListenerContext.Commit,
		Event:  nil,
		Ack:    mockListenerContext.Ack,
	}, time.Now(), make([]elasticsearch.ActionDocument, 100), "example-collection")

	// then
	if !reflect.DeepEqual(true, mockEsClient.BulkFnCalled) {
		t.Errorf("Bulk should be called")
	}

	if !reflect.DeepEqual(true, mockListenerContext.AckCalled) {
		t.Errorf("Ack should be called")
	}
}

func TestProcessor_AddActions_EsClient_Return_Err(t *testing.T) {
	// given
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, but none occurred")
		}
	}()

	c := &config.Config{
		Elasticsearch: config.Elasticsearch{
			BatchSizeLimit:      1,
			BatchByteSizeLimit:  100,
			BatchTickerDuration: 1,
		},
	}

	dcpCheckpointCommit := func() {}

	mockEsClient := mock.NewMockEsClient()
	mockEsClient.OnBulk(func(reader *bytes.Reader) error {
		return errors.New("es client error")
	})

	processor, err := NewProcessor(c, logger.Log, logger.Log, dcpCheckpointCommit, mockEsClient)
	if err != nil {
		t.Fatalf("Failed to create Processor: %v", err)
	}

	mockListenerContext := mock.NewMockListenerContext()

	// when
	processor.AddActions(&models.ListenerContext{
		Commit: mockListenerContext.Commit,
		Event:  nil,
		Ack:    mockListenerContext.Ack,
	}, time.Now(), make([]elasticsearch.ActionDocument, 100), "example-collection")

	// then
	if !reflect.DeepEqual(true, mockEsClient.BulkFnCalled) {
		t.Errorf("Bulk should be called")
	}

	if !reflect.DeepEqual(true, mockListenerContext.AckCalled) {
		t.Errorf("Ack should be called")
	}
}

func TestProcessor_Close(t *testing.T) {
	// given
	c := &config.Config{
		Elasticsearch: config.Elasticsearch{
			BatchSizeLimit:      1,
			BatchByteSizeLimit:  1,
			BatchTickerDuration: 1,
		},
	}
	dcpCheckpointCommit := func() {}

	processor, err := NewProcessor(c, logger.Log, logger.Log, dcpCheckpointCommit, nil)
	if err != nil {
		t.Fatalf("Failed to create Processor: %v", err)
	}

	// when
	processor.Close()
}

func TestProcessor_GetMetric(t *testing.T) {
	// given
	c := &config.Config{
		Elasticsearch: config.Elasticsearch{
			BatchSizeLimit:      1,
			BatchByteSizeLimit:  1,
			BatchTickerDuration: 1,
		},
	}
	dcpCheckpointCommit := func() {}

	processor, err := NewProcessor(c, logger.Log, logger.Log, dcpCheckpointCommit, nil)
	if err != nil {
		t.Fatalf("Failed to create Processor: %v", err)
	}

	// when
	metric := processor.GetMetric()

	// then
	if metric == nil {
		t.Errorf("Metric shouldn't be nil")
	}
}
