package bulk

import (
	"errors"
	"fmt"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"reflect"
	"testing"
)

func Test_getActions(t *testing.T) {
	// Given
	givenBatchItems := []BatchItem{
		{Action: &document.ESActionDocument{ID: []byte("1")}},
		{Action: &document.ESActionDocument{ID: []byte("2")}},
	}

	expected := []*document.ESActionDocument{
		{ID: []byte("1")},
		{ID: []byte("2")},
	}

	// When
	result := getActions(givenBatchItems)

	// Then
	if !reflect.DeepEqual(expected, result) {
		t.Fatal("Must be equal")
	}
}

func TestBulk_executeSinkResponseHandler(t *testing.T) {
	t.Run("Should_Ignore_When_Not_Sink_Response_Handler_Specified", func(t *testing.T) {
		// Given
		sut := Bulk{}

		// When
		sut.executeSinkResponseHandler(nil, nil)

		// Then
	})
	t.Run("Should_Handle_Success_And_Failure_Messages", func(t *testing.T) {
		// Given
		handler := mockSinkResponseHandler{}

		sut := Bulk{
			sinkResponseHandler: &handler,
			metric: &Metric{
				IndexingSuccessActionCounter: map[string]int64{},
				IndexingErrorActionCounter:   map[string]int64{},
			},
		}

		batchActions := []*document.ESActionDocument{
			{IndexName: "someIndex", ID: []byte("1"), Type: document.Index},
			{IndexName: "someIndex", ID: []byte("2"), Type: document.Index},
		}

		errorData := map[string]string{
			"2:someIndex": "a error occurred",
		}

		// When
		sut.executeSinkResponseHandler(batchActions, errorData)

		// Then
		if handler.successResult != "1:someIndex" {
			t.Fatalf("Success Result must be `someIndex:1` but has %s", handler.successResult)
		}
		if sut.metric.IndexingSuccessActionCounter["someIndex"] != 1 {
			t.Fatalf("Indexing success action counter must equal to 1 but has %d", sut.metric.IndexingSuccessActionCounter["someIndex"])
		}
		if handler.errorResult != "2:someIndex Error=a error occurred" {
			t.Fatalf("Success Result must be `2:someIndex Error=a error occurred` but has %s", handler.errorResult)
		}
		if sut.metric.IndexingErrorActionCounter["someIndex"] != 1 {
			t.Fatalf("Indexing error action counter must equal to 1 but has %d", sut.metric.IndexingErrorActionCounter["someIndex"])
		}
	})
}

func Test_fillErrorDataWithBulkRequestError(t *testing.T) {
	// Given
	batchActions := []*document.ESActionDocument{
		{IndexName: "someIndex", ID: []byte("1"), Type: document.Index},
		{IndexName: "someIndex", ID: []byte("2"), Type: document.Index},
	}

	// When
	result := fillErrorDataWithBulkRequestError(batchActions, errors.New("bulk request error, 400"))

	// Then
	if result["1:someIndex"] != "bulk request error, 400" {
		t.Fatalf("result' key `1:someIndex` must equal to `bulk request error, 400` but has %s", result["1:someIndex"])
	}
	if result["2:someIndex"] != "bulk request error, 400" {
		t.Fatalf("result' key `2:someIndex` must equal to `bulk request error, 400` but has %s", result["2:someIndex"])
	}
}

type mockSinkResponseHandler struct {
	successResult string
	errorResult   string
}

func (m *mockSinkResponseHandler) OnSuccess(ctx *elasticsearch.SinkResponseHandlerContext) {
	m.successResult = fmt.Sprintf("%s:%s", ctx.Action.ID, ctx.Action.IndexName)
}
func (m *mockSinkResponseHandler) OnError(ctx *elasticsearch.SinkResponseHandlerContext) {
	m.errorResult = fmt.Sprintf("%s:%s Error=%s", ctx.Action.ID, ctx.Action.IndexName, ctx.Err.Error())
}

func (m *mockSinkResponseHandler) OnInit(ctx *elasticsearch.SinkResponseHandlerInitContext) {}
