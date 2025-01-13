package bulk

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
)

const (
	testIndexName    = "test-index"
	testDocID        = "123"
	testRouting      = "shard-1"
	testSimpleDoc    = `{"name":"test"}`
	testUpdatedDoc   = `{"name":"updated"}`
	updateActionMeta = `{"update":{"_index":"test-index","_id":"123"}}`
	scriptTemplate   = `{"source":"if (ctx._source.containsKey('items')) { ctx._source.items.add(params.item) } else { ctx._source.items = [params.item] }","lang":"painless","params":{"item":{"id":1,"name":"test"}}}`
)

func Test_getEsActionJSON(t *testing.T) {
	t.Run("basic_index_actions", testBasicIndexActions)
	t.Run("routing_index_actions", testRoutingIndexActions)
	t.Run("type_index_actions", testTypeIndexActions)
	t.Run("delete_actions", testDeleteActions)
	t.Run("update_actions", testUpdateActions)
	t.Run("script_update_actions", testScriptUpdateActions)
}

func testBasicIndexActions(t *testing.T) {
	docID := []byte(testDocID)
	action := document.Index
	source := []byte(testSimpleDoc)
	var routing *string
	var typeName []byte

	actionJSON := getEsActionJSON(docID, action, testIndexName, routing, source, typeName)

	expectedAction := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, testIndexName, testDocID) + "\n" + testSimpleDoc + "\n"
	assertJSONEqual(t, expectedAction, string(actionJSON))
}

func testRoutingIndexActions(t *testing.T) {
	docID := []byte(testDocID)
	action := document.Index
	source := []byte(testSimpleDoc)
	routing := testRouting
	var typeName []byte

	actionJSON := getEsActionJSON(docID, action, testIndexName, &routing, source, typeName)

	expectedAction := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s","routing":"%s"}}`, testIndexName, testDocID, routing) + "\n" + testSimpleDoc + "\n"
	assertJSONEqual(t, expectedAction, string(actionJSON))
}

func testTypeIndexActions(t *testing.T) {
	docID := []byte(testDocID)
	action := document.Index
	source := []byte(testSimpleDoc)
	var routing *string
	typeName := []byte("_doc")

	actionJSON := getEsActionJSON(docID, action, testIndexName, routing, source, typeName)

	expectedAction := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s","_type":"_doc"}}`, testIndexName, testDocID) + "\n" + testSimpleDoc + "\n"
	assertJSONEqual(t, expectedAction, string(actionJSON))
}

func testDeleteActions(t *testing.T) {
	t.Run("basic_delete", func(t *testing.T) {
		docID := []byte(testDocID)
		action := document.Delete
		var source []byte
		var routing *string
		var typeName []byte

		actionJSON := getEsActionJSON(docID, action, testIndexName, routing, source, typeName)

		expectedAction := fmt.Sprintf(`{"delete":{"_index":"%s","_id":"%s"}}`, testIndexName, testDocID) + "\n"
		assertJSONEqual(t, expectedAction, string(actionJSON))
	})
}

func testUpdateActions(t *testing.T) {
	t.Run("basic_update", func(t *testing.T) {
		docID := []byte(testDocID)
		action := document.DocUpdate
		source := []byte(testUpdatedDoc)
		var routing *string
		var typeName []byte

		actionJSON := getEsActionJSON(docID, action, testIndexName, routing, source, typeName)

		expectedAction := updateActionMeta + "\n" +
			fmt.Sprintf(`{"doc":%s, "doc_as_upsert":true}`, testUpdatedDoc) + "\n"
		assertJSONEqual(t, expectedAction, string(actionJSON))
	})
}

func testScriptUpdateActions(t *testing.T) {
	t.Run("basic_script_update", testBasicScriptUpdate)
}

func testBasicScriptUpdate(t *testing.T) {
	docID := []byte(testDocID)
	action := document.ScriptUpdate
	script := []byte(scriptTemplate)
	var routing *string
	var typeName []byte

	actionJSON := getEsActionJSON(docID, action, testIndexName, routing, script, typeName)

	expectedAction := fmt.Sprintf(`{"update":{"_index":"%s","_id":"%s"}}`, testIndexName, testDocID) + "\n" +
		fmt.Sprintf(`{"script":%s,"scripted_upsert":true}`, scriptTemplate) + "\n"

	assertJSONEqual(t, expectedAction, string(actionJSON))
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

func Test_getActions(t *testing.T) {
	givenBatchItems := []BatchItem{
		{Action: &document.ESActionDocument{ID: []byte("1")}},
		{Action: &document.ESActionDocument{ID: []byte("2")}},
	}

	expected := []*document.ESActionDocument{
		{ID: []byte("1")},
		{ID: []byte("2")},
	}

	result := getActions(givenBatchItems)

	if !reflect.DeepEqual(expected, result) {
		t.Fatal("Must be equal")
	}
}

func assertJSONEqual(t *testing.T, expected, actual string) {
	t.Helper()
	expectedParts := strings.Split(strings.TrimSpace(expected), "\n")
	actualParts := strings.Split(strings.TrimSpace(actual), "\n")

	if len(expectedParts) != len(actualParts) {
		t.Fatalf("Number of lines differ. Expected %d lines, got %d lines", len(expectedParts), len(actualParts))
	}

	for i := 0; i < len(expectedParts); i++ {
		expectedLine := strings.TrimSpace(expectedParts[i])
		actualLine := strings.TrimSpace(actualParts[i])

		if expectedLine == "" && actualLine == "" {
			continue
		}

		var expectedObj, actualObj interface{}
		if err := json.Unmarshal([]byte(expectedLine), &expectedObj); err != nil {
			t.Fatalf("Failed to parse expected JSON line %d: %v", i+1, err)
		}
		if err := json.Unmarshal([]byte(actualLine), &actualObj); err != nil {
			t.Fatalf("Failed to parse actual JSON line %d: %v", i+1, err)
		}

		if !reflect.DeepEqual(expectedObj, actualObj) {
			t.Errorf("Line %d differs.\nExpected: %s\nGot: %s", i+1, expectedLine, actualLine)
		}
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
