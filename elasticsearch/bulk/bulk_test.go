package bulk

import (
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
)

func Test_getEsActionJSON(t *testing.T) {
	t.Run("index_actions", testIndexActions)
	t.Run("delete_actions", testDeleteActions)
	t.Run("update_actions", testUpdateActions)
	t.Run("script_update_actions", testScriptUpdateActions)
}

func testIndexActions(t *testing.T) {
	t.Run("Should_Generate_Index_Action_JSON", func(t *testing.T) {
		// Given
		docID := []byte(testDocID)
		action := document.Index
		source := []byte(testSimpleDoc)
		var routing *string
		var typeName []byte

		// When
		actionJSON := getEsActionJSON(docID, action, testIndexName, routing, source, typeName)

		// Then
		expectedAction := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, testIndexName, testDocID) + "\n" + testSimpleDoc + "\n"
		assertJSONEqual(t, expectedAction, string(actionJSON))
	})

	t.Run("Should_Generate_Index_Action_JSON", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.Index
		indexName := testIndexName
		source := []byte(testSimpleDoc)
		var routing *string
		var typeName []byte

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, routing, source, typeName)

		// Then
		expectedAction := `{"index":{"_index":"test-index","_id":"123"}}` + "\n" + `{"name":"test"}` + "\n"
		if string(actionJSON) != expectedAction {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})

	t.Run("Should_Include_Routing_When_Provided", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.Index
		indexName := testIndexName
		source := []byte(testSimpleDoc)
		routing := testRouting
		var typeName []byte

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, &routing, source, typeName)

		// Then
		expectedAction := `{"index":{"_index":"test-index","_id":"123","routing":"shard-1"}}` + "\n" + `{"name":"test"}` + "\n"
		if string(actionJSON) != expectedAction {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})

	t.Run("Should_Include_Type_When_Provided", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.Index
		indexName := testIndexName
		source := []byte(testSimpleDoc)
		var routing *string
		typeName := []byte("_doc")

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, routing, source, typeName)

		// Then
		expectedAction := `{"index":{"_index":"test-index","_id":"123","_type":"_doc"}}` + "\n" + `{"name":"test"}` + "\n"
		if string(actionJSON) != expectedAction {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})
}

func testDeleteActions(t *testing.T) {
	t.Run("Should_Generate_Delete_Action_JSON", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.Delete
		indexName := testIndexName
		var source []byte
		var routing *string
		var typeName []byte

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, routing, source, typeName)

		// Then
		expectedAction := `{"delete":{"_index":"test-index","_id":"123"}}` + "\n"
		if string(actionJSON) != expectedAction {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})

	t.Run("Should_Generate_Delete_Action_JSON_With_Routing_And_Type", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.Delete
		indexName := testIndexName
		var source []byte
		routing := testRouting
		typeName := []byte("_doc")

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, &routing, source, typeName)

		// Then
		expectedAction := `{"delete":{"_index":"test-index","_id":"123","routing":"shard-1","_type":"_doc"}}` + "\n"
		if string(actionJSON) != expectedAction {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})
}

func testUpdateActions(t *testing.T) {
	updatedDocJson := `{"doc":{"name":"updated"}, "doc_as_upsert":true}`

	t.Run("Should_Generate_Update_Action_JSON", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.DocUpdate
		indexName := testIndexName
		source := []byte(testUpdatedDoc)
		var routing *string
		var typeName []byte

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, routing, source, typeName)

		// Then
		expectedAction := updateActionMeta + "\n" + updatedDocJson + "\n"
		if string(actionJSON) != expectedAction {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})

	t.Run("Should_Generate_DocUpdate_Action_JSON_With_Routing", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.DocUpdate
		indexName := testIndexName
		source := []byte(testUpdatedDoc)
		routing := testRouting
		var typeName []byte

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, &routing, source, typeName)

		// Then
		expectedAction := `{"update":{"_index":"test-index","_id":"123","routing":"shard-1"}}` + "\n" +
			updatedDocJson + "\n"
		if string(actionJSON) != expectedAction {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})

	t.Run("Should_Generate_DocUpdate_Action_JSON_With_Routing_And_Type", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.DocUpdate
		indexName := testIndexName
		source := []byte(testUpdatedDoc)
		routing := testRouting
		typeName := []byte("_doc")

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, &routing, source, typeName)

		// Then
		expectedAction := `{"update":{"_index":"test-index","_id":"123","routing":"shard-1","_type":"_doc"}}` + "\n" +
			updatedDocJson + "\n"
		if string(actionJSON) != expectedAction {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})
}

func testScriptUpdateActions(t *testing.T) {

	t.Run("Should_Generate_ScriptUpdate_Action_JSON_With_Routing_And_Type", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.ScriptUpdate
		indexName := testIndexName
		script := []byte(`{"source": "ctx._source.counter += 1","lang": "painless"}`)
		routing := testRouting
		typeName := []byte("_doc")

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, &routing, script, typeName)

		// Then
		expectedAction := `{"update":{"_index":"test-index","_id":"123","routing":"shard-1","_type":"_doc"}}` + "\n" +
			`{"script":{"source":"ctx._source.counter += 1","lang":"painless"},"scripted_upsert":true}` + "\n"

		normalizedActual := strings.Join(strings.Fields(string(actionJSON)), "")
		normalizedExpected := strings.Join(strings.Fields(expectedAction), "")

		if normalizedActual != normalizedExpected {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})

	t.Run("Should_Generate_Script_Update_Action_JSON", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.ScriptUpdate
		indexName := testIndexName
		script := []byte(`{
			"source": "ctx._source.counter += params.count",
			"lang": "painless",
			"params": {
				"count": 1
			}
		}`)
		var routing *string
		var typeName []byte

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, routing, script, typeName)

		// Then
		expectedAction := updateActionMeta + "\n" +
			`{"script":{"source":"ctx._source.counter += params.count","lang":"painless","params":{"count":1}},"scripted_upsert":true}` + "\n"

		normalizedActual := strings.Join(strings.Fields(string(actionJSON)), "")
		normalizedExpected := strings.Join(strings.Fields(expectedAction), "")

		if normalizedActual != normalizedExpected {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})

	t.Run("Should_Generate_Script_Update_Action_JSON_With_Routing", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.ScriptUpdate
		indexName := testIndexName
		script := []byte(`{
			"source": "ctx._source.tags.add(params.tag)",
			"lang": "painless",
			"params": {
				"tag": "new-tag"
			}
		}`)
		routing := testRouting
		var typeName []byte

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, &routing, script, typeName)

		// Then
		expectedAction := `{"update":{"_index":"test-index","_id":"123","routing":"shard-1"}}` + "\n" +
			`{"script":{"source":"ctx._source.tags.add(params.tag)","lang":"painless","params":{"tag":"new-tag"}},"scripted_upsert":true}` + "\n"

		normalizedActual := strings.Join(strings.Fields(string(actionJSON)), "")
		normalizedExpected := strings.Join(strings.Fields(expectedAction), "")

		if normalizedActual != normalizedExpected {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})

	t.Run("Should_Generate_Script_Update_Action_JSON_With_Complex_Script", func(t *testing.T) {
		// Given
		docID := []byte("123")
		action := document.ScriptUpdate
		indexName := testIndexName
		script := []byte(`{
			"source": "if (ctx._source.containsKey('items')) { ctx._source.items.add(params.item) } else { ctx._source.items = [params.item] }",
			"lang": "painless",
			"params": {
				"item": {"id": 1, "name": "test"}
			}
		}`)
		var routing *string
		var typeName []byte

		// When
		actionJSON := getEsActionJSON(docID, action, indexName, routing, script, typeName)

		// Then
		expectedAction := updateActionMeta + "\n" +
			`{"script":{"source":"if (ctx._source.containsKey('items')) { ctx._source.items.add(params.item) } else { ctx._source.items = [params.item] }",` +
			`"lang":"painless","params":{"item":{"id":1,"name":"test"}}},"scripted_upsert":true}` + "\n"

		normalizedActual := strings.Join(strings.Fields(string(actionJSON)), "")
		normalizedExpected := strings.Join(strings.Fields(expectedAction), "")

		if normalizedActual != normalizedExpected {
			t.Errorf("Expected action JSON %s, got %s", expectedAction, string(actionJSON))
		}
	})
}

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

func assertJSONEqual(t *testing.T, expected, actual string) {
	t.Helper()
	normalizedExpected := strings.Join(strings.Fields(expected), "")
	normalizedActual := strings.Join(strings.Fields(actual), "")
	if normalizedActual != normalizedExpected {
		t.Errorf("Expected JSON %s, got %s", expected, actual)
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
