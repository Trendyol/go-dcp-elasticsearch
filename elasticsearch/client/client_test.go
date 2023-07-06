package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	dcpcf "github.com/Trendyol/go-dcp-client/config"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/config"
	_ "github.com/Trendyol/go-elasticsearch-connect-couchbase/config"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"io/ioutil"
	"log"
	"net/http"
	"testing"

	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch/client"
	"github.com/stretchr/testify/assert"
)

var esClient = createClient()

type esResponse struct {
	ID          string                 `json:"_id"`
	Index       string                 `json:"_index"`
	PrimaryTerm float64                `json:"_primary_term"`
	SeqNo       float64                `json:"_seq_no"`
	Source      map[string]interface{} `json:"_source"`
	Type        string                 `json:"_type"`
	Version     float64                `json:"_version"`
	Found       bool                   `json:"found"`
}

func TestBulkIntegration(t *testing.T) {
	index := "my_index"
	deleteIndex(index)
	createIndex(index)

	c, _ := client.New(&config.Config{
		Elasticsearch: config.Elasticsearch{
			CollectionIndexMapping: map[string]string{"default": "my_index"},
		},
		Dcp: dcpcf.Dcp{},
	})

	err := c.Bulk(createBulkRequest())
	assert.NoError(t, err)

	// Retrieve the indexed document to verify the insertion
	resp, err := http.Get("http://localhost:9200/my_index/_doc/1")
	assert.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)

	var actual esResponse
	err = json.Unmarshal(respBody, &actual)
	if err != nil {
		// Handle unmarshal error
	}

	expected := esResponse{
		ID:          "1",
		Index:       "my_index",
		PrimaryTerm: 1,
		SeqNo:       0,
		Source:      map[string]interface{}{"foo": "bar"},
		Type:        "_doc",
		Version:     1,
		Found:       true,
	}

	assert.Equal(t, expected, actual)
}

func createBulkRequest() *bytes.Reader {
	doc := map[string]interface{}{
		"foo": "bar",
	}

	docBytes, _ := json.Marshal(doc)

	bulkReq := bytes.Buffer{}

	// Write the `index` object to the buffer.
	bulkReq.WriteString(`{"index": {"_index": "my_index", "_id": "1"}}`)
	bulkReq.WriteByte('\n')

	// Write the document bytes to the buffer.
	bulkReq.Write(docBytes)
	bulkReq.WriteByte('\n')

	// Create a `bytes.Reader` from the `bytes.Buffer`.
	reader := bytes.NewReader(bulkReq.Bytes())

	// Return the `bytes.Reader`.
	return reader
}

func createClient() *elasticsearch.Client {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"}, // Replace with your Elasticsearch URL
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	return es
}

func createIndex(indexName string) {
	req := esapi.IndicesCreateRequest{
		Index: indexName,
	}

	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		log.Fatalf("Error executing the request: %s", err)
	}

	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error creating index: %s", res.String())
	} else {
		log.Println("Index created successfully")
	}
}

func deleteIndex(indexName string) {
	req := esapi.IndicesDeleteRequest{
		Index: []string{indexName},
	}

	// Perform the request
	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		log.Fatalf("Err while requesting elasticsearch: %s", err)
	}

	defer res.Body.Close()

	// Check the response status
	if res.IsError() {
		log.Fatalf("Elasticsearch error response: %s", res.String())
	}
}
