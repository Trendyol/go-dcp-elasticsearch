package integration

import (
	"context"
	dcpelasticsearch "github.com/Trendyol/go-dcp-elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"github.com/elastic/go-elasticsearch/v7"
	jsoniter "github.com/json-iterator/go"
	"sync"
	"testing"
	"time"
)

func Mapper(event couchbase.Event) []document.ESActionDocument {
	if event.IsMutated {
		return []document.ESActionDocument{document.NewIndexAction(event.Key, event.Value, nil)}
	}
	return nil
}

func BenchmarkElasticsearch(t *testing.B) {
	connector, err := dcpelasticsearch.NewConnectorBuilder("config.yml").SetMapper(Mapper).Build()
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		connector.Start()
	}()

	go func() {
		time.Sleep(40 * time.Second)
		es, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses: []string{"http://localhost:9200"},
		})
		if err != nil {
			t.Fatalf("could not open connection to elasticsearch %s", err)
		}

		ctx, _ := context.WithTimeout(context.Background(), 3*time.Minute)

	CountCheckLoop:
		for {
			select {
			case <-ctx.Done():
				t.Fatalf("deadline exceed")
			default:
				response, err := es.Count(
					es.Count.WithIndex("test"),
				)
				if err != nil {
					t.Fatalf("could not get count from elasticsearch %s", err)
				}
				var countResponse CountResponse
				err = jsoniter.NewDecoder(response.Body).Decode(&countResponse)
				if err != nil {
					t.Fatalf("could not decode response from elasticsearch %s", err)
				}
				if countResponse.Count == 31591 {
					connector.Close()
					goto CountCheckLoop
				}
				time.Sleep(2 * time.Second)
			}
		}

	}()

	wg.Wait()
	t.Log("done")
}

type CountResponse struct {
	Count int64 `json:"count"`
}
