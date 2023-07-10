package client

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"

	"github.com/Trendyol/go-elasticsearch-connect-couchbase/config"
)

type ESClient interface {
	Bulk(reader *bytes.Reader) error
}

type client struct {
	esClient *elasticsearch.Client
}

func New(cfg *config.Config) (ESClient, error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		MaxRetries:           math.MaxInt,
		Addresses:            cfg.Elasticsearch.Urls,
		DiscoverNodesOnStart: true,
		Transport:            newTransport(cfg.Elasticsearch),
		CompressRequestBody:  cfg.Elasticsearch.CompressionEnabled,
	})
	if err != nil {
		return nil, err
	}

	return &client{
		esClient: es,
	}, nil
}

func (c *client) Bulk(reader *bytes.Reader) error {
	r, err := c.esClient.Bulk(reader)
	if err != nil {
		return err
	}

	err = hasResponseError(r)
	if err != nil {
		return err
	}

	return nil
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
