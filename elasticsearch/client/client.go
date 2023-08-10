package client

import (
	"errors"
	"io"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/valyala/fasthttp"
)

type ElasticClient interface {
	Bulk(bodyStream io.Reader, bodySize int) ([]byte, error)
}

type elasticClient struct {
	url                string
	compressionEnabled bool
}

func (e *elasticClient) Bulk(bodyStream io.Reader, bodySize int) ([]byte, error) {
	req := fasthttp.AcquireRequest()
	req.SetBodyStream(bodyStream, bodySize)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")
	req.SetRequestURI(e.url + "/_bulk")

	res := fasthttp.AcquireResponse()

	if err := fasthttp.Do(req, res); err != nil {
		return nil, err
	}

	fasthttp.ReleaseRequest(req)

	body := res.Body()

	fasthttp.ReleaseResponse(res)

	return body, nil
}

func NewElasticClient(config *config.Config) ElasticClient {
	if len(config.Elasticsearch.Urls) < 1 {
		panic(errors.New("invalid elasticsearch urls"))
	}

	return &elasticClient{
		url:                config.Elasticsearch.Urls[0],
		compressionEnabled: config.Elasticsearch.CompressionEnabled,
	}
}
