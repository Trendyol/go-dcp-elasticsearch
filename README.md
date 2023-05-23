# Go Elasticsearch Connect Couchbase

[![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-elasticsearch-connect-couchbase.svg)](https://pkg.go.dev/github.com/Trendyol/go-elasticsearch-connect-couchbase) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-elasticsearch-connect-couchbase)](https://goreportcard.com/report/github.com/Trendyol/go-elasticsearch-connect-couchbase)

Go implementation of
the [Elasticsearch Connect Couchbase](https://github.com/couchbase/couchbase-elasticsearch-connector).

**Go Elasticsearch Connect Couchbase** streams documents from Couchbase Database Change Protocol (DCP) and writes to
Elasticsearch index in near real-time.

## Features

* **Less resource usage** and **higher throughput**(see [Benchmarks](#benchmarks)).
* **Custom routing support**(see [Example](#example)).
* **Update multiple documents** for a DCP event**(see [Example](#example)).
* Easier to handle different DCP events such as **expiration, deletion and mutation**(see [Example](#example)).
* **Elasticsearch compression request body** support.
* **Easier to manage batch configurations** such as maximum batch size, batch bytes, batch ticker durations.
* **Easier to scale up and down** by custom membership algorithms(Couchbase, KubernetesHa, Kubernetes StatefulSet or
  Static, see [examples](https://github.com/Trendyol/go-dcp-client#examples)).
* **Easier to configure**.

## Benchmarks

TODO
| Package | Time | Time % to | Objects Allocated |
| :------ | :--: | :-----------: | :---------------: |
| Java Elasticsearch Connect Couchbase | 1744 ns/op | +0% | 5 allocs/op
| **Go Elasticsearch Connect Couchbase** | 2483 ns/op | +42% | 10 allocs/op

## Example

```go
package main

import (
	goelasticsearchconnectcouchbase "github.com/Trendyol/go-elasticsearch-connect-couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch/document"
)

func mapper(event couchbase.Event) []document.ESActionDocument {
	if event.IsMutated {
		e := document.NewIndexAction(event.Key, event.Value, nil)
		return []document.ESActionDocument{e}
	}
	e := document.NewDeleteAction(event.Key, nil)
	return []document.ESActionDocument{e}
}

func main() {
	connector, err := goelasticsearchconnectcouchbase.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		Build()
	if err != nil {
		return
	}

	defer connector.Close()
	connector.Start()
}

```

Custom log structures can be used with the connector

```go
package main

import (
	goelasticsearchconnectcouchbase "github.com/Trendyol/go-elasticsearch-connect-couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch/document"
	"log"
	"os"
)

func mapper(event couchbase.Event) []document.ESActionDocument {
	if event.IsMutated {
		e := document.NewIndexAction(event.Key, event.Value, nil)
		return []document.ESActionDocument{e}
	}
	e := document.NewDeleteAction(event.Key, nil)
	return []document.ESActionDocument{e}
}

func main() {
	logger := log.New(os.Stdout, "cb2elastic: ", log.Ldate|log.Ltime|log.Llongfile)

	connector, err := goelasticsearchconnectcouchbase.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		SetLogger(logger).
		SetErrorLogger(logger).
		Build()
	if err != nil {
		return
	}

	defer connector.Close()
	connector.Start()
}

```

## Configuration

### Dcp Configuration

Check out on [go-dcp-client](https://github.com/Trendyol/go-dcp-client#configuration)

### Elasticsearch Specific Configuration

| Variable                               | Type              | Required | Default  | Description                                                                                         |                                                           
|----------------------------------------|-------------------|----------|----------|-----------------------------------------------------------------------------------------------------|
| `elasticsearch.collectionIndexMapping` | map[string]string | yes      |          | Defines which Couchbase collection events will be written to which index                            |
| `elasticsearch.urls`                   | []string          | yes      |          | Elasticsearch connection urls                                                                       |
| `elasticsearch.typeName`               | string            | no       | _doc     | Defines Elasticsearch index type name                                                               |
| `elasticsearch.batchSizeLimit`         | int               | no       | 1000     | Maximum message count for batch, if exceed flush will be triggered.                                 |
| `elasticsearch.batchTickerDuration`    | time.Duration     | no       | 10s      | Batch is being flushed automatically at specific time intervals for long waiting messages in batch. |
| `elasticsearch.batchByteSizeLimit`     | int               | no       | 10485760 | Maximum size(byte) for batch, if exceed flush will be triggered.                                    |
| `elasticsearch.maxConnsPerHost`        | int               | no       | 512      | Maximum number of connections per each host which may be established                                |
| `elasticsearch.maxIdleConnDuration`    | time.Duration     | no       | 10s      | Idle keep-alive connections are closed after this duration.                                         | 
| `elasticsearch.compressionEnabled`     | boolean           | no       | false    | Compression can be used if message size is large, CPU usage may be affected.                        |

## Exposed metrics

| Metric Name                                             | Description                                             | Labels | Value Type |
|---------------------------------------------------------|---------------------------------------------------------|--------|------------|
| elasticsearch_connector_latency_ms                      | Elasticsearch connector latency ms                      | N/A    | Gauge      |
| elasticsearch_connector_bulk_request_process_latency_ms | Elasticsearch connector bulk request process latency ms | N/A    | Gauge      |
