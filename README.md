# Go Elasticsearch Connect Couchbase [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-elasticsearch-connect-couchbase.svg)](https://pkg.go.dev/github.com/Trendyol/go-elasticsearch-connect-couchbase) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-elasticsearch-connect-couchbase)](https://goreportcard.com/report/github.com/Trendyol/go-elasticsearch-connect-couchbase)

This repository contains the Go implementation of the Couchbase Elasticsearch Connector.

### Contents

---

* [What is Couchbase Elasticsearch Connector?](#what-is-couchbase-elasticsearch-connector)
* [Why?](#why)
* [Features](#features)
* [Usage](#usage)
* [Configuration](#configuration)
* [Examples](#examples)

### What is Couchbase Elasticsearch Connector?

**Go Elasticsearch Connect Couchbase is a source connector**. So it writes Couchbase mutations to Elasticsearch as
documents.

---

### Why?

+ Build a Couchbase Elasticsearch Connector by using **Go** to reduce resource usage.
+ Suggesting more **configurations** so users can make changes to less code.
+ By presenting the connector as a **library**, ensuring that users do not clone the code they don't need.

---

### Example

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

---

### Usage

```
$ go get github.com/Trendyol/go-elasticsearch-connect-couchbase

```

---

### Dcp Configuration

Check out on [go-dcp-client](https://github.com/Trendyol/go-dcp-client#configuration)

### Elasticsearch Specific Configuration

| Variable                               | Type              | Required | Default  |                                                             
|----------------------------------------|-------------------|----------|----------|
| `elasticsearch.collectionIndexMapping` | map[string]string | yes      |          |
| `elasticsearch.urls`                   | []string          | yes      |          |
| `elasticsearch.typeName`               | string            | no       | _doc     |
| `elasticsearch.batchSizeLimit`         | int               | no       | 1000     |
| `elasticsearch.batchTickerDuration`    | time.Duration     | no       | 10s      |
| `elasticsearch.batchByteSizeLimit`     | int               | no       | 10485760 |
| `elasticsearch.batchByteSizeLimit`     | int               | no       | 10485760 |
| `elasticsearch.maxConnsPerHost`        | int               | no       | 512      |
| `elasticsearch.maxIdleConnDuration`    | time.Duration     | no       | 10s      |

---

### Examples

- [example](example/main.go)