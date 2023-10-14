# Go Dcp Elasticsearch

[![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp-elasticsearch.svg)](https://pkg.go.dev/github.com/Trendyol/go-dcp-elasticsearch) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-dcp-elasticsearch)](https://goreportcard.com/report/github.com/Trendyol/go-dcp-elasticsearch)

Go implementation of
the [Elasticsearch Connect Couchbase](https://github.com/couchbase/couchbase-elasticsearch-connector).

**Go Dcp Elasticsearch** streams documents from Couchbase Database Change Protocol (DCP) and writes to
Elasticsearch index in near real-time.

## Features

* **Less resource usage** and **higher throughput**(see [Benchmarks](#benchmarks)).
* **Custom routing** support(see [Example](#example)).
* **Update multiple documents** for a DCP event(see [Example](#example)).
* Handling different DCP events such as **expiration, deletion and mutation**(see [Example](#example)).
* **Elasticsearch compression request body** support.
* **Managing batch configurations** such as maximum batch size, batch bytes, batch ticker durations.
* **Scale up and down** by custom membership algorithms(Couchbase, KubernetesHa, Kubernetes StatefulSet or
  Static, see [examples](https://github.com/Trendyol/go-dcp#examples)).
* **Easily manageable configurations**.

## Benchmarks

The benchmark was made with the  **1,001,006** Couchbase document, because it is possible to more clearly observe the
difference in the batch structure between the two packages. **Default configurations** for Java Elasticsearch Connect Couchbase
used for both connectors.

| Package                                         | Time to Process Events | Elasticsearch Indexing Rate(/s) | Average CPU Usage(Core) | Average Memory Usage |
|:------------------------------------------------|:----------------------:|:-------------------------------:|:-----------------------:|:--------------------:|
| **Go Dcp Elasticsearch**(Go 1.20) |        **50s**         |    ![go](./benchmark/go.png)    |        **0.486**        |      **408MB**       
| Java Elasticsearch Connect Couchbase(JDK15)     |          80s           |   ![go](./benchmark/java.png)   |          0.31           |        1091MB         

## Example
[Struct Config](example/struct-config/main.go)
```go
func mapper(event couchbase.Event) []document.ESActionDocument {
	if event.IsMutated {
		e := document.NewIndexAction(event.Key, event.Value, nil)
		return []document.ESActionDocument{e}
	}
	e := document.NewDeleteAction(event.Key, nil)
	return []document.ESActionDocument{e}
}

func main() {
	connector, err := dcpelasticsearch.NewConnectorBuilder(config.Config{
		Elasticsearch: config.Elasticsearch{
			CollectionIndexMapping: map[string]string{
				"_default": "indexname",
			},
			Urls: []string{"http://localhost:9200"},
		},
		Dcp: dcpConfig.Dcp{
			Username:   "user",
			Password:   "password",
			BucketName: "dcp-test",
			Hosts:      []string{"localhost:8091"},
			Dcp: dcpConfig.ExternalDcp{
				Group: dcpConfig.DCPGroup{
					Name: "groupName",
					Membership: dcpConfig.DCPGroupMembership{
						Type: "static",
					},
				},
			},
			Metadata: dcpConfig.Metadata{
				Config: map[string]string{
					"bucket":     "checkpoint-bucket-name",
					"scope":      "_default",
					"collection": "_default",
				},
				Type: "couchbase",
			},
		},
	}).
		SetMapper(mapper).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
```

[File Config](example/simple/main.go)

[Default Mapper](example/default-mapper/main.go)
## Configuration

### Dcp Configuration

Check out on [go-dcp](https://github.com/Trendyol/go-dcp#configuration)

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
| `elasticsearch.concurrentRequest`      | int               | no       | 1        | Concurrent bulk request count                                                                       |
| `elasticsearch.username`      		 | string            | no       |    ""     | Elasticsearch username	                                 |
| `elasticsearch.password`      		 | string            | no       |    ""     | Elasticsearch password	                                 |
| `elasticsearch.cloudId`      		 	 | string            | no       |    ""     | Elasticsearch cloud id	                               	 |
| `elasticsearch.apiKey`      		 	 | string            | no       |    ""     | Elasticsearch API Key	                               	 |
| `elasticsearch.serviceToken`      	 | string            | no       |    ""     | Elasticsearch service token	                               	 		 |
| `elasticsearch.certificateFingerprint` | string            | no       |    ""     | Elasticsearch certificate fingerprint	                                 		 |
| `elasticsearch.header`      		 	 | map[string][]string	| no       |   nil      | Headers to set when client makes a request. 	                                 		 |
| `elasticsearch.caCert`      		 	 | []byte            | no       |    nil     | Elasticsearch PEM-encoded certificate authorities. 	                               	 		 |
| `elasticsearch.retryOnStatus`      	 | []int             | no       |   502, 503, 504      | List of status codes for retry for the client	                               	 		 |
| `elasticsearch.disableRetry`      	 | bool              | no       |   false      | Disables retry on client.	                                 |
| `elasticsearch.enableRetryOnTimeout`   | bool              | no       |   false     | Enables retry when timeout occurs.	                                 |
| `elasticsearch.discoverNodesOnStart`   | bool              | no       |   false       | Enables discovery of nodes when initializing the client.	                               	 |
| `elasticsearch.discoverNodesInterval`  | time.Duration     | no       |  0       | Sets time interval to discover nodes periodically.	                               	 |
| `elasticsearch.enableMetrics`      	 | bool              | no       |   false     | Enables the metrics collection. 	                               	 		 |
| `elasticsearch.enableDebugLogger` 	 | bool              | no       |    false    | Enables the debug logging. 	                                 		 |
| `elasticsearch.enableCompatibilityMode`| bool              | no       |    false     | Enables sending compatibility header. 	                                 		 |
| `elasticsearch.disableMetaHeader`      | bool              | no       |    false     | Disables the additional "X-Elastic-Client-Meta" HTTP header when client performs a request. 	                               	 		 |
| `elasticsearch.useResponseCheckOnly`   | bool              | no       |    false     | Disables checking if the cluster is a genuine Elasticsearch product before performing request.
| `elasticsearch.retryBackoff` 			 | func(int) time.Duration	| no       |   nil      | Retry backoff duration. 	                                 		 |
| `elasticsearch.logger`      		 	 | estransport.Logger	| no       |    nil     | The logger object. 	                                 		 |
| `elasticsearch.selector`      		 | estransport.Selector	| no       |    nil     | The selector object.	                               	 		 |
| `elasticsearch.connectionPoolFunc`     | func([]*estransport.Connection, estransport.Selector) estransport.ConnectionPool	| no       |  nil       | Constructor function for a custom ConnectionPool. 	                               	 		 |

## Exposed metrics

| Metric Name                                             | Description                   | Labels | Value Type |
|---------------------------------------------------------|-------------------------------|--------|------------|
| elasticsearch_connector_latency_ms                      | Time to adding to the batch.  | N/A    | Gauge      |
| elasticsearch_connector_bulk_request_process_latency_ms | Time to process bulk request. | N/A    | Gauge      |

You can also use all DCP-related metrics explained [here](https://github.com/Trendyol/go-dcp#exposed-metrics).
All DCP-related metrics are automatically injected. It means you don't need to do anything. 

## Contributing

Go Dcp Elasticsearch is always open for direct contributions. For more information please check
our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).
