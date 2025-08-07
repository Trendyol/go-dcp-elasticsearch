# Chapter 4: Config

Welcome back! In the previous chapter, [Chapter 3: Connector](03_connector_.md), we learned that the Connector is the central piece that orchestrates the entire data replication process. It connects to Couchbase, listens for changes, and manages sending those changes to Elasticsearch.

But how does the Connector know *where* to connect? How does it know which data from Couchbase should go to which index in Elasticsearch? How does it know how many changes to group together before sending them to Elasticsearch?

This is where the **Config** comes in.

## What is the Config?

Think of the **Config** as the **instruction manual** for your Connector. Before you start the Connector, you give it a Config, and this Config contains all the essential settings it needs to operate.

It tells the Connector things like:

*   The network addresses and credentials for your **Couchbase cluster**.
*   The network addresses and credentials for your **Elasticsearch cluster**.
*   Which **Bucket**, **Scope**, and **Collection** in Couchbase to listen to.
*   Crucially, how to **map** documents from specific Couchbase Collections to specific Elasticsearch Indexes.
*   Performance settings, such as how many documents to batch together before sending to Elasticsearch and how often to send batches.
*   How to handle errors, like where to log documents that Elasticsearch rejects.

Without a Config, the Connector wouldn't know where to go or what to do!

## The Core Use Case: Telling the Connector Where to Connect and What to Send

The most fundamental use case for the Config is answering these questions:

1.  Where is Couchbase?
2.  Where is Elasticsearch?
3.  If a document changes in Couchbase Collection `my_collection`, which Elasticsearch Index should it go into?

The Config file is typically written in YAML format, which is easy for humans to read and write. Let's look at a simplified example of what a `config.yml` might look like, using the example files provided in the project (e.g., `example/simple/config.yml`):

```yaml
# Couchbase connection details
hosts:
  - localhost:8091      # Address of your Couchbase cluster
username: user
password: password
bucketName: dcp-test     # The Couchbase bucket to replicate from

# Settings for the underlying go-dcp library
dcp:
  group:
    name: groupName      # A name for this connector instance (important if you run multiple)
    membership:
      type: static

# Where the connector stores its progress (checkpoints)
metadata:
  type: couchbase
  config:
    bucket: checkpoint-bucket-name # A bucket to store checkpoints
    scope: _default
    collection: _default

# *** THIS IS THE ELASTICSEARCH SPECIFIC CONFIG ***
elasticsearch:
  # Addresses of your Elasticsearch cluster
  urls:
    - "http://localhost:9200"
  # Mapping Couchbase Collections to Elasticsearch Indexes
  collectionIndexMapping:
    _default: indexname # Documents from Couchbase's _default collection go to 'indexname' in ES
  # Optional: Authentication for Elasticsearch (if needed)
  # username: elastic
  # password: changeme

  # Performance settings (optional, defaults are applied if not set)
  # batchSizeLimit: 1000        # Max documents per batch
  # batchTickerDuration: 10s    # How often to send batches even if not full
  # batchByteSizeLimit: 10mb    # Max size of a batch in bytes
  # concurrentRequest: 1        # How many batches to send at the same time
```

This `config.yml` tells the Connector:

*   Connect to Couchbase at `localhost:8091` using the username `user` and password `password` for the bucket `dcp-test`.
*   Store progress (`checkpoints`) in the `checkpoint-bucket-name` bucket.
*   Connect to Elasticsearch at `http://localhost:9200`.
*   Send documents from the Couchbase `_default` collection to the Elasticsearch index named `indexname`.
*   Use default values for batching and concurrency (unless uncommented and specified).

This simple configuration file covers the essential instructions for the Connector to start replicating data.

## How to Provide the Config to the Connector

As shown in [Chapter 3: Connector](03_connector_.md), you provide the configuration when you build the Connector instance using the `NewConnectorBuilder`.

You can give it the path to your YAML file:

```go
// Simplified snippet from example/simple/main.go
import (
	"github.com/Trendyol/go-dcp-elasticsearch"
)

func main() {
	// Create builder, giving it the path to the config file
	connector, err := dcpelasticsearch.NewConnectorBuilder("config.yml").Build()
	if err != nil {
		panic(err) // Always handle errors!
	}

	// ... start and defer close ...
}
```

Or, you can load the config yourself (e.g., from environment variables, a database, etc.) and provide the Go `Config` struct directly:

```go
// Example: Providing config struct directly
import (
	"github.com/Trendyol/go-dcp-elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/config" // Import the config package
)

func main() {
	// Create and populate the config struct manually
	myConfig := &config.Config{
		Dcp: config.Dcp{ /* ... couchbase settings ... */ },
		Elasticsearch: config.Elasticsearch{
			Urls: []string{"http://localhost:9200"},
			CollectionIndexMapping: map[string]string{
				"_default": "my_target_index",
			},
			// ... other ES settings ...
		},
	}

	// Create builder, giving it the config struct
	connector, err := dcpelasticsearch.NewConnectorBuilder("").SetConfig(myConfig).Build() // Use "" for path and SetConfig()
	if err != nil {
		panic(err)
	}

	// ... start and defer close ...
}
```
*(Note: When using `SetConfig`, you pass an empty string `""` to `NewConnectorBuilder` to indicate that you are providing the config via `SetConfig` rather than a file path.)*

Most commonly, you'll use a YAML file as it's flexible and easy to manage outside of your Go code.

## Inside the Config Structure (`config/config.go`)

Let's peek at the Go code definition of the `Config` struct (`config/config.go`) to understand its structure.

```go
// Simplified snippet from config/config.go
package config

import (
	"time"
	"github.com/Trendyol/go-dcp/config" // Import go-dcp's config
	"github.com/Trendyol/go-dcp/helpers"
)

// Config holds the overall configuration for the connector
type Config struct {
	// Elasticsearch section specific to go-dcp-elasticsearch
	Elasticsearch Elasticsearch `yaml:"elasticsearch" mapstructure:"elasticsearch"`

	// Dcp section is embedded from the underlying go-dcp library's config
	Dcp config.Dcp `yaml:",inline" mapstructure:",squash"`

	// Other go-dcp top-level fields are also embedded implicitly
	// like hosts, username, password, bucketName, metadata, checkpoint, etc.
}

// Elasticsearch holds settings specific to the Elasticsearch sink
type Elasticsearch struct {
	Urls                        []string          `yaml:"urls"`
	Username                    string            `yaml:"username"`
	Password                    string            `yaml:"password"`
	CollectionIndexMapping      map[string]string `yaml:"collectionIndexMapping"`

	// Performance related settings
	BatchSizeLimit              int               `yaml:"batchSizeLimit"`
	BatchTickerDuration         time.Duration     `yaml:"batchTickerDuration"`
	BatchByteSizeLimit          any               `yaml:"batchByteSizeLimit"` // Can be int or string like "10mb"
	ConcurrentRequest           int               `yaml:"concurrentRequest"`
	CompressionEnabled          bool              `yaml:"compressionEnabled"`

	// Elasticsearch client settings
	MaxConnsPerHost             *int              `yaml:"maxConnsPerHost"`
	MaxIdleConnDuration         *time.Duration    `yaml:"maxIdleConnDuration"`
	DiscoverNodesInterval       *time.Duration    `yaml:"discoverNodesInterval"`
	DisableDiscoverNodesOnStart bool              `yaml:"disableDiscoverNodesOnStart"`
	TypeName                    string            `yaml:"typeName"` // Deprecated field in Elasticsearch

	// Error logging settings
	RejectionLog                RejectionLog      `yaml:"rejectionLog"`
}

// RejectionLog specifies where to log documents rejected by Elasticsearch
type RejectionLog struct {
	Index         string `yaml:"index"`         // The ES index for rejection logs
	IncludeSource bool   `yaml:"includeSource"` // Whether to include the rejected document source
}

// ApplyDefaults sets default values for fields if they are not provided
func (c *Config) ApplyDefaults() {
	// ... (Checks and sets defaults for Elasticsearch fields) ...
	if c.Elasticsearch.BatchTickerDuration == 0 {
		c.Elasticsearch.BatchTickerDuration = 10 * time.Second
	}
	// ... etc. ...
}
```

As you can see:

*   The main `Config` struct includes an `Elasticsearch` field (which holds all the settings specific to Elasticsearch) and also embeds `config.Dcp` and other fields from the underlying `go-dcp` library's configuration structure. This means you specify Couchbase settings (like `hosts`, `bucketName`) directly at the top level of your `config.yml`.
*   The `Elasticsearch` struct defines all the parameters needed for the Elasticsearch connection and behavior.
*   The `CollectionIndexMapping` is a `map[string]string`, where the key is the Couchbase Collection name and the value is the target Elasticsearch Index name. This is a critical part of the configuration for directing data.
*   There's a separate `RejectionLog` struct for configuring where failed Elasticsearch requests should be logged within Elasticsearch itself.
*   The `ApplyDefaults()` method is automatically called when the config is loaded. It ensures that if you don't specify optional fields like `BatchSizeLimit` or `BatchTickerDuration`, sensible default values are used. This makes it easier to get started with a minimal config file.

When the Connector is built, it loads the Config (from file or struct), applies defaults, and then uses the values within the `Config` object to initialize the [Elasticsearch Client](07_elasticsearch_client_.md), configure the [Bulk Processor](06_bulk_processor_.md), and set up the underlying `go-dcp` library for connecting to Couchbase.

## Summary

In this chapter, we focused on the **Config**, the essential instruction manual for the `go-dcp-elasticsearch` Connector. We learned that it's typically a YAML file that tells the Connector where to connect (Couchbase and Elasticsearch addresses/credentials), how to map data from Couchbase Collections to Elasticsearch Indexes (`collectionIndexMapping`), and various performance and error handling settings.

We saw how to provide the Config to the ConnectorBuilder and looked at the structure of the `Config` and `Elasticsearch` Go structs to understand the available settings.

Understanding the Config is crucial because it dictates the fundamental behavior of the connector – where data comes from, where it goes, and how efficiently it gets there.

Now that the Connector knows *where* the data comes from and *where* it should generally go, the next question is *how* the content of the Couchbase document should be transformed or prepared before being sent to Elasticsearch. This is the job of the **Mapper**.

[Next Chapter: Mapper](05_mapper_.md)