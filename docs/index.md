# Tutorial: go-dcp-elasticsearch

The `go-dcp-elasticsearch` project acts as a **bridge** between Couchbase and Elasticsearch.
It **listens** for *data changes* in a Couchbase bucket using the Database Change Protocol (DCP).
These changes are then *transformed* and **sent** to Elasticsearch in an efficient, **batched** manner,
optionally handling *processing results* (successes/failures) and logging rejections.


## Visual Overview

```mermaid
flowchart TD
    A0["Connector
"]
    A1["Config
"]
    A2["Couchbase Event
"]
    A3["Mapper
"]
    A4["Elasticsearch Action Document
"]
    A5["Bulk Processor
"]
    A6["Elasticsearch Client
"]
    A7["Sink Response Handler
"]
    A0 -- "Uses Config" --> A1
    A0 -- "Receives Event" --> A2
    A0 -- "Uses Mapper" --> A3
    A3 -- "Creates Action" --> A4
    A0 -- "Uses Bulk Processor" --> A5
    A4 -- "Batched by Bulk" --> A5
    A5 -- "Uses Client" --> A6
    A5 -- "Notifies Handler" --> A7
    A7 -- "Logs via Client" --> A6
```

## Chapters

1. [Couchbase Event
   ](01_couchbase_event_.md)
2. [Elasticsearch Action Document
   ](02_elasticsearch_action_document_.md)
3. [Connector
   ](03_connector_.md)
4. [Config
   ](04_config_.md)
5. [Mapper
   ](05_mapper_.md)
6. [Bulk Processor
   ](06_bulk_processor_.md)
7. [Elasticsearch Client
   ](07_elasticsearch_client_.md)
8. [Sink Response Handler
   ](08_sink_response_handler_.md)
