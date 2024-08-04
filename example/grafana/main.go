package main

import (
	"fmt"
	dcpelasticsearch "github.com/Trendyol/go-dcp-elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"github.com/couchbase/gocb/v2"
	"log"
	"time"
)

func mapper(event couchbase.Event) []document.ESActionDocument {
	print("Captured %s", string(event.Key))
	if event.IsMutated {
		e := document.NewIndexAction(event.Key, event.Value, nil)
		return []document.ESActionDocument{e}
	}
	e := document.NewDeleteAction(event.Key, nil)
	return []document.ESActionDocument{e}
}

func main() {
	go seedCouchbaseBucket()

	connector, err := dcpelasticsearch.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}

func seedCouchbaseBucket() {
	cluster, err := gocb.Connect("couchbase://couchbase", gocb.ClusterOptions{
		Username: "user",
		Password: "password",
	})
	if err != nil {
		log.Fatal(err)
	}

	bucket := cluster.Bucket("dcp-test")
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	}

	collection := bucket.DefaultCollection()

	counter := 0
	for {
		counter++
		documentID := fmt.Sprintf("doc-%d", counter)
		document := map[string]interface{}{
			"counter": counter,
			"message": "Hello Couchbase",
			"time":    time.Now().Format(time.RFC3339),
		}
		_, err := collection.Upsert(documentID, document, nil)
		if err != nil {
			log.Println("Error inserting document:", err)
		} else {
			log.Println("Inserted document:", documentID)
		}
		time.Sleep(1 * time.Second)
	}
}
