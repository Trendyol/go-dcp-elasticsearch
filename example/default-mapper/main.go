package main

import (
	"github.com/Trendyol/go-dcp-elasticsearch"
)

func main() {
	connector, err := dcpelasticsearch.NewConnectorBuilder("config.yml").Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
