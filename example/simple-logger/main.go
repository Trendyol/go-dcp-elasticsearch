package main

import (
	dcpelasticsearch "github.com/Trendyol/go-dcp-elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"github.com/sirupsen/logrus"
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
	logger := createLogger()
	connector, err := dcpelasticsearch.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		SetLogger(logger).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}

func createLogger() *logrus.Logger {
	logger := logrus.New()

	logger.SetLevel(logrus.ErrorLevel)
	formatter := &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyMsg:   "msg",
			logrus.FieldKeyLevel: "logLevel",
			logrus.FieldKeyTime:  "timestamp",
		},
	}

	logger.SetFormatter(formatter)
	return logger
}
