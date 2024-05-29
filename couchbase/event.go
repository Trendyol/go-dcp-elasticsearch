package couchbase

import (
	"time"

	"github.com/elastic/go-elasticsearch/v7"
)

type Event struct {
	ElasticsearchClient *elasticsearch.Client
	EventTime           time.Time
	CollectionName      string
	Key                 []byte
	Value               []byte
	Cas                 uint64
	VbID                uint16
	IsDeleted           bool
	IsExpired           bool
	IsMutated           bool
	SeqNo               uint64
	RevNo               uint64
}

func NewDeleteEvent(
	esClient *elasticsearch.Client,
	key []byte, value []byte,
	collectionName string, cas uint64, eventTime time.Time, vbID uint16, seqNo uint64, revNo uint64,
) Event {
	return Event{
		ElasticsearchClient: esClient,
		Key:                 key,
		Value:               value,
		IsDeleted:           true,
		CollectionName:      collectionName,
		Cas:                 cas,
		EventTime:           eventTime,
		VbID:                vbID,
		SeqNo:               seqNo,
		RevNo:               revNo,
	}
}

func NewExpireEvent(
	esClient *elasticsearch.Client,
	key []byte, value []byte,
	collectionName string, cas uint64, eventTime time.Time, vbID uint16, seqNo uint64, revNo uint64,
) Event {
	return Event{
		ElasticsearchClient: esClient,
		Key:                 key,
		Value:               value,
		IsExpired:           true,
		CollectionName:      collectionName,
		Cas:                 cas,
		EventTime:           eventTime,
		VbID:                vbID,
		SeqNo:               seqNo,
		RevNo:               revNo,
	}
}

func NewMutateEvent(
	esClient *elasticsearch.Client,
	key []byte, value []byte,
	collectionName string, cas uint64, eventTime time.Time, vbID uint16, seqNo uint64, revNo uint64,
) Event {
	return Event{
		ElasticsearchClient: esClient,
		Key:                 key,
		Value:               value,
		IsMutated:           true,
		CollectionName:      collectionName,
		Cas:                 cas,
		EventTime:           eventTime,
		VbID:                vbID,
		SeqNo:               seqNo,
		RevNo:               revNo,
	}
}
