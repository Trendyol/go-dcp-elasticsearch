package couchbase

import "time"

type Event struct {
	CollectionName string
	EventTime      time.Time
	Key            []byte
	Value          []byte
	Cas            uint64
	IsDeleted      bool
	IsExpired      bool
	IsMutated      bool
}

func NewDeleteEvent(key []byte, value []byte, collectionName string, cas uint64) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsDeleted:      true,
		CollectionName: collectionName,
		Cas:            cas,
	}
}

func NewExpireEvent(key []byte, value []byte, collectionName string, cas uint64) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsExpired:      true,
		CollectionName: collectionName,
		Cas:            cas,
	}
}

func NewMutateEvent(key []byte, value []byte, collectionName string, cas uint64) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsMutated:      true,
		CollectionName: collectionName,
		Cas:            cas,
	}
}
