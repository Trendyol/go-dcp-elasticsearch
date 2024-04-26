package couchbase

import "time"

type Event struct {
	EventTime      time.Time
	CollectionName string
	Key            []byte
	Value          []byte
	Cas            uint64
	VbID           uint16
	IsDeleted      bool
	IsExpired      bool
	IsMutated      bool
	SeqNo          uint64
	RevNo          uint64
}

func NewDeleteEvent(
	key []byte, value []byte,
	collectionName string, cas uint64, eventTime time.Time, vbID uint16, seqNo uint64, revNo uint64,
) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsDeleted:      true,
		CollectionName: collectionName,
		Cas:            cas,
		EventTime:      eventTime,
		VbID:           vbID,
		SeqNo:          seqNo,
		RevNo:          revNo,
	}
}

func NewExpireEvent(
	key []byte, value []byte,
	collectionName string, cas uint64, eventTime time.Time, vbID uint16, seqNo uint64, revNo uint64,
) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsExpired:      true,
		CollectionName: collectionName,
		Cas:            cas,
		EventTime:      eventTime,
		VbID:           vbID,
		SeqNo:          seqNo,
		RevNo:          revNo,
	}
}

func NewMutateEvent(
	key []byte, value []byte,
	collectionName string, cas uint64, eventTime time.Time, vbID uint16, seqNo uint64, revNo uint64,
) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsMutated:      true,
		CollectionName: collectionName,
		Cas:            cas,
		EventTime:      eventTime,
		VbID:           vbID,
		SeqNo:          seqNo,
		RevNo:          revNo,
	}
}
